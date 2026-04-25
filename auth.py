"""
auth.py — Multi-user authentication for emerging-edge.

When the server is launched with the env var ``MULTI_USER=1`` (typical
on a hosted deploy like Fly.io), every request is gated by a session
cookie. The control-plane SQLite (``users.db`` next to the per-user
DBs) holds two tables:

    users     (id, email, pw_hash, created_at)
    sessions  (token, user_id, created_at, last_seen_at)

Each user gets their own per-user SQLite file at
``$EE_DATA_DIR/u_<user_id>.db`` — the same schema that
``Database()`` auto-creates on first write. So a fresh signup yields
an empty watchlist, empty portfolio, empty fundamentals, and the
defaults shipped in code (Telegram channels, fund aliases) flow in
on first read.

Local-dev (``MULTI_USER`` unset) keeps the legacy single-DB behaviour
exactly as before — this whole module is bypassed.
"""

from __future__ import annotations

import datetime
import hashlib
import http.cookies
import os
import re
import secrets
import sqlite3
import threading
from typing import Optional


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DATA_DIR = os.environ.get("EE_DATA_DIR") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data"
)
USERS_DB = os.path.join(DATA_DIR, "users.db")

_lock = threading.Lock()


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def is_multiuser() -> bool:
    """Are we running in multi-user mode?"""
    return os.environ.get("MULTI_USER", "").lower() in ("1", "true", "yes")


def ensure_dirs() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Control-plane DB (users + sessions)
# ---------------------------------------------------------------------------

def _users_conn() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(USERS_DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            email       TEXT NOT NULL UNIQUE,
            pw_hash     TEXT NOT NULL,
            created_at  TEXT NOT NULL
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token        TEXT PRIMARY KEY,
            user_id      INTEGER NOT NULL,
            created_at   TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )""")
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Password hashing — stdlib scrypt
# ---------------------------------------------------------------------------
# scrypt is built into hashlib (no external dependency). We store the
# salt + the derived key concatenated, base64'd, plus the cost params
# embedded in a Modular Crypt-Format-style string.
#
# Format: scrypt$N$r$p$<salt_b64>$<key_b64>

_SCRYPT_N = 2 ** 14
_SCRYPT_R = 8
_SCRYPT_P = 1
_SCRYPT_DKLEN = 32


def hash_password(plain: str) -> str:
    salt = secrets.token_bytes(16)
    key = hashlib.scrypt(
        plain.encode("utf-8"), salt=salt,
        n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P, dklen=_SCRYPT_DKLEN,
        maxmem=64 * 1024 * 1024,
    )
    import base64
    return (
        f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}$"
        + base64.b64encode(salt).decode("ascii") + "$"
        + base64.b64encode(key).decode("ascii")
    )


def verify_password(plain: str, stored: str) -> bool:
    if not stored or not stored.startswith("scrypt$"):
        return False
    try:
        _, n, r, p, salt_b64, key_b64 = stored.split("$")
        import base64
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(key_b64)
        got = hashlib.scrypt(
            plain.encode("utf-8"), salt=salt,
            n=int(n), r=int(r), p=int(p), dklen=len(expected),
            maxmem=64 * 1024 * 1024,
        )
        return secrets.compare_digest(got, expected)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Public API: signup / login / logout / resolve
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class AuthError(Exception):
    pass


def signup(email: str, password: str) -> tuple[int, str]:
    """Create a new user and return (user_id, session_token).

    Raises AuthError on validation failure or duplicate email.
    """
    email = (email or "").strip().lower()
    password = password or ""
    if not _EMAIL_RE.match(email):
        raise AuthError("Please enter a valid email address.")
    if len(password) < 8:
        raise AuthError("Password must be at least 8 characters.")
    pw_hash = hash_password(password)
    with _lock:
        conn = _users_conn()
        try:
            conn.execute(
                "INSERT INTO users (email, pw_hash, created_at) VALUES (?, ?, ?)",
                (email, pw_hash, _now()),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise AuthError("That email is already registered. Try logging in.")
        row = conn.execute(
            "SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        user_id = row["id"]
        token = secrets.token_urlsafe(32)
        conn.execute(
            "INSERT INTO sessions (token, user_id, created_at, last_seen_at) "
            "VALUES (?, ?, ?, ?)",
            (token, user_id, _now(), _now()))
        conn.commit()
        conn.close()
    return user_id, token


def login(email: str, password: str) -> tuple[int, str]:
    """Verify credentials and return (user_id, session_token)."""
    email = (email or "").strip().lower()
    with _lock:
        conn = _users_conn()
        row = conn.execute(
            "SELECT id, pw_hash FROM users WHERE email = ?", (email,)).fetchone()
        if not row or not verify_password(password, row["pw_hash"]):
            conn.close()
            raise AuthError("Email or password is incorrect.")
        user_id = row["id"]
        token = secrets.token_urlsafe(32)
        conn.execute(
            "INSERT INTO sessions (token, user_id, created_at, last_seen_at) "
            "VALUES (?, ?, ?, ?)",
            (token, user_id, _now(), _now()))
        conn.commit()
        conn.close()
    return user_id, token


def logout(token: str) -> None:
    if not token:
        return
    with _lock:
        conn = _users_conn()
        conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
        conn.commit()
        conn.close()


def resolve_session(token: str) -> Optional[dict]:
    """Return a {id, email} dict for the user owning ``token``, or None."""
    if not token:
        return None
    with _lock:
        conn = _users_conn()
        row = conn.execute(
            """SELECT u.id AS id, u.email AS email
               FROM sessions s JOIN users u ON s.user_id = u.id
               WHERE s.token = ?""", (token,)).fetchone()
        if row:
            conn.execute(
                "UPDATE sessions SET last_seen_at = ? WHERE token = ?",
                (_now(), token))
            conn.commit()
        conn.close()
    if not row:
        return None
    return {"id": row["id"], "email": row["email"]}


# ---------------------------------------------------------------------------
# Cookie helpers
# ---------------------------------------------------------------------------

COOKIE_NAME = "ee_session"


def parse_session_token(cookie_header: str | None) -> str:
    """Pull the session token out of a Cookie header."""
    if not cookie_header:
        return ""
    try:
        c = http.cookies.SimpleCookie()
        c.load(cookie_header)
        morsel = c.get(COOKIE_NAME)
        return morsel.value if morsel else ""
    except Exception:
        return ""


def cookie_set(token: str, secure: bool = True) -> str:
    """Build a Set-Cookie header value for a fresh session."""
    parts = [
        f"{COOKIE_NAME}={token}",
        "Path=/",
        "HttpOnly",
        "SameSite=Lax",
        "Max-Age=2592000",  # 30 days
    ]
    if secure:
        parts.append("Secure")
    return "; ".join(parts)


def cookie_clear(secure: bool = True) -> str:
    parts = [
        f"{COOKIE_NAME}=",
        "Path=/",
        "HttpOnly",
        "SameSite=Lax",
        "Max-Age=0",
    ]
    if secure:
        parts.append("Secure")
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# Per-user DB path
# ---------------------------------------------------------------------------

def user_db_path(user_id: int) -> str:
    """Where a given user's data lives on disk."""
    ensure_dirs()
    return os.path.join(DATA_DIR, f"u_{user_id}.db")


# ---------------------------------------------------------------------------
# Login / signup HTML pages
# ---------------------------------------------------------------------------

_AUTH_PAGE_CSS = """
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; height: 100%; }
body {
    background: #0f1117; color: #e2e4ea; font-size: 15px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                 Inter, sans-serif;
    display: flex; align-items: center; justify-content: center;
}
.auth-card {
    background: #1a1d27; border: 1px solid #2d3040;
    border-radius: 14px; padding: 2rem 2.4rem; width: 100%;
    max-width: 380px;
    box-shadow: 0 10px 40px rgba(0,0,0,0.4);
}
.auth-card h1 {
    margin: 0 0 0.4rem 0; font-size: 1.4rem; font-weight: 700;
}
.auth-card h1 span { color: #6c8cff; }
.auth-card .sub {
    color: #8b8fa3; font-size: 0.85rem; margin-bottom: 1.6rem;
}
.auth-card label {
    display: block; font-size: 0.78rem; color: #8b8fa3;
    margin-bottom: 0.3rem; text-transform: uppercase;
    letter-spacing: 0.04em; font-weight: 600;
}
.auth-card input {
    width: 100%; background: #0f1117; color: #e2e4ea;
    border: 1px solid #2d3040; border-radius: 8px;
    padding: 0.7rem 0.9rem; font-size: 0.95rem; margin-bottom: 1rem;
}
.auth-card input:focus {
    outline: none; border-color: #6c8cff;
}
.auth-card button {
    width: 100%; background: #6c8cff; color: #0f1117;
    border: none; border-radius: 8px; padding: 0.75rem;
    font-size: 0.95rem; font-weight: 700; cursor: pointer;
    margin-top: 0.4rem;
}
.auth-card button:hover { filter: brightness(1.08); }
.auth-card .alt {
    color: #8b8fa3; font-size: 0.8rem; margin-top: 1.2rem;
    text-align: center;
}
.auth-card .alt a { color: #6c8cff; text-decoration: none; }
.auth-card .alt a:hover { text-decoration: underline; }
.auth-card .err {
    background: rgba(255,107,107,0.1); color: #ff6b6b;
    border: 1px solid rgba(255,107,107,0.3);
    padding: 0.6rem 0.8rem; border-radius: 8px;
    font-size: 0.82rem; margin-bottom: 1rem;
}
"""


def render_login_page(error: str = "", email: str = "") -> str:
    err_html = (
        f'<div class="err">{_html_escape(error)}</div>' if error else ""
    )
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sign in — Emerging Edge</title>
<style>{_AUTH_PAGE_CSS}</style>
</head><body>
<form class="auth-card" method="POST" action="/login">
    <h1><span>Emerging Edge</span></h1>
    <div class="sub">Sign in to your dashboard.</div>
    {err_html}
    <label>Email</label>
    <input type="email" name="email" autocomplete="email" autofocus
           value="{_html_escape(email)}" required>
    <label>Password</label>
    <input type="password" name="password" autocomplete="current-password" required>
    <button type="submit">Sign in</button>
    <div class="alt">No account yet? <a href="/signup">Create one</a></div>
</form>
</body></html>"""


def render_signup_page(error: str = "", email: str = "") -> str:
    err_html = (
        f'<div class="err">{_html_escape(error)}</div>' if error else ""
    )
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Create account — Emerging Edge</title>
<style>{_AUTH_PAGE_CSS}</style>
</head><body>
<form class="auth-card" method="POST" action="/signup">
    <h1><span>Emerging Edge</span></h1>
    <div class="sub">Create your account — your own private monitor &amp; portfolio.</div>
    {err_html}
    <label>Email</label>
    <input type="email" name="email" autocomplete="email" autofocus
           value="{_html_escape(email)}" required>
    <label>Password (8+ characters)</label>
    <input type="password" name="password" autocomplete="new-password" required minlength="8">
    <button type="submit">Create account</button>
    <div class="alt">Already have an account? <a href="/login">Sign in</a></div>
</form>
</body></html>"""


def _html_escape(s: str) -> str:
    import html as _html
    return _html.escape(str(s)) if s else ""
