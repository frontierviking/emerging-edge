#!/usr/bin/env python3
"""
monitor.py — CLI entry point for emerging-edge.

Usage:
    python monitor.py run           # Fetch all data now (news, contracts, earnings, forums)
    python monitor.py digest        # Generate and display today's markdown digest
    python monitor.py html          # Generate HTML dashboard and open in browser
    python monitor.py upcoming      # Show the earnings calendar

Environment:
    SERPER_API_KEY   — required for Serper MCP news/web searches

The stock portfolio and all URLs are configured in config.json.
Edit that file to add/remove stocks without touching this code.
"""

import argparse
import html as _html_mod
import http.server
import json
import logging
import os


def _html_escape(s) -> str:
    return _html_mod.escape(str(s)) if s is not None else ""
import sqlite3
import sys
import threading
import traceback
import urllib.parse
import webbrowser
from datetime import datetime, timezone

# Ensure we can import sibling modules when run from any directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import Database
from fetchers import load_config, run_all, fetch_prices, get_active_stocks
from stock_search import search_stocks
from digest import generate_digest, save_digest, print_upcoming
from dashboard import save_html, open_html, generate_html
from portfolio import (import_transactions_csv, save_portfolio_html,
                       generate_portfolio_html, compute_reinvest_shortfall,
                       compute_convert_shortfall)
from engine_room import save_engine_room_html
from screener import save_screener_html


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
import re as _re

_MONTH_TOKEN = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _derive_report_date(url: str) -> str:
    """Best-effort YYYY-MM extraction from a fund-letter URL/filename.
    Falls back to today's YYYY-MM if no date found in the URL."""
    s = (url or "").lower()
    # YYYY-MM or YYYY_MM (4-digit year, dash/underscore, 2-digit month 01-12)
    m = _re.search(r"(20\d{2})[-_](0[1-9]|1[0-2])", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    # YYYYMM compact (e.g. PA-Comm-202503.pdf)
    m = _re.search(r"(20\d{2})(0[1-9]|1[0-2])(?!\d)", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    # MonthName-YYYY or MonthName_YYYY
    m = _re.search(
        r"\b(january|february|march|april|may|june|july|august|"
        r"september|october|november|december|jan|feb|mar|apr|"
        r"jun|jul|aug|sep|sept|oct|nov|dec)[\s_-]+(20\d{2})\b", s)
    if m:
        idx = _MONTH_TOKEN.get(m.group(1))
        if idx:
            return f"{m.group(2)}-{idx:02d}"
    return datetime.now(timezone.utc).strftime("%Y-%m")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

def cmd_run(args, config: dict, db: Database):
    """Fetch everything for all stocks and produce today's digest."""
    active_stocks = get_active_stocks(db, config)
    print(f"\n🚀 emerging-edge — running full fetch for {len(active_stocks)} stocks")
    print(f"   Database: {config.get('db_path', 'emerging_edge.db')}")
    print(f"   Time:     {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print()

    # Check for Serper API key
    if not os.environ.get("SERPER_API_KEY"):
        print("⚠️  WARNING: SERPER_API_KEY not set. News and contract searches will fail.")
        print("   Set it with: export SERPER_API_KEY=your_key_here")
        print()

    summary = run_all(config, db)

    # Print summary table
    print("\n" + "=" * 65)
    print("📋 Fetch Summary")
    print("=" * 65)
    print(f"{'Ticker':<12} {'Exchange':<8} {'News':>6} {'Contracts':>10} {'Earnings':>9} {'Forum':>6} {'Price':>6}")
    print("-" * 72)

    total_news = 0
    total_contracts = 0
    total_forum = 0

    for ticker, s in summary.items():
        stock = next((st for st in active_stocks if st["ticker"] == ticker), {})
        exchange = stock.get("exchange", "?")
        earn_str = "✅" if s.get("earnings") else "—"
        price_str = "✅" if s.get("price") else "—"
        print(f"{ticker:<12} {exchange:<8} {s.get('news', 0):>6} {s.get('contracts', 0):>10} {earn_str:>9} {s.get('forum', 0):>6} {price_str:>6}")
        total_news += s.get("news", 0)
        total_contracts += s.get("contracts", 0)
        total_forum += s.get("forum", 0)

    print("-" * 72)
    print(f"{'TOTAL':<12} {'':8} {total_news:>6} {total_contracts:>10} {'':>9} {total_forum:>6}")
    print()

    # Auto-generate today's digest (markdown + HTML)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filepath = save_digest(db, config, today)
    print(f"📝 Markdown digest: {filepath}")
    html_path = save_html(db, config, today)
    print(f"🌐 HTML dashboard:  {html_path}")
    print()


def cmd_digest(args, config: dict, db: Database):
    """Generate and display today's digest."""
    target_date = args.date if hasattr(args, "date") and args.date else datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Generate and save
    filepath = save_digest(db, config, target_date)
    print(f"📝 Digest saved to: {filepath}\n")

    # Also print to stdout
    content = generate_digest(db, config, target_date)
    print(content)


def cmd_html(args, config: dict, db: Database):
    """Generate HTML dashboard and open in browser."""
    target_date = args.date if hasattr(args, "date") and args.date else datetime.now(timezone.utc).strftime("%Y-%m-%d")

    filepath = save_html(db, config, target_date)
    print(f"🌐 HTML dashboard saved to: {filepath}")
    open_html(filepath)
    print("   Opened in browser.")


def cmd_publish(args, config: dict, db: Database):
    """Generate a view-only snapshot for sharing with beta testers.

    Writes ``./public/index.html`` (the Monitor view, view-only — no
    Add Stock / Refresh / Engine Room / Portfolio links) plus a Vercel
    deploy config that gates the URL with HTTP Basic Auth.

    Usage:
        python monitor.py publish              # write snapshot to ./public/
        cd public && vercel --prod              # deploy
        # Set BETA_USERNAME / BETA_PASSWORD on the Vercel project.
        # Share the URL + password with the beta tester.
    """
    out_dir = args.out if hasattr(args, "out") and args.out else "./public"
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"📦 Building view-only snapshot for {today} → {out_dir}")
    html = generate_html(db, config, today, view_only=True)
    index_path = os.path.join(out_dir, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"   wrote {index_path} ({len(html)//1024} KB)")

    # Vercel project config — static deployment + Edge Middleware for
    # password protection. Free-tier-compatible (no Vercel Pro needed).
    vercel_json = os.path.join(out_dir, "vercel.json")
    if not os.path.exists(vercel_json):
        with open(vercel_json, "w", encoding="utf-8") as f:
            f.write(json.dumps({
                "name": "emerging-edge-beta",
                "cleanUrls": True,
                "headers": [{
                    "source": "/(.*)",
                    "headers": [
                        {"key": "X-Frame-Options", "value": "SAMEORIGIN"},
                        {"key": "Referrer-Policy", "value": "no-referrer"},
                    ],
                }],
            }, indent=2) + "\n")
        print(f"   wrote {vercel_json}")

    middleware_js = os.path.join(out_dir, "middleware.js")
    if not os.path.exists(middleware_js):
        with open(middleware_js, "w", encoding="utf-8") as f:
            f.write(_BETA_MIDDLEWARE_JS)
        print(f"   wrote {middleware_js}")

    package_json = os.path.join(out_dir, "package.json")
    if not os.path.exists(package_json):
        with open(package_json, "w", encoding="utf-8") as f:
            f.write(json.dumps({
                "name": "emerging-edge-beta",
                "version": "1.0.0",
                "private": True,
            }, indent=2) + "\n")
        print(f"   wrote {package_json}")

    readme_path = os.path.join(out_dir, "README.md")
    if not os.path.exists(readme_path):
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(_BETA_README)
        print(f"   wrote {readme_path}")

    print()
    print("✅ Snapshot ready. To deploy:")
    print(f"   1. cd {out_dir}")
    print("   2. vercel --prod         # first run will prompt to link project")
    print("   3. On the Vercel dashboard for this project, set env vars:")
    print("        BETA_PASSWORD = <pick a password>")
    print("        BETA_USERNAME = beta   (optional, defaults to 'beta')")
    print("   4. Share the URL + password with your beta tester.")
    print()
    print("Re-run `python monitor.py publish` whenever you want to update")
    print("the snapshot, then `cd public && vercel --prod` to redeploy.")


# Vercel Edge Middleware: HTTP Basic Auth gate. Reads BETA_PASSWORD /
# BETA_USERNAME from the project's environment variables. If the
# password is unset the gate is open (handy for local `vercel dev`).
_BETA_MIDDLEWARE_JS = r"""// Vercel Edge Middleware — HTTP Basic Auth for the beta deploy.
// Set BETA_PASSWORD (and optionally BETA_USERNAME) on your Vercel
// project's Settings → Environment Variables. With BETA_PASSWORD
// unset the page is open (useful for local `vercel dev`).
export const config = {
  matcher: '/((?!_next|_vercel|favicon.ico).*)',
};

export default function middleware(req) {
  const password = process.env.BETA_PASSWORD || '';
  if (!password) return; // gate open

  const username = process.env.BETA_USERNAME || 'beta';
  const auth = req.headers.get('authorization') || '';

  let provided = '';
  if (auth.startsWith('Basic ')) {
    try {
      provided = atob(auth.slice(6));
    } catch (_) { /* fall through */ }
  }
  const expected = username + ':' + password;
  if (provided !== expected) {
    return new Response('Authentication required', {
      status: 401,
      headers: {
        'WWW-Authenticate': 'Basic realm="Emerging Edge Beta"',
        'Content-Type': 'text/plain; charset=utf-8',
      },
    });
  }
}
"""

_BETA_README = """# Emerging Edge — beta snapshot

This directory is a self-contained Vercel deploy of a single, view-only
snapshot of the Emerging Edge Monitor. Re-generate with
`python monitor.py publish` from the repo root.

## First-time deploy

    cd public
    vercel --prod

Vercel prompts to log in / link a new project on first run.

## Password protection

Set two environment variables on your Vercel project
(Settings → Environment Variables):

- `BETA_PASSWORD` — required. Without it the gate is open.
- `BETA_USERNAME` — optional. Defaults to `beta`.

Re-deploy after changing env vars (`vercel --prod` again) so the
middleware picks them up.

## Updating the snapshot

    python monitor.py publish     # regenerates public/index.html
    cd public && vercel --prod    # redeploys

Each redeploy is a fresh static snapshot. Beta testers can read but
cannot refresh, add stocks, edit settings, or see the Portfolio /
Engine Room pages.
"""


def cmd_upcoming(args, config: dict, db: Database):
    """Show earnings calendar."""
    print_upcoming(db, config)


def cmd_portfolio(args, config: dict, db: Database):
    """Portfolio tracking — import, show, or clear transactions."""
    subcmd = args.portfolio_cmd

    if subcmd == "import":
        filepath = args.file
        if not os.path.exists(filepath):
            print(f"❌ File not found: {filepath}")
            return
        import_transactions_csv(filepath, db, config)

    elif subcmd == "show":
        fp = save_portfolio_html(db, config)
        print(f"📊 Portfolio page: {fp}")
        webbrowser.open(f"file://{os.path.abspath(fp)}")

    elif subcmd == "clear":
        db.clear_transactions()
        print("🗑️  All portfolio transactions cleared")

    else:
        print("Usage: python monitor.py portfolio {import|show|clear}")


def cmd_serve(args, config: dict, db: Database):
    """
    Start a local web server that serves the dashboard and provides
    API endpoints for refreshing data from the browser.

    Endpoints:
      GET  /             → redirects to today's dashboard
      GET  /daily_*.html → serves the dashboard HTML
      POST /api/refresh   → re-fetches all data, regenerates dashboard, returns new HTML path
      POST /api/regen     → just regenerates dashboard from existing DB data (fast)
      GET  /api/status    → returns JSON with last refresh time and stock count

    Multi-user mode (env var MULTI_USER=1):
      Each request resolves the logged-in user from the ee_session
      cookie and routes DB access to that user's per-user SQLite at
      $EE_DATA_DIR/u_<user_id>.db. Per-user Serper key is set from
      the same DB at request time. /signup /login /logout are public;
      everything else 302→/login if there's no session.
    """
    port = args.port if hasattr(args, "port") and args.port else 8878
    import auth as _auth
    multiuser = _auth.is_multiuser()

    # In multi-user mode, every handler method should operate on the
    # CURRENT user's DB. Rather than rewriting all ~50 `db` references
    # throughout the handler, rebind `db` to a transparent proxy that
    # forwards every attribute access to a thread-local Database
    # instance set at the start of each request.
    import threading as _th
    _request_local = _th.local()
    _shared_db = db
    if multiuser:
        class _DBProxy:
            def __getattr__(self, name):
                actual = getattr(_request_local, "db", None) or _shared_db
                return getattr(actual, name)
            def __setattr__(self, name, value):
                actual = getattr(_request_local, "db", None) or _shared_db
                setattr(actual, name, value)
        db = _DBProxy()  # type: ignore[assignment]

    # Load any DB-stored Serper API key as an override so subsequent
    # _call_serper calls use the user's key (managed from the Engine Room).
    # In multi-user mode the global key is unused; every request applies
    # its own user's key via _set_serper_for_user() in do_GET / do_POST.
    try:
        import fetchers as _f
        stored_key = _shared_db.get_setting("serper_api_key", "")
        if stored_key and not multiuser:
            _f.set_serper_api_key(stored_key)
    except Exception as _e:
        print(f"⚠️  could not load stored Serper key: {_e}")

    # Generate initial dashboard
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    digest_dir = config.get("digest_dir", "./digests")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    abs_digest_dir = os.path.join(script_dir, digest_dir) if not os.path.isabs(digest_dir) else digest_dir

    def _invalidate_monitor_cache(dir_path: str):
        """No-op. /monitor generates HTML fresh in-memory on every request,
        so there is no cached file to invalidate. Kept as a stub so existing
        call sites compile without churn."""
        pass

    filepath = save_html(db, config, today)
    print(f"🌐 Initial dashboard: {filepath}")

    # Track state — progress is a dict updated in real-time by background threads
    state = {
        "last_refresh": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "refreshing": False,
        "refresh_mode": "",    # "free" or "full" — set when a refresh starts
        "progress": {          # detailed progress for the UI
            "step": "",        # current step: "news", "contracts", "earnings", "forum", "price", "insider", "generating"
            "ticker": "",      # current stock ticker
            "done": 0,         # stocks completed
            "total": 0,        # total stocks to process
            "error": "",       # last error message (empty = no error)
        },
    }

    class DashboardHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=abs_digest_dir, **kw)

        # ── Multi-user request setup ────────────────────────────────
        # _setup_request returns the user dict on success, None if the
        # request should be redirected to /login (already sent), or
        # False if it's a public route that doesn't need auth (handled
        # below in do_GET/do_POST before this is called).
        def _setup_request(self):
            """Resolve session, set thread-local DB and Serper key.
            Returns:
              dict   — authenticated user; proceed
              None   — redirect already sent (no session); stop
              "skip" — single-user mode; carry on with shared db
            """
            if not multiuser:
                return "skip"
            cookie = self.headers.get("Cookie", "")
            token = _auth.parse_session_token(cookie)
            user = _auth.resolve_session(token) if token else None
            if not user:
                self.send_response(302)
                self.send_header("Location", "/login")
                self.end_headers()
                return None
            from db import Database as _DB
            user_db = _DB(_auth.user_db_path(user["id"]))
            _request_local.db = user_db
            # Per-user Serper key for the duration of this request
            try:
                import fetchers as _f
                key = user_db.get_setting("serper_api_key", "")
                _f.set_serper_api_key(key or "")
            except Exception:
                pass
            return user

        def _teardown_request(self):
            if multiuser:
                try:
                    actual = getattr(_request_local, "db", None)
                    if actual is not None:
                        actual.conn.close()
                except Exception:
                    pass
                _request_local.db = None
                try:
                    import fetchers as _f
                    _f.set_serper_api_key("")
                except Exception:
                    pass

        def _is_public_route(self, path: str) -> bool:
            """Routes that bypass the auth gate."""
            return path in ("/login", "/signup", "/logout", "/healthz") \
                   or path.startswith("/static/") \
                   or path.startswith("/logos/")

        def _reconnect_db(self):
            """Reopen the SQLite connection after a sleep/wake or error.
            Mutates the enclosing `db` via its `conn` attribute."""
            try:
                db.conn.close()
            except Exception:
                pass
            try:
                db.conn = sqlite3.connect(config.get("db_path", "emerging_edge.db"),
                                          check_same_thread=False)
                db.conn.row_factory = sqlite3.Row
                print("  🔄 DB reconnected")
            except Exception as e:
                print(f"  ⚠ DB reconnect failed: {e}")

        # ── Public auth pages ──────────────────────────────────────
        def _serve_html(self, html: str, status: int = 200,
                        extra_headers: list | None = None):
            data = html.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            for k, v in (extra_headers or []):
                self.send_header(k, v)
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def _read_form(self) -> dict:
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length).decode("utf-8", errors="replace")
            try:
                return {k: v[0] for k, v in
                        urllib.parse.parse_qs(raw, keep_blank_values=True).items()}
            except Exception:
                return {}

        def _is_secure_request(self) -> bool:
            """Best-effort detect whether the deploy is HTTPS so the
            session cookie's Secure flag is correct (browsers reject
            Secure cookies over plain HTTP)."""
            xfp = (self.headers.get("X-Forwarded-Proto") or "").lower()
            if xfp:
                return xfp == "https"
            # Local dev / Fly internal traffic over plain HTTP
            return False

        def _handle_auth_route(self, parsed):
            """Return True if request was handled (login/signup/logout)."""
            if parsed.path == "/login":
                if self.command == "GET":
                    self._serve_html(_auth.render_login_page())
                    return True
                # POST /login
                form = self._read_form()
                try:
                    user_id, token = _auth.login(
                        form.get("email", ""), form.get("password", ""))
                except _auth.AuthError as e:
                    self._serve_html(
                        _auth.render_login_page(str(e), form.get("email", "")),
                        status=200)
                    return True
                self.send_response(302)
                self.send_header("Location", "/monitor")
                self.send_header("Set-Cookie",
                    _auth.cookie_set(token, secure=self._is_secure_request()))
                self.end_headers()
                return True

            if parsed.path == "/signup":
                if self.command == "GET":
                    self._serve_html(_auth.render_signup_page())
                    return True
                form = self._read_form()
                try:
                    user_id, token = _auth.signup(
                        form.get("email", ""), form.get("password", ""))
                except _auth.AuthError as e:
                    self._serve_html(
                        _auth.render_signup_page(str(e), form.get("email", "")),
                        status=200)
                    return True
                # New user — kick off DB schema by touching it
                from db import Database as _DB
                _DB(_auth.user_db_path(user_id)).conn.close()
                self.send_response(302)
                self.send_header("Location", "/monitor")
                self.send_header("Set-Cookie",
                    _auth.cookie_set(token, secure=self._is_secure_request()))
                self.end_headers()
                return True

            if parsed.path == "/logout":
                cookie = self.headers.get("Cookie", "")
                token = _auth.parse_session_token(cookie)
                _auth.logout(token)
                self.send_response(302)
                self.send_header("Location", "/login")
                self.send_header("Set-Cookie",
                    _auth.cookie_clear(secure=self._is_secure_request()))
                self.end_headers()
                return True

            if parsed.path == "/healthz":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"ok")
                return True

            return False

        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)

            # Auth + multi-user routes (only meaningful when multiuser=True;
            # in single-user mode /login etc. are 404s as before)
            if multiuser:
                if self._handle_auth_route(parsed):
                    return
                if not self._is_public_route(parsed.path):
                    user = self._setup_request()
                    if user is None:
                        return  # redirect to /login already sent

            try:
                self._do_GET_inner(parsed)
            finally:
                self._teardown_request()

        def _do_GET_inner(self, parsed):
            if parsed.path == "/":
                # Land on the Monitor — the headline page. Used to be
                # Portfolio but the Monitor is what newcomers should
                # see first (Welcome card → Add your first stock).
                self.send_response(302)
                self.send_header("Location", "/monitor")
                self.end_headers()
                return

            if parsed.path == "/admin/users" and multiuser:
                # Owner-only signup audit list. Gated to a single email
                # configured via the EE_ADMIN_EMAIL env var (default:
                # martinmsjogren@gmail.com).
                user = _request_local.__dict__.get("user")
                # _setup_request stored the user; if missing, recompute
                if not user:
                    cookie = self.headers.get("Cookie", "")
                    token = _auth.parse_session_token(cookie)
                    user = _auth.resolve_session(token) if token else None
                admin_email = os.environ.get(
                    "EE_ADMIN_EMAIL", "martinmsjogren@gmail.com").strip().lower()
                if not user or (user.get("email", "").strip().lower()
                                != admin_email):
                    self.send_response(403)
                    self.send_header("Content-Type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"403 - admin only")
                    return
                # Build a list of users + last-seen + DB size + watchlist count
                rows_html = []
                conn = _auth._users_conn()
                user_rows = conn.execute("""
                    SELECT u.id, u.email, u.created_at,
                           (SELECT MAX(s.last_seen_at) FROM sessions s
                              WHERE s.user_id = u.id) AS last_seen,
                           (SELECT COUNT(*) FROM sessions s
                              WHERE s.user_id = u.id) AS sessions
                    FROM users u
                    ORDER BY u.created_at DESC
                """).fetchall()
                conn.close()
                from db import Database as _DB
                for r in user_rows:
                    db_path = _auth.user_db_path(r["id"])
                    size_kb = "—"
                    nstocks = "—"
                    try:
                        size_kb = f"{os.path.getsize(db_path)//1024:,} KB"
                        u_db = _DB(db_path)
                        n = u_db.conn.execute(
                            "SELECT COUNT(*) FROM user_stocks").fetchone()[0]
                        nstocks = str(n)
                        u_db.conn.close()
                    except Exception:
                        pass
                    rows_html.append(
                        f"<tr><td>{r['id']}</td>"
                        f"<td>{_html_escape(r['email'])}</td>"
                        f"<td>{_html_escape(r['created_at'])}</td>"
                        f"<td>{_html_escape(r['last_seen'] or '—')}</td>"
                        f"<td>{r['sessions']}</td>"
                        f"<td>{nstocks}</td>"
                        f"<td class='mono'>{size_kb}</td></tr>"
                    )
                page = f"""<!DOCTYPE html><html><head>
<meta charset="utf-8"><title>Users — Emerging Edge</title>
<style>
body{{background:#0f1117;color:#e2e4ea;font-family:-apple-system,Segoe UI,
sans-serif;margin:0;padding:2rem}}
h1{{margin:0 0 0.4rem 0;font-size:1.4rem}}
h1 span{{color:#6c8cff}}
.muted{{color:#8b8fa3;font-size:0.85rem;margin-bottom:1.5rem}}
table{{width:100%;border-collapse:collapse;font-size:0.85rem}}
th{{text-align:left;padding:0.5rem 0.7rem;border-bottom:2px solid #2d3040;
color:#8b8fa3;font-weight:600;text-transform:uppercase;font-size:0.7rem;
letter-spacing:0.04em}}
td{{padding:0.55rem 0.7rem;border-bottom:1px solid #1f222e}}
tr:hover{{background:#1a1d27}}
.mono{{font-family:ui-monospace,Menlo,monospace}}
a{{color:#6c8cff;text-decoration:none}}
.actions{{margin-top:1.5rem;font-size:0.78rem;color:#8b8fa3}}
.actions code{{background:#1a1d27;padding:2px 6px;border-radius:4px}}
</style></head><body>
<h1><span>Emerging Edge</span> · Users</h1>
<div class="muted">{len(user_rows)} signup{('s' if len(user_rows)!=1 else '')} ·
admin <code style="color:#6c8cff">{_html_escape(user['email'])}</code> ·
<a href="/monitor">→ back to Monitor</a></div>
<table><thead><tr>
<th>ID</th><th>Email</th><th>Signed up (UTC)</th>
<th>Last seen (UTC)</th><th>Sessions</th><th>Stocks</th><th>DB size</th>
</tr></thead><tbody>
{''.join(rows_html) if rows_html else '<tr><td colspan="7" class="muted" style="padding:1rem">No signups yet.</td></tr>'}
</tbody></table>
<div class="actions">Reset a user (CLI):
<code>fly ssh console -a emerging-edge -C "rm /data/u_&lt;id&gt;.db"</code>
then have them sign in again — schema auto-recreates empty.
</div></body></html>"""
                self._serve_html(page)
                return

            if parsed.path in ("/monitor", "/emergingedge"):
                # Serve today's dashboard. Generate fresh content in-memory
                # on every request to avoid a race: the /api/watchlist/add
                # bg thread calls _invalidate_monitor_cache (os.remove) and
                # would occasionally delete daily_{t}.html between a prior
                # save_html writing it and do_GET opening it.
                t = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                content = None
                try:
                    content = generate_html(db, config, t)
                except Exception:
                    traceback.print_exc()
                    self._reconnect_db()
                    try:
                        content = generate_html(db, config, t)
                    except Exception as e2:
                        traceback.print_exc()
                        self.send_error(500, f"Monitor unavailable: {e2}")
                        return
                # Also persist to disk (best-effort) so the CLI export
                # and digest scripts still see today's daily file.
                try:
                    os.makedirs(abs_digest_dir, exist_ok=True)
                    with open(os.path.join(abs_digest_dir, f"daily_{t}.html"),
                              "w", encoding="utf-8") as f:
                        f.write(content)
                except OSError:
                    pass  # serving works without the file
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                    self.send_header("Pragma", "no-cache")
                    self.end_headers()
                    self.wfile.write(content.encode("utf-8"))
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

            if parsed.path == "/portfolio":
                # Regenerate and serve portfolio page on each request.
                # Retry once after reconnecting the DB on failure.
                fp = None
                try:
                    fp = save_portfolio_html(db, config)
                except Exception:
                    traceback.print_exc()
                    self._reconnect_db()
                    try:
                        fp = save_portfolio_html(db, config)
                    except Exception as e2:
                        traceback.print_exc()
                        self.send_error(500, f"Portfolio error: {e2}")
                        return
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                    self.send_header("Pragma", "no-cache")
                    self.end_headers()
                    with open(fp, "rb") as f:
                        self.wfile.write(f.read())
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

            if parsed.path == "/screener":
                # Regenerate screener on each request. Country is taken
                # from the ?country=... query param. ?refresh=1 forces a
                # re-fetch from stockanalysis.com.
                country = None
                refresh = False
                qs = urllib.parse.parse_qs(parsed.query or "")
                if "country" in qs and qs["country"]:
                    country = qs["country"][0]
                if qs.get("refresh", [""])[0] in ("1", "true", "yes"):
                    refresh = True
                fp = None
                try:
                    fp = save_screener_html(db, config, country, refresh=refresh)
                except Exception:
                    traceback.print_exc()
                    self._reconnect_db()
                    try:
                        fp = save_screener_html(db, config, country, refresh=refresh)
                    except Exception as e2:
                        traceback.print_exc()
                        self.send_error(500, f"Screener error: {e2}")
                        return
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                    self.send_header("Pragma", "no-cache")
                    self.end_headers()
                    with open(fp, "rb") as f:
                        self.wfile.write(f.read())
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

            if parsed.path == "/engine-room":
                # Regenerate engine room status page on each request.
                fp = None
                try:
                    fp = save_engine_room_html(db, config)
                except Exception:
                    traceback.print_exc()
                    self._reconnect_db()
                    try:
                        fp = save_engine_room_html(db, config)
                    except Exception as e2:
                        traceback.print_exc()
                        self.send_error(500, f"Engine room error: {e2}")
                        return
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                    self.send_header("Pragma", "no-cache")
                    self.end_headers()
                    with open(fp, "rb") as f:
                        self.wfile.write(f.read())
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

            if parsed.path == "/api/status":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "last_refresh": state["last_refresh"],
                    "refreshing": state["refreshing"],
                    "refresh_mode": state.get("refresh_mode", ""),
                    "stocks": len(get_active_stocks(db, config)),
                    "progress": state["progress"],
                }).encode())
                return

            if parsed.path == "/api/stock-search":
                # Query param: ?q=<query>
                try:
                    params = urllib.parse.parse_qs(parsed.query)
                    q = (params.get("q", [""])[0] or "").strip()
                    if len(q) < 2:
                        self._json_response({"results": []})
                        return
                    results = search_stocks(q, limit=10)
                    self._json_response({"results": results})
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 400)
                return

            # Serve logo images from /logos/ path
            if parsed.path.startswith("/logos/"):
                # Strip query string for file lookup
                clean_path = parsed.path.split("?")[0]
                logo_file = os.path.join(os.path.dirname(__file__) or ".", clean_path.lstrip("/"))
                if os.path.isfile(logo_file):
                    ext = os.path.splitext(logo_file)[1].lower()
                    mime = {'.png': 'image/png', '.jpg': 'image/jpeg',
                            '.jpeg': 'image/jpeg', '.svg': 'image/svg+xml',
                            '.webp': 'image/webp'}.get(ext, 'image/png')
                    self.send_response(200)
                    self.send_header("Content-Type", mime)
                    self.send_header("Cache-Control", "public, max-age=3600")
                    self.end_headers()
                    with open(logo_file, "rb") as f:
                        self.wfile.write(f.read())
                    return

            # Serve static files from digest dir
            super().do_GET()

        def do_POST(self):
            parsed = urllib.parse.urlparse(self.path)

            if multiuser:
                if self._handle_auth_route(parsed):
                    return
                if not self._is_public_route(parsed.path):
                    user = self._setup_request()
                    if user is None:
                        return  # redirect already sent

            try:
                self._do_POST_inner(parsed)
            finally:
                self._teardown_request()

        def _do_POST_inner(self, parsed):
            if parsed.path == "/api/refresh":
                if state["refreshing"]:
                    self._json_response({"status": "busy", "message": "Refresh already in progress"})
                    return

                # POST body may include:
                #   force: bool — override staleness thresholds
                #   mode:  "full" | "free" — "free" disables Serper for this run
                force = False
                mode = "full"
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    if length > 0:
                        body = json.loads(self.rfile.read(length))
                        force = body.get("force", False)
                        mode = (body.get("mode") or "full").lower()
                except Exception:
                    pass
                free_only = (mode == "free")

                if force:
                    # Temporarily set staleness to 0 to force re-fetch
                    import fetchers
                    fetchers.STALE_NEWS_HOURS = 0
                    fetchers.STALE_CONTRACTS_HOURS = 0
                    fetchers.STALE_INSIDER_HOURS = 0
                    fetchers.STALE_FORUM_HOURS = 0

                state["refreshing"] = True
                state["refresh_mode"] = mode
                state["progress"] = {"step": "starting", "ticker": "", "done": 0,
                                     "total": len(get_active_stocks(db, config)), "error": ""}
                if free_only:
                    msg = "Refreshing from free sources only (no Serper credits)..."
                else:
                    msg = "Force-fetching all data..." if force else "Fetching new data (skipping fresh)..."
                self._json_response({"status": "started", "mode": mode, "message": msg})

                # Capture the per-request DB + Serper key so the
                # background thread sees the right user. The
                # _request_local proxy is thread-local; the bg thread
                # would otherwise fall back to the shared global DB.
                _captured_db = (
                    getattr(_request_local, "db", None) if multiuser else None)
                _captured_serper_key = ""
                if multiuser and _captured_db is not None:
                    try:
                        _captured_serper_key = _captured_db.get_setting(
                            "serper_api_key", "") or ""
                    except Exception:
                        pass

                def do_refresh():
                    import fetchers as _f
                    from fetchers import (fetch_news, fetch_contracts, fetch_earnings,
                                          fetch_forums, fetch_prices, fetch_insiders)
                    # Re-establish the per-user DB + Serper key in this
                    # background thread so the proxy resolves to the
                    # right user's data.
                    if multiuser and _captured_db is not None:
                        _request_local.db = _captured_db
                        _f.set_serper_api_key(_captured_serper_key)
                    if free_only:
                        _f.set_serper_enabled(False)
                    stocks = get_active_stocks(db, config)
                    prog = state["progress"]
                    prog["total"] = len(stocks)
                    if free_only:
                        # Skip steps that are 100% Serper (contracts) so the
                        # UI doesn't show them running for no effect. news /
                        # forums / earnings / insiders still run because they
                        # have free backends in addition to Serper.
                        steps = [
                            ("prices", fetch_prices),
                            ("news", fetch_news),
                            ("earnings", fetch_earnings),
                            ("insiders", fetch_insiders),
                            ("forums", fetch_forums),
                        ]
                    else:
                        steps = [
                            ("news", fetch_news), ("contracts", fetch_contracts),
                            ("earnings", fetch_earnings), ("forums", fetch_forums),
                            ("prices", fetch_prices), ("insiders", fetch_insiders),
                        ]
                    try:
                        for i, stock in enumerate(stocks):
                            tk = stock["ticker"]
                            prog["done"] = i
                            for step_name, step_fn in steps:
                                prog["step"] = step_name
                                prog["ticker"] = tk
                                try:
                                    step_fn(stock, db, config)
                                except Exception as e:
                                    print(f"  {step_name} failed for {tk}: {e}")
                            prog["done"] = i + 1

                        prog["step"] = "generating"
                        prog["ticker"] = ""
                        t = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        save_digest(db, config, t)
                        save_html(db, config, t)
                        state["last_refresh"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                        prog["step"] = "done"
                        print(f"✅ Refresh complete at {state['last_refresh']} (mode={mode})")
                    except Exception as e:
                        prog["error"] = str(e)[:200]
                        print(f"❌ Refresh failed: {e}")
                    finally:
                        state["refreshing"] = False
                        state["refresh_mode"] = ""
                        # Restore staleness thresholds if they were overridden
                        _f.STALE_NEWS_HOURS = 48
                        _f.STALE_CONTRACTS_HOURS = 168
                        _f.STALE_INSIDER_HOURS = 168
                        _f.STALE_FORUM_HOURS = 168
                        # Always re-enable Serper — free mode is per-run
                        _f.set_serper_enabled(True)

                threading.Thread(target=do_refresh, daemon=True).start()
                return

            if parsed.path == "/api/refresh-prices":
                if state["refreshing"]:
                    self._json_response({"status": "busy", "message": "Refresh already in progress"})
                    return

                # Read optional exchange filter from POST body
                exchange_filter = None
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    if length > 0:
                        body = json.loads(self.rfile.read(length))
                        exchange_filter = body.get("exchange")
                except Exception:
                    pass

                state["refreshing"] = True
                label = exchange_filter or "all"
                price_stocks = [s for s in get_active_stocks(db, config)
                                if not exchange_filter or s["exchange"] == exchange_filter]
                state["progress"] = {"step": "prices", "ticker": "", "done": 0,
                                     "total": len(price_stocks), "error": ""}
                self._json_response({"status": "started", "message": f"Updating {label} prices..."})

                # Capture the per-request DB so the bg thread uses the
                # right user's DB when calling fetch_prices.
                _captured_db_pr = (
                    getattr(_request_local, "db", None) if multiuser else None)

                def do_price_refresh():
                    if multiuser and _captured_db_pr is not None:
                        _request_local.db = _captured_db_pr
                    prog = state["progress"]
                    try:
                        for i, s in enumerate(price_stocks):
                            prog["ticker"] = s["ticker"]
                            prog["done"] = i
                            try:
                                fetch_prices(s, db, config)
                            except Exception as e:
                                print(f"  Price failed for {s['ticker']}: {e}")
                            prog["done"] = i + 1
                        prog["step"] = "generating"
                        prog["ticker"] = ""
                        t = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        save_html(db, config, t)
                        state["last_refresh"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                        prog["step"] = "done"
                        print(f"✅ Price refresh ({label}) complete at {state['last_refresh']}")
                    except Exception as e:
                        prog["error"] = str(e)[:200]
                        print(f"❌ Price refresh failed: {e}")
                    finally:
                        state["refreshing"] = False

                threading.Thread(target=do_price_refresh, daemon=True).start()
                return

            if parsed.path == "/api/regen":
                # Quick regenerate from existing DB (no fetching)
                t = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                save_digest(db, config, t)
                fp = save_html(db, config, t)
                self._json_response({"status": "done", "file": os.path.basename(fp)})
                return

            if parsed.path == "/api/portfolio/import":
                # Import CSV from POST body
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = self.rfile.read(length).decode("utf-8-sig")

                    # Save to temp file and import
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv",
                                                     delete=False, encoding="utf-8") as tmp:
                        tmp.write(body)
                        tmp_path = tmp.name

                    from portfolio import import_transactions_csv
                    # Clear existing and re-import (full replace)
                    db.clear_transactions()
                    count = import_transactions_csv(tmp_path, db, config)
                    os.unlink(tmp_path)

                    self._json_response({
                        "status": "ok",
                        "imported": count,
                        "message": f"Imported {count} transactions"
                    })
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 500)
                return

            if parsed.path == "/api/portfolio/add":
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length))
                    txn_type = body["type"].strip().upper()
                    txn_date = body["date"].strip()

                    if txn_type not in ("BUY", "SELL", "DIVIDEND", "REINVEST", "CONVERT"):
                        self._json_response({"status": "error", "message": f"Invalid type: {txn_type}"}, 400)
                        return

                    if txn_type == "CONVERT":
                        from_currency = body["currency"].strip().upper()
                        from_amount = float(body["shares"])  # reuse shares column
                        to_currency = body["to_currency"].strip().upper()
                        to_amount = float(body["to_amount"])
                        if from_currency == to_currency:
                            self._json_response({"status": "error",
                                "message": "From and to currencies must differ"}, 400)
                            return
                        new_id = db.insert_transaction(
                            "_CASH_", "_CASH_", "CONVERT",
                            shares=from_amount, price=1.0,
                            currency=from_currency, txn_date=txn_date,
                            to_currency=to_currency, to_amount=to_amount)
                        resp = {"status": "ok", "new": bool(new_id)}
                        if new_id:
                            shortfall = compute_convert_shortfall(db, new_id)
                            if shortfall > 0:
                                resp["warning"] = (
                                    f"Convert exceeded available {from_currency} cash by "
                                    f"{from_currency} {shortfall:,.2f}. The shortfall "
                                    f"was treated as a new external deposit."
                                )
                        self._json_response(resp)
                        return

                    # Non-CONVERT path
                    ticker = body["ticker"].strip().upper()
                    exchange = body["exchange"].strip().upper()
                    shares = float(body["shares"])
                    price = float(body["price"])
                    currency = body["currency"].strip().upper()
                    new_id = db.insert_transaction(ticker, exchange, txn_type, shares, price, currency, txn_date)
                    resp = {"status": "ok", "new": bool(new_id)}
                    if new_id and txn_type == "REINVEST":
                        shortfall = compute_reinvest_shortfall(db, new_id)
                        if shortfall > 0:
                            resp["warning"] = (
                                f"Reinvest exceeded available cash by "
                                f"{currency} {shortfall:,.2f}. The shortfall "
                                f"was treated as a new external deposit, so "
                                f"Total Invested increased by that amount."
                            )
                    if new_id:
                        # Auto-add unknown stocks to the watchlist so the
                        # monitor and price fetcher know about them. Catches
                        # manual-entry submits and CSV imports for stocks
                        # that were never explicitly added via /api/watchlist/add.
                        active = {(s.get("ticker"), s.get("exchange"))
                                  for s in get_active_stocks(db, config)}
                        if (ticker, exchange) not in active:
                            from stock_search import get_exchange_defaults
                            defaults = get_exchange_defaults(exchange, ticker)
                            db.add_user_stock({
                                "ticker": ticker,
                                "exchange": exchange,
                                "name": ticker,  # fallback when no name supplied
                                "currency": currency,
                                "yahoo_ticker": "",
                                "lang": "en",
                                "forum_sources": defaults.get("forum_sources", []),
                                "earnings_source": defaults.get("earnings_source", ""),
                                "code": ticker,
                                "country": "",
                                "notes": "",
                                "price_url": defaults.get("price_url", ""),
                            })
                            _invalidate_monitor_cache(abs_digest_dir)
                        # Background price fetch if we don't have a snapshot yet.
                        if txn_type in ("BUY", "REINVEST") and not db.get_latest_price(ticker, exchange):
                            stock_meta = next(
                                (s for s in get_active_stocks(db, config)
                                 if s.get("ticker") == ticker
                                 and s.get("exchange") == exchange),
                                None,
                            )
                            if stock_meta:
                                def _bg_price():
                                    try:
                                        from fetchers import fetch_prices
                                        fetch_prices(stock_meta, db, config)
                                    except Exception as e:
                                        print(f"  bg price fetch failed for {ticker}: {e}")
                                threading.Thread(target=_bg_price, daemon=True).start()
                    self._json_response(resp)
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 400)
                return

            if parsed.path == "/api/portfolio/update":
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length))
                    txn_id = int(body["id"])
                    txn_type = body["type"].strip().upper()
                    txn_date = body["date"].strip()

                    if txn_type not in ("BUY", "SELL", "DIVIDEND", "REINVEST", "CONVERT"):
                        self._json_response({"status": "error", "message": f"Invalid type: {txn_type}"}, 400)
                        return

                    if txn_type == "CONVERT":
                        from_currency = body["currency"].strip().upper()
                        from_amount = float(body["shares"])
                        to_currency = body["to_currency"].strip().upper()
                        to_amount = float(body["to_amount"])
                        if from_currency == to_currency:
                            self._json_response({"status": "error",
                                "message": "From and to currencies must differ"}, 400)
                            return
                        updated = db.update_transaction(
                            txn_id, "_CASH_", "_CASH_", "CONVERT",
                            shares=from_amount, price=1.0,
                            currency=from_currency, txn_date=txn_date,
                            to_currency=to_currency, to_amount=to_amount)
                        if not updated:
                            self._json_response({"status": "error", "message": "Transaction not found"}, 404)
                            return
                        resp = {"status": "ok"}
                        shortfall = compute_convert_shortfall(db, txn_id)
                        if shortfall > 0:
                            resp["warning"] = (
                                f"Convert exceeded available {from_currency} cash by "
                                f"{from_currency} {shortfall:,.2f}. The shortfall "
                                f"was treated as a new external deposit."
                            )
                        self._json_response(resp)
                        return

                    # Non-CONVERT path
                    ticker = body["ticker"].strip().upper()
                    exchange = body["exchange"].strip().upper()
                    shares = float(body["shares"])
                    price = float(body["price"])
                    currency = body["currency"].strip().upper()
                    updated = db.update_transaction(txn_id, ticker, exchange, txn_type,
                                                    shares, price, currency, txn_date)
                    if not updated:
                        self._json_response({"status": "error", "message": "Transaction not found"}, 404)
                        return
                    resp = {"status": "ok"}
                    if txn_type == "REINVEST":
                        shortfall = compute_reinvest_shortfall(db, txn_id)
                        if shortfall > 0:
                            resp["warning"] = (
                                f"Reinvest exceeded available cash by "
                                f"{currency} {shortfall:,.2f}. The shortfall "
                                f"was treated as a new external deposit, so "
                                f"Total Invested increased by that amount."
                            )
                    self._json_response(resp)
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 400)
                return

            if parsed.path == "/api/portfolio/delete":
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length))
                    txn_id = int(body["id"])
                    db.conn.execute("DELETE FROM portfolio_transactions WHERE id = ?", (txn_id,))
                    db.conn.commit()
                    self._json_response({"status": "ok"})
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 400)
                return

            if parsed.path == "/api/portfolio/label":
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length))
                    ticker = body["ticker"].strip().upper()
                    label = body.get("label", "").strip().upper()
                    db.set_holding_label(ticker, label)
                    self._json_response({"status": "ok"})
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 400)
                return

            if parsed.path == "/api/watchlist/add":
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length))
                    # Minimal validation: require ticker + exchange
                    if not body.get("ticker") or not body.get("exchange"):
                        self._json_response({"status": "error",
                            "message": "ticker and exchange are required"}, 400)
                        return
                    # Auto-derive yahoo_ticker if the autocomplete /
                    # catalog hit didn't supply one (common for stocks
                    # added via the stockanalysis.com listing scrape —
                    # e.g. ART/JSE, BSH/WSE — which have no Yahoo
                    # metadata). Lets prices flow on the next refresh.
                    if not body.get("yahoo_ticker"):
                        try:
                            from stock_search import derive_yahoo_ticker
                            yt = derive_yahoo_ticker(
                                body.get("ticker", ""), body.get("exchange", ""))
                            if yt:
                                body["yahoo_ticker"] = yt
                        except Exception:
                            pass
                    added = db.add_user_stock(body)
                    # Invalidate cached dashboard HTML so the new stock shows up.
                    _invalidate_monitor_cache(abs_digest_dir)
                    count = len(db.get_user_stocks())

                    # Kick off a background fetch of today's price so the
                    # dashboard has data immediately. Don't fetch news/forums
                    # here — those would burn API credits and slow the response.
                    if added:
                        stock_meta = dict(body)
                        stock_meta["ticker"] = stock_meta["ticker"].strip().upper()
                        stock_meta["exchange"] = stock_meta["exchange"].strip().upper()

                        def _bg_fetch_price():
                            try:
                                from fetchers import fetch_prices
                                fetch_prices(stock_meta, db, config)
                                _invalidate_monitor_cache(abs_digest_dir)
                            except Exception as e:
                                print(f"  bg price fetch failed for {stock_meta.get('ticker')}: {e}")

                        threading.Thread(target=_bg_fetch_price, daemon=True).start()

                    self._json_response({"status": "ok", "added": added,
                                         "watchlist_size": count})
                except Exception as e:
                    traceback.print_exc()
                    self._json_response({"status": "error", "message": str(e)}, 400)
                return

            if parsed.path == "/api/watchlist/remove":
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length))
                    ticker = (body.get("ticker") or "").strip().upper()
                    exchange = (body.get("exchange") or "").strip().upper()
                    if not ticker or not exchange:
                        self._json_response({"status": "error",
                            "message": "ticker and exchange are required"}, 400)
                        return
                    removed = db.remove_user_stock(ticker, exchange)
                    _invalidate_monitor_cache(abs_digest_dir)
                    self._json_response({"status": "ok", "removed": removed})
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 400)
                return

            if parsed.path == "/api/fundamentals/save":
                # Upsert P/E, ROE, Growth for a ticker+exchange
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length))
                    ticker = (body.get("ticker") or "").strip().upper()
                    exchange = (body.get("exchange") or "").strip().upper()
                    if not ticker or not exchange:
                        self._json_response({"status": "error",
                            "message": "ticker and exchange are required"}, 400)
                        return
                    def _opt_float(v):
                        if v is None or v == "": return None
                        try: return float(v)
                        except (TypeError, ValueError): return None
                    ok = db.upsert_fundamentals(
                        ticker=ticker, exchange=exchange,
                        pe=_opt_float(body.get("pe")),
                        roe_pct=_opt_float(body.get("roe_pct")),
                        growth_pct=_opt_float(body.get("growth_pct")),
                        notes=(body.get("notes") or "").strip(),
                    )
                    self._json_response({"status": "ok" if ok else "error",
                                         "saved": ok})
                except Exception as e:
                    traceback.print_exc()
                    self._json_response({"status": "error", "message": str(e)}, 400)
                return

            if parsed.path == "/api/fundamentals/delete":
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length))
                    ticker = (body.get("ticker") or "").strip().upper()
                    exchange = (body.get("exchange") or "").strip().upper()
                    if not ticker or not exchange:
                        self._json_response({"status": "error",
                            "message": "ticker and exchange are required"}, 400)
                        return
                    removed = db.delete_fundamentals(ticker, exchange)
                    self._json_response({"status": "ok", "removed": removed})
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 400)
                return

            if parsed.path == "/api/settings/serper-key":
                # Save or clear the user's Serper API key. The key lives in
                # the app_settings table and is loaded on server startup.
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length)) if length else {}
                    new_key = (body.get("api_key") or "").strip()
                    db.set_setting("serper_api_key", new_key)
                    import fetchers as _f
                    _f.set_serper_api_key(new_key)
                    masked = (new_key[:4] + "…" + new_key[-4:]) if len(new_key) >= 10 else ("set" if new_key else "")
                    self._json_response({
                        "status": "ok",
                        "has_key": bool(new_key),
                        "masked": masked,
                    })
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 500)
                return

            if parsed.path == "/api/settings/telegram-channels":
                # Replace the entire telegram_channels mapping with
                # whatever the client sends. Stored as JSON in
                # app_settings. Structure: {"EXCHANGE": ["handle", ...]}.
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length)) if length else {}
                    channels = body.get("channels") or {}
                    if not isinstance(channels, dict):
                        self._json_response({"status": "error",
                            "message": "channels must be an object"}, 400)
                        return
                    # Normalize: uppercase exchange keys, strip @/t.me/ prefixes
                    import re as _re
                    clean: dict[str, list] = {}
                    for ex, handles in channels.items():
                        if not isinstance(handles, list):
                            continue
                        ex_key = str(ex).strip().upper()
                        if not ex_key:
                            continue
                        out = []
                        for h in handles:
                            s = str(h).strip()
                            # Accept pastes of "t.me/foo" or "@foo" or "https://t.me/foo"
                            s = _re.sub(r"^https?://", "", s)
                            s = _re.sub(r"^t\.me/", "", s)
                            s = s.lstrip("@").strip("/")
                            if s and _re.match(r"^[A-Za-z0-9_]{3,64}$", s):
                                out.append(s)
                        if out:
                            clean[ex_key] = out
                    db.set_setting("telegram_channels", json.dumps(clean))
                    self._json_response({"status": "ok", "channels": clean})
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 500)
                return

            if parsed.path == "/api/funds/manual-ingest":
                # Manually scan a URL (HTML or PDF) for watchlist mentions.
                # Used for blocked sources (Pangolin Asia, anything 403-ed).
                # Body: { "url": "...", "fund_id": "pangolin_asia",
                #         "fund_name": "Pangolin Asia", "report_date": "2025-09" }
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length)) if length else {}
                    url = (body.get("url") or "").strip()
                    fund_id = (body.get("fund_id") or "manual").strip().lower()
                    fund_name = (body.get("fund_name") or "Manual import").strip()
                    report_date = (body.get("report_date") or "").strip()
                    if not url:
                        self._json_response({"status": "error",
                            "message": "url is required"}, 400)
                        return
                    if not report_date:
                        # Try to parse a date from the filename. Patterns:
                        #   YYYYMM             → 202503
                        #   YYYY-MM / YYYY_MM  → 2025-03 / 2025_03
                        #   <Month>-YYYY       → March-2025
                        report_date = _derive_report_date(url)

                    import funds as _funds
                    # Pull bytes; if it looks like PDF (URL ends .pdf or content
                    # starts with %PDF-), parse with pypdf, else strip HTML.
                    raw = _funds._http_get_bytes(url)
                    if not raw:
                        self._json_response({"status": "error",
                            "message": "Could not fetch the URL"}, 502)
                        return
                    text = ""
                    if url.lower().endswith(".pdf") or raw[:5] == b"%PDF-":
                        text = _funds._extract_pdf_text(raw)
                    else:
                        try:
                            html = raw.decode("utf-8", errors="replace")
                        except Exception:
                            html = raw.decode("latin-1", errors="replace")
                        text = _funds._strip_html_to_text(html)
                    if not text:
                        self._json_response({"status": "error",
                            "message": "Could not extract any readable text"}, 422)
                        return

                    # Re-use the matcher (get_active_stocks comes from the
                    # module-level import at the top of monitor.py — do NOT
                    # re-import here, that shadows the name and triggers an
                    # UnboundLocalError in earlier branches of this method.)
                    aliases = _funds.get_aliases(db)
                    watchlist = get_active_stocks(db, config)
                    new_count = 0
                    hits: list[dict] = []
                    for stock in watchlist:
                        snippets = _funds._find_mentions(text, stock,
                                                        aliases=aliases)
                        if not snippets:
                            continue
                        joined = "  •  ".join(snippets)[:600]
                        stored = db.insert_fund_mention(
                            fund_id=fund_id, fund_name=fund_name,
                            report_date=report_date, report_url=url,
                            ticker=stock.get("ticker", ""),
                            exchange=stock.get("exchange", ""),
                            snippet=joined,
                        )
                        if stored:
                            new_count += 1
                            hits.append({
                                "ticker": stock.get("ticker"),
                                "exchange": stock.get("exchange"),
                                "snippet": joined[:200],
                            })
                    self._json_response({
                        "status": "ok",
                        "stored": new_count,
                        "text_length": len(text),
                        "hits": hits,
                    })
                except Exception as e:
                    self._json_response({"status": "error",
                        "message": str(e)}, 500)
                return

            if parsed.path == "/api/settings/fund-aliases":
                # Save the user's fund-mention alias map. Body shape:
                #   { "aliases": { "URTS:UZSE": ["Uzbek Commodity Exchange"], ... } }
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length)) if length else {}
                    aliases = body.get("aliases") or {}
                    if not isinstance(aliases, dict):
                        self._json_response({"status": "error",
                            "message": "aliases must be an object"}, 400)
                        return
                    from funds import set_aliases as _set_funds_aliases
                    _set_funds_aliases(db, aliases)
                    self._json_response({
                        "status": "ok",
                        "count": len(aliases),
                    })
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 500)
                return

            if parsed.path == "/api/settings/translate-skip-langs":
                # Save the user's "do not translate" language list. Each
                # element is a 2-letter code (e.g. "sv", "fr", "it"). Used
                # by translate.py to bypass translation for those langs.
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length)) if length else {}
                    langs = body.get("langs") or []
                    if not isinstance(langs, list):
                        self._json_response({"status": "error",
                            "message": "langs must be an array"}, 400)
                        return
                    from translate import set_skip_langs as _set_sk
                    _set_sk(db, langs)
                    self._json_response({
                        "status": "ok",
                        "langs": sorted({str(l).strip().lower() for l in langs if l}),
                    })
                except Exception as e:
                    self._json_response({"status": "error", "message": str(e)}, 500)
                return

            if parsed.path == "/api/catalog/refresh":
                # Re-scrape the public listing page for one exchange
                # and update frontier_stocks.json + catalog_meta.
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length)) if length else {}
                    exchange = (body.get("exchange") or "").strip().upper()
                    if not exchange:
                        self._json_response({"status": "error",
                            "message": "exchange is required"}, 400)
                        return
                    import catalog_updaters as _cu
                    if exchange not in _cu.UPDATERS:
                        self._json_response({"status": "error",
                            "message": f"unsupported exchange: {exchange}"}, 400)
                        return
                    ok, count, msg = _cu.refresh_exchange(exchange)
                    db.set_catalog_meta(
                        exchange, count,
                        status=("ok" if ok else "error"),
                        source_url=msg[:200])
                    self._json_response({
                        "status": "ok" if ok else "error",
                        "exchange": exchange,
                        "count": count,
                        "message": msg,
                    })
                except Exception as e:
                    self._json_response({"status": "error",
                        "message": str(e)}, 500)
                return

            if parsed.path == "/api/logo/upload":
                # Accept JSON: {ticker, filename, content_base64}
                # Writes to logos/{TICKER}.{ext}. Max 2 MB.
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    if length > 3 * 1024 * 1024:  # 3 MB JSON = ~2 MB binary
                        self._json_response({"status": "error",
                            "message": "File too large (max 2 MB)"}, 400)
                        return
                    body = json.loads(self.rfile.read(length))
                    ticker = (body.get("ticker") or "").strip().upper()
                    filename = (body.get("filename") or "").strip()
                    content_b64 = body.get("content_base64") or ""
                    if not ticker or not content_b64:
                        self._json_response({"status": "error",
                            "message": "ticker and content_base64 are required"}, 400)
                        return
                    # Sanitize ticker: uppercase alphanumerics only (allow - _ .)
                    import re as _re
                    if not _re.match(r"^[A-Z0-9._-]{1,32}$", ticker):
                        self._json_response({"status": "error",
                            "message": "Invalid ticker"}, 400)
                        return
                    # Pick extension from filename; restrict to known image types
                    ext = os.path.splitext(filename)[1].lower().lstrip(".")
                    if ext == "jpeg":
                        ext = "jpg"
                    if ext not in ("png", "jpg", "svg", "webp", "gif"):
                        self._json_response({"status": "error",
                            "message": "Unsupported format (png/jpg/svg/webp/gif)"}, 400)
                        return
                    import base64 as _b64
                    try:
                        binary = _b64.b64decode(content_b64, validate=True)
                    except Exception:
                        self._json_response({"status": "error",
                            "message": "Invalid base64 content"}, 400)
                        return
                    if len(binary) > 2 * 1024 * 1024:
                        self._json_response({"status": "error",
                            "message": "File too large (max 2 MB)"}, 400)
                        return
                    # Delete any existing logo files for this ticker first
                    # (we only want one logo per ticker, in any extension)
                    logos_dir = os.path.join(os.path.dirname(__file__) or ".", "logos")
                    os.makedirs(logos_dir, exist_ok=True)
                    for existing_ext in ("png", "jpg", "jpeg", "svg", "webp", "gif"):
                        old_path = os.path.join(logos_dir, f"{ticker}.{existing_ext}")
                        if os.path.exists(old_path):
                            try:
                                os.remove(old_path)
                            except OSError:
                                pass
                    # Write the new file
                    out_path = os.path.join(logos_dir, f"{ticker}.{ext}")
                    with open(out_path, "wb") as f:
                        f.write(binary)
                    self._json_response({"status": "ok",
                                         "path": f"/logos/{ticker}.{ext}",
                                         "size": len(binary)})
                except Exception as e:
                    traceback.print_exc()
                    self._json_response({"status": "error", "message": str(e)}, 400)
                return

            self.send_error(404)

        def _json_response(self, data, code=200):
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())

        def log_message(self, fmt, *a):
            # Quieter logging — only show API calls, not static file requests
            msg = str(a[0]) if a else ""
            if "/api/" in msg:
                print(f"  {msg}")

    # Kill any existing server on this port before starting. Local
    # convenience only — `lsof` isn't installed in slim Docker images
    # (Fly deploy) and isn't needed there since each Machine has a
    # clean process tree. Skip silently if it's missing.
    import subprocess as _sp, shutil as _shutil
    if _shutil.which("lsof"):
        try:
            pids = _sp.check_output(
                ["lsof", "-ti", f":{port}"], text=True).strip()
            if pids:
                for pid in pids.split("\n"):
                    try:
                        os.kill(int(pid), 9)
                    except (ProcessLookupError, ValueError):
                        pass
                import time; time.sleep(1)
                print(f"   Killed old process on port {port}")
        except _sp.CalledProcessError:
            pass  # no process on port — good

    # Bind to 0.0.0.0 but restrict access to Tailscale + localhost only
    # in single-user mode. Multi-user mode (hosted deploy) accepts all
    # connections — auth is enforced via the session cookie gate, and
    # the Fly / load-balancer proxy adds the public-internet edge.
    _ALLOWED_PREFIXES = ("127.0.0.1", "100.64.", "100.65.", "100.66.", "100.67.",
                         "100.68.", "100.69.", "100.70.", "100.71.", "100.72.",
                         "100.73.", "100.74.", "100.75.", "100.76.", "100.77.",
                         "100.78.", "100.79.", "100.80.", "100.81.", "100.82.",
                         "100.83.", "100.84.", "100.85.", "100.86.", "100.87.",
                         "100.88.", "100.89.", "100.90.", "100.91.", "100.92.",
                         "100.93.", "100.94.", "100.95.", "100.96.", "100.97.",
                         "100.98.", "100.99.", "100.100.", "100.101.", "100.102.",
                         "100.103.", "100.104.", "100.105.", "100.106.", "100.107.",
                         "100.108.", "100.109.", "100.110.", "100.111.", "100.112.",
                         "100.113.", "100.114.", "100.115.", "100.116.", "100.117.",
                         "100.118.", "100.119.", "100.120.", "100.121.", "100.122.",
                         "100.123.", "100.124.", "100.125.", "100.126.", "100.127.")

    class SecureHandler(DashboardHandler):
        """Only allow connections from localhost and Tailscale (100.64.0.0/10)."""
        def handle(self):
            if multiuser:
                # On Fly / hosted deploy, all traffic comes via the
                # platform proxy from arbitrary public IPs. Auth is
                # enforced by the session-cookie gate inside do_GET /
                # do_POST, not by IP allowlist.
                super().handle()
                return
            ip = self.client_address[0]
            if not ip.startswith(_ALLOWED_PREFIXES):
                self.request.close()
                return
            super().handle()

    server = http.server.HTTPServer(("0.0.0.0", port), SecureHandler)

    # Get Tailscale IP for display
    ts_ip = ""
    try:
        ts_ip = _sp.check_output(["tailscale", "ip", "-4"], text=True).strip()
    except Exception:
        pass

    url = f"http://localhost:{port}"
    ts_url = f"http://{ts_ip}:{port}" if ts_ip else ""
    print(f"\n🚀 Emerging Edge server running")
    print(f"   Local:     {url}/emergingedge")
    if ts_url:
        print(f"   Tailscale: {ts_url}/emergingedge")
    print(f"   Security:  localhost + Tailscale only (LAN blocked)")
    print(f"   Press Ctrl+C to stop\n")

    # Open in browser — local single-user dev only. Skipped on hosted
    # multi-user deploys (no display) and when the user explicitly
    # disables it via NO_BROWSER=1 (handy when running under nohup).
    if not multiuser and not os.environ.get("NO_BROWSER"):
        import webbrowser
        try:
            webbrowser.open(url)
        except Exception:
            pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Server stopped.")
        server.server_close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="emerging-edge",
        description="Frontier market stock monitor — news, contracts, earnings, forums",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("-c", "--config", default="config.json", help="Path to config file")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- run ---
    sub_run = subparsers.add_parser("run", help="Fetch all data now")

    # --- digest ---
    sub_digest = subparsers.add_parser("digest", help="Generate today's digest")
    sub_digest.add_argument("--date", help="Target date (YYYY-MM-DD), default: today")

    # --- html ---
    sub_html = subparsers.add_parser("html", help="Generate HTML dashboard and open in browser")
    sub_html.add_argument("--date", help="Target date (YYYY-MM-DD), default: today")

    # --- serve ---
    sub_serve = subparsers.add_parser("serve", help="Start local dashboard server with refresh button")
    sub_serve.add_argument("--port", type=int, default=8878, help="Port number (default: 8878)")

    # --- portfolio ---
    sub_portfolio = subparsers.add_parser("portfolio", help="Portfolio tracking")
    portfolio_sub = sub_portfolio.add_subparsers(dest="portfolio_cmd")
    p_import = portfolio_sub.add_parser("import", help="Import transactions CSV")
    p_import.add_argument("file", help="Path to transactions CSV")
    portfolio_sub.add_parser("show", help="Generate and open portfolio page")
    portfolio_sub.add_parser("clear", help="Clear all transactions")

    # --- upcoming ---
    sub_upcoming = subparsers.add_parser("upcoming", help="Show earnings calendar")

    # --- publish ---
    sub_publish = subparsers.add_parser(
        "publish",
        help="Build a view-only snapshot for Vercel beta-tester deploy")
    sub_publish.add_argument(
        "--out", default="./public",
        help="Output directory (default: ./public)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    setup_logging(args.verbose)

    # Load config relative to the script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, args.config) if not os.path.isabs(args.config) else args.config
    config = load_config(config_path)

    # Resolve db_path relative to script dir
    db_path = config.get("db_path", "./emerging_edge.db")
    if not os.path.isabs(db_path):
        db_path = os.path.join(script_dir, db_path)

    db = Database(db_path)

    try:
        commands = {
            "run": cmd_run,
            "digest": cmd_digest,
            "html": cmd_html,
            "serve": cmd_serve,
            "portfolio": cmd_portfolio,
            "upcoming": cmd_upcoming,
            "publish": cmd_publish,
        }
        commands[args.command](args, config, db)
    finally:
        db.close()


if __name__ == "__main__":
    main()
