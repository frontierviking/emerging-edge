"""
Engine Room — operational status page for the Emerging Edge.

Renders a single HTML page showing:
  - Server / watchdog status (uptime, restart count, DB size)
  - Backup status (last commit, file size, recent backup history)
  - Serper API credits + rate limit + recent search count
  - Source sites health (last successful fetch per data source)
  - Recent errors from the monitor log

This is mostly read-only — it queries the DB, reads log files, and makes
one HTTP call to Serper's /account endpoint for live credit balance.
"""

import html as html_mod
import json
import os
import re
import subprocess
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta

from db import Database


REPO_DIR = os.path.dirname(os.path.abspath(__file__))
WATCHDOG_LOG = "/tmp/emerging-edge-watchdog.log"
MONITOR_LOG = "/tmp/emerging-edge.log"
DB_BACKUP_LOG = "/tmp/emerging-edge-db-backup.log"


def _esc(text) -> str:
    return html_mod.escape(str(text)) if text is not None else ""


def _human_size(num_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:,.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:,.1f} TB"


def _human_age(timestamp_str: str) -> str:
    """Convert an ISO timestamp to '5 minutes ago' style text."""
    if not timestamp_str:
        return "never"
    try:
        # Handle both ISO with T and 'YYYY-MM-DD HH:MM:SS'
        ts = timestamp_str.replace("T", " ").rstrip("Z")
        # Strip microseconds if present
        if "." in ts:
            ts = ts.split(".")[0]
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        delta = datetime.utcnow() - dt
        secs = int(delta.total_seconds())
        if secs < 60:
            return f"{secs}s ago"
        if secs < 3600:
            return f"{secs // 60}m ago"
        if secs < 86400:
            return f"{secs // 3600}h ago"
        return f"{secs // 86400}d ago"
    except Exception:
        return timestamp_str


def _age_class(timestamp_str: str, fresh_hours: int = 6,
               stale_hours: int = 24) -> str:
    """Return a CSS class indicating freshness."""
    if not timestamp_str:
        return "stale"
    try:
        ts = timestamp_str.replace("T", " ").rstrip("Z")
        if "." in ts:
            ts = ts.split(".")[0]
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        delta = datetime.utcnow() - dt
        hours = delta.total_seconds() / 3600
        if hours < fresh_hours:
            return "fresh"
        if hours < stale_hours:
            return "warm"
        return "stale"
    except Exception:
        return "stale"


# ---------------------------------------------------------------------------
# Server status
# ---------------------------------------------------------------------------

def _server_status() -> dict:
    """Inspect running processes and watchdog log for server status."""
    info = {
        "server_pid": None,
        "watchdog_pid": None,
        "watchdog_started": None,
        "restart_count": 0,
        "last_restart": None,
        "db_size": 0,
        "db_size_human": "—",
    }

    # Find PIDs via pgrep — exclude self (we run inside the server)
    self_pid = os.getpid()
    try:
        out = subprocess.check_output(["/usr/bin/pgrep", "-f", "monitor.py serve"],
                                       text=True, timeout=2).strip()
        for line in out.split("\n"):
            line = line.strip()
            if line and int(line) != 0:
                # The first PID that's not us is the server (might be us if we're
                # the server, but that's also valid)
                info["server_pid"] = int(line)
                break
    except subprocess.CalledProcessError:
        pass  # pgrep returns 1 when no match
    except Exception as e:
        info["server_error"] = str(e)

    # Fallback: if we're running inside the server, use our own pid
    if info["server_pid"] is None:
        info["server_pid"] = self_pid

    try:
        out = subprocess.check_output(["/usr/bin/pgrep", "-f", "watchdog.sh"],
                                       text=True, timeout=2).strip()
        if out:
            info["watchdog_pid"] = int(out.split("\n")[0])
    except subprocess.CalledProcessError:
        pass
    except Exception:
        pass

    # Parse watchdog log for restart count + start time
    if os.path.exists(WATCHDOG_LOG):
        try:
            with open(WATCHDOG_LOG, "r") as f:
                lines = f.readlines()
            for line in lines:
                if "watchdog started" in line:
                    m = re.match(r"\[([\d\- :]+)\]", line)
                    if m:
                        info["watchdog_started"] = m.group(1)
                if "restarting server" in line or "starting server" in line:
                    info["restart_count"] += 1
                    m = re.match(r"\[([\d\- :]+)\]", line)
                    if m:
                        info["last_restart"] = m.group(1)
        except Exception:
            pass

    # DB file size
    db_path = os.path.join(REPO_DIR, "emerging_edge.db")
    if os.path.exists(db_path):
        info["db_size"] = os.path.getsize(db_path)
        info["db_size_human"] = _human_size(info["db_size"])

    return info


# ---------------------------------------------------------------------------
# Backup status
# ---------------------------------------------------------------------------

def _backup_status() -> dict:
    """Inspect git log and the SQL dump for backup status."""
    info = {
        "dump_exists": False,
        "dump_size_human": "—",
        "dump_modified": None,
        "last_backup_commit": None,
        "last_backup_age": "never",
        "recent_commits": [],
    }
    dump_path = os.path.join(REPO_DIR, "emerging_edge_backup.sql")
    if os.path.exists(dump_path):
        info["dump_exists"] = True
        info["dump_size_human"] = _human_size(os.path.getsize(dump_path))
        mtime = os.path.getmtime(dump_path)
        info["dump_modified"] = datetime.utcfromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")

    # Recent backup commits via git log
    try:
        out = subprocess.check_output(
            ["git", "log", "--pretty=format:%h|%cd|%s",
             "--date=format:%Y-%m-%d %H:%M",
             "--grep=Daily DB backup", "-n", "5"],
            cwd=REPO_DIR, text=True, timeout=5).strip()
        if out:
            for line in out.split("\n"):
                parts = line.split("|", 2)
                if len(parts) == 3:
                    info["recent_commits"].append({
                        "hash": parts[0],
                        "date": parts[1],
                        "subject": parts[2],
                    })
            if info["recent_commits"]:
                info["last_backup_commit"] = info["recent_commits"][0]["hash"]
                info["last_backup_age"] = _human_age(
                    info["recent_commits"][0]["date"] + ":00")
    except Exception:
        pass

    return info


# ---------------------------------------------------------------------------
# Serper API status
# ---------------------------------------------------------------------------

# Serper free tier: 2,500 credits at signup
SERPER_FREE_TIER = 2500


def _serper_status(db: Database) -> dict:
    """
    Call Serper /account for live balance and query the serper_calls table
    for accurate usage attribution by caller category.
    """
    info = {
        "balance": None,
        "rate_limit": None,
        "free_tier": SERPER_FREE_TIER,
        "credits_used": 0,
        "pct_used": 0,
        "queries_today": 0,
        "queries_week": 0,
        "queries_total": 0,
        "last_call_at": None,
        "by_caller_today": [],
        "by_caller_week": [],
        "by_caller_total": [],
        "by_ticker_week": [],
        "error": None,
    }
    api_key = os.environ.get("SERPER_API_KEY", "")
    if not api_key:
        info["error"] = "SERPER_API_KEY not set in env"
    else:
        try:
            req = urllib.request.Request(
                "https://google.serper.dev/account",
                headers={"X-API-KEY": api_key})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                info["balance"] = data.get("balance")
                info["rate_limit"] = data.get("rateLimit")
                if info["balance"] is not None:
                    info["credits_used"] = max(0, SERPER_FREE_TIER - info["balance"])
                    info["pct_used"] = round(100 * info["credits_used"] / SERPER_FREE_TIER, 1)
        except Exception as e:
            info["error"] = str(e)

    # Query the serper_calls table for proper attribution
    try:
        # Totals
        row = db.conn.execute(
            "SELECT COUNT(*) AS n, MAX(called_at) AS last FROM serper_calls WHERE ok = 1"
        ).fetchone()
        if row:
            info["queries_total"] = row["n"]
            info["last_call_at"] = row["last"]

        info["queries_today"] = db.conn.execute(
            "SELECT COUNT(*) FROM serper_calls "
            "WHERE ok = 1 AND called_at >= datetime('now', '-1 day')"
        ).fetchone()[0]
        info["queries_week"] = db.conn.execute(
            "SELECT COUNT(*) FROM serper_calls "
            "WHERE ok = 1 AND called_at >= datetime('now', '-7 days')"
        ).fetchone()[0]

        # By caller category — today / week / total
        for window_label, where in (("today", "AND called_at >= datetime('now', '-1 day')"),
                                     ("week", "AND called_at >= datetime('now', '-7 days')"),
                                     ("total", "")):
            rows = db.conn.execute(
                f"SELECT caller, COUNT(*) AS n FROM serper_calls "
                f"WHERE ok = 1 {where} GROUP BY caller ORDER BY n DESC"
            ).fetchall()
            info[f"by_caller_{window_label}"] = [
                {"caller": r["caller"], "count": r["n"]} for r in rows
            ]

        # Top tickers this week
        rows = db.conn.execute(
            "SELECT ticker, COUNT(*) AS n FROM serper_calls "
            "WHERE ok = 1 AND ticker != '' "
            "AND called_at >= datetime('now', '-7 days') "
            "GROUP BY ticker ORDER BY n DESC LIMIT 8"
        ).fetchall()
        info["by_ticker_week"] = [
            {"ticker": r["ticker"], "count": r["n"]} for r in rows
        ]
    except Exception as e:
        info["error"] = (info["error"] + "; " if info["error"] else "") + f"DB query: {e}"

    return info


# ---------------------------------------------------------------------------
# Source site health
# ---------------------------------------------------------------------------

# Cost classification — which forum/insider backends are direct-scraped (FREE)
# vs go through the Serper search API (PAID).
#
# Forums:
#   FREE — i3investor (page scrape), richbourse (page scrape),
#          telegram/* (t.me/s/ web preview, direct fetch)
#   PAID — twitter (serper_web_search site:x.com),
#          web (serper_discuss generic search),
#          anything else served by Serper organic results
#
# Insiders:
#   FREE — "KLSE Screener" (direct page scrape via _fetch_insiders_klse)
#   PAID — everything else (Serper organic results from fetch_insiders)

_FREE_FORUM_BACKENDS = {"i3investor", "richbourse"}  # exact match
_FREE_INSIDER_SOURCES = {"klse screener", "sec edgar"}  # exact, case-insensitive
_FREE_NEWS_SOURCES = {"yahoo finance"}                  # exact, case-insensitive


def _is_paid_news(source_value: str) -> bool:
    """news_items.source column: classify as paid (Serper) or free."""
    s = (source_value or "").strip().lower()
    return s not in _FREE_NEWS_SOURCES


def _is_paid_forum(forum_value: str) -> bool:
    """forum_mentions.forum column: classify as paid (Serper) or free."""
    f = (forum_value or "").strip().lower()
    if f in _FREE_FORUM_BACKENDS:
        return False
    if f.startswith("telegram/") or f.startswith("telegram:"):
        return False
    return True  # twitter, web, and any Serper-sourced value


def _is_paid_insider(source_value: str) -> bool:
    """insider_transactions.source column: classify as paid or free."""
    s = (source_value or "").strip().lower()
    return s not in _FREE_INSIDER_SOURCES


def _source_health(db: Database) -> list[dict]:
    """Query each data source to determine when it last produced data."""
    sources = []

    # Price sources — all FREE (direct scrapes / Yahoo)
    price_sources = [
        ("Yahoo Finance (NASDAQ)", "NASDAQ", False),
        ("Yahoo Finance (KLSE)", "KLSE", False),
        ("Yahoo Finance (JSE)", "JSE", False),
        ("Yahoo Finance (SGX)", "SGX", False),
        ("BRVM (West Africa)", "BRVM", False),
        ("NGX (TradingView)", "NGX", False),
        ("UZSE (stockscope.uz)", "UZSE", False),
        ("KSE Kyrgyzstan (kse.kg)", "KSE", False),
    ]
    for label, exchange, paid in price_sources:
        row = db.conn.execute(
            "SELECT MAX(snapshot_at) AS last, COUNT(DISTINCT ticker) AS tickers "
            "FROM price_snapshots WHERE exchange = ?", (exchange,)
        ).fetchone()
        if row and row["last"]:
            sources.append({
                "category": "Prices",
                "name": label,
                "last": row["last"],
                "count": row["tickers"],
                "unit": "tickers",
                "paid": paid,
            })

    # News sources — Yahoo Finance is FREE (RSS), everything else is PAID (Serper)
    rows = db.conn.execute("""
        SELECT source, COUNT(*) AS cnt, MAX(fetched_at) AS last
        FROM news_items
        WHERE fetched_at >= datetime('now', '-30 days') AND source != ''
        GROUP BY source ORDER BY cnt DESC LIMIT 15
    """).fetchall()
    for r in rows:
        sources.append({
            "category": "News",
            "name": r["source"],
            "last": r["last"],
            "count": r["cnt"],
            "unit": "items (30d)",
            "paid": _is_paid_news(r["source"]),
        })

    # Forum sources — i3investor / richbourse / telegram/* are FREE (direct scrape)
    # twitter and 'web' (serper_discuss) are PAID
    rows = db.conn.execute("""
        SELECT forum AS source, COUNT(*) AS cnt, MAX(fetched_at) AS last
        FROM forum_mentions
        WHERE fetched_at >= datetime('now', '-30 days') AND forum != ''
        GROUP BY forum ORDER BY cnt DESC LIMIT 10
    """).fetchall()
    for r in rows:
        sources.append({
            "category": "Forums",
            "name": r["source"],
            "last": r["last"],
            "count": r["cnt"],
            "unit": "posts (30d)",
            "paid": _is_paid_forum(r["source"]),
        })

    # Insider sources — group by source when present, otherwise by exchange
    # so Serper-fetched rows (empty source) still show up. Use a longer
    # 365-day window because insider data is fetched less often.
    rows = db.conn.execute("""
        SELECT
            CASE
                WHEN source = '' OR source IS NULL THEN 'Serper search (' || exchange || ')'
                ELSE source
            END AS display_name,
            COUNT(*) AS cnt,
            MAX(fetched_at) AS last,
            CASE WHEN source = '' OR source IS NULL THEN 1 ELSE 0 END AS via_serper
        FROM insider_transactions
        WHERE fetched_at >= datetime('now', '-365 days')
        GROUP BY display_name
        ORDER BY cnt DESC LIMIT 15
    """).fetchall()
    for r in rows:
        # If grouped by "Serper search (...)", it's by definition PAID
        # because empty source fields only happen on Serper-returned items.
        # Otherwise use the named-source rule (KLSE Screener = FREE, rest PAID).
        paid = bool(r["via_serper"]) or _is_paid_insider(r["display_name"])
        sources.append({
            "category": "Insiders",
            "name": r["display_name"],
            "last": r["last"],
            "count": r["cnt"],
            "unit": "items (365d)",
            "paid": paid,
        })

    # Earnings calendar — page scrape first (FREE), Serper fallback (PAID)
    row = db.conn.execute("""
        SELECT MAX(fetched_at) AS last, COUNT(*) AS cnt
        FROM earnings_dates
    """).fetchone()
    if row and row["last"]:
        sources.append({
            "category": "Earnings",
            "name": "Earnings calendar (mixed)",
            "last": row["last"],
            "count": row["cnt"],
            "unit": "entries",
            "paid": False,  # primary is free; Serper is fallback only
        })

    return sources


# ---------------------------------------------------------------------------
# Recent errors from monitor log
# ---------------------------------------------------------------------------

def _recent_errors(limit: int = 15) -> list[str]:
    """Scan the monitor log for ERROR / WARNING / Traceback lines."""
    if not os.path.exists(MONITOR_LOG):
        return []
    try:
        with open(MONITOR_LOG, "r") as f:
            lines = f.readlines()
    except Exception:
        return []

    out = []
    for line in lines[-2000:]:  # only scan tail
        if any(kw in line for kw in ("ERROR", "Traceback", "Exception",
                                     "Failed", "WARN", "warning")):
            out.append(line.rstrip())
    return out[-limit:]


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def generate_engine_room_html(db: Database, config: dict) -> str:
    server = _server_status()
    backup = _backup_status()
    serper = _serper_status(db)
    sources = _source_health(db)
    errors = _recent_errors()

    # Group sources by category
    by_cat = defaultdict(list)
    for s in sources:
        by_cat[s["category"]].append(s)

    # Server card
    server_pid_html = (f'<span class="status-ok">PID {server["server_pid"]}</span>'
                       if server["server_pid"] else
                       '<span class="status-bad">not running</span>')
    watchdog_pid_html = (f'<span class="status-ok">PID {server["watchdog_pid"]}</span>'
                         if server["watchdog_pid"] else
                         '<span class="status-bad">not running</span>')

    server_card = f"""
    <div class="er-card">
        <div class="er-card-title">⚙ Server</div>
        <div class="er-row"><span class="er-label">Server process</span>{server_pid_html}</div>
        <div class="er-row"><span class="er-label">Watchdog process</span>{watchdog_pid_html}</div>
        <div class="er-row"><span class="er-label">Watchdog started</span><span>{_esc(server["watchdog_started"]) or "—"}</span></div>
        <div class="er-row"><span class="er-label">Restart count</span><span>{server["restart_count"]}</span></div>
        <div class="er-row"><span class="er-label">Last restart</span><span>{_esc(server["last_restart"]) or "—"}</span></div>
        <div class="er-row"><span class="er-label">Database size</span><span>{server["db_size_human"]}</span></div>
    </div>"""

    # Backup card
    recent_backup_html = ""
    if backup["recent_commits"]:
        items = "".join(
            f'<div class="er-mini-row"><code>{_esc(c["hash"])}</code> '
            f'<span class="muted">{_esc(c["date"])}</span> '
            f'{_esc(c["subject"])}</div>'
            for c in backup["recent_commits"]
        )
        recent_backup_html = f'<div class="er-recent">{items}</div>'

    backup_card = f"""
    <div class="er-card">
        <div class="er-card-title">💾 Backups</div>
        <div class="er-row"><span class="er-label">SQL dump file</span><span>{"✓ exists" if backup["dump_exists"] else "✗ missing"}</span></div>
        <div class="er-row"><span class="er-label">Dump size</span><span>{backup["dump_size_human"]}</span></div>
        <div class="er-row"><span class="er-label">Dump last modified</span><span>{_esc(backup["dump_modified"]) or "—"}</span></div>
        <div class="er-row"><span class="er-label">Last backup commit</span><span>{_esc(backup["last_backup_commit"]) or "—"} ({_esc(backup["last_backup_age"])})</span></div>
        {recent_backup_html}
    </div>"""

    # Serper card with progress bar and breakdown
    bal = serper["balance"]
    used = serper["credits_used"]
    pct = serper["pct_used"]
    free_tier = serper["free_tier"]

    if serper["error"] and bal is None:
        bar_html = f'<div class="muted">{_esc(serper["error"])}</div>'
        balance_text = '<span class="status-bad">unavailable</span>'
    else:
        if pct < 50:
            bar_color = "var(--green)"
            bar_cls = "status-ok"
        elif pct < 80:
            bar_color = "var(--orange)"
            bar_cls = "status-warn"
        else:
            bar_color = "var(--red)"
            bar_cls = "status-bad"
        bar_html = f"""
        <div class="er-progress-wrap">
            <div class="er-progress-bar"><div class="er-progress-fill" style="width:{pct}%;background:{bar_color}"></div></div>
            <div class="er-progress-labels">
                <span class="muted">{used:,} used</span>
                <span class="{bar_cls}">{pct}%</span>
                <span class="muted">{(bal or 0):,} / {free_tier:,} free credits remaining</span>
            </div>
        </div>"""
        balance_text = f'<span class="{bar_cls}">{(bal or 0):,} credits</span>'

    # By caller breakdown — show both today and total
    def _caller_rows(items):
        if not items:
            return '<div class="muted" style="font-size:0.75rem">none yet</div>'
        rows = ""
        for it in items:
            rows += (
                f'<div class="er-cat-row">'
                f'<span class="er-cat-label">{_esc(it["caller"])}</span>'
                f'<span class="er-cat-bar"><span class="er-cat-fill" style="width:{min(100, it["count"]*4)}%"></span></span>'
                f'<span class="er-cat-count">{it["count"]}</span>'
                f'</div>'
            )
        return rows

    today_rows = _caller_rows(serper["by_caller_today"])
    week_rows = _caller_rows(serper["by_caller_week"])
    total_rows = _caller_rows(serper["by_caller_total"])

    # Top tickers
    ticker_rows = ""
    if serper["by_ticker_week"]:
        for it in serper["by_ticker_week"]:
            ticker_rows += (
                f'<div class="er-cat-row">'
                f'<span class="er-cat-label">{_esc(it["ticker"])}</span>'
                f'<span class="er-cat-bar"><span class="er-cat-fill" style="width:{min(100, it["count"]*8)}%"></span></span>'
                f'<span class="er-cat-count">{it["count"]}</span>'
                f'</div>'
            )
    else:
        ticker_rows = '<div class="muted" style="font-size:0.75rem">none yet</div>'

    serper_card = f"""
    <div class="er-card er-card-wide">
        <div class="er-card-title">🔍 Serper API <span class="muted" style="font-weight:400;font-size:0.75rem">— 2,500 free credits at signup</span></div>
        {bar_html}
        <div class="er-serper-grid">
            <div class="er-serper-stat"><span class="er-label">Balance</span>{balance_text}</div>
            <div class="er-serper-stat"><span class="er-label">Rate limit</span><span>{_esc(serper["rate_limit"]) or "—"} req/s</span></div>
            <div class="er-serper-stat"><span class="er-label">Calls today</span><span>{serper["queries_today"]}</span></div>
            <div class="er-serper-stat"><span class="er-label">Calls this week</span><span>{serper["queries_week"]}</span></div>
            <div class="er-serper-stat"><span class="er-label">Total tracked</span><span>{serper["queries_total"]}</span></div>
            <div class="er-serper-stat"><span class="er-label">Last call</span><span>{_human_age(serper["last_call_at"])}</span></div>
        </div>
        <div class="er-serper-breakdown">
            <div>
                <div class="er-source-cat-title">By category — today</div>
                {today_rows}
            </div>
            <div>
                <div class="er-source-cat-title">By category — this week</div>
                {week_rows}
            </div>
            <div>
                <div class="er-source-cat-title">By category — all time</div>
                {total_rows}
            </div>
            <div>
                <div class="er-source-cat-title">Top tickers — this week</div>
                {ticker_rows}
            </div>
        </div>
        <div class="muted" style="font-size:0.7rem;margin-top:0.6rem;border-top:1px solid var(--border);padding-top:0.5rem">
            Tracking started after this update — totals reflect calls made since serper_calls table was created.
            Categories: <strong>news</strong> (every stock news fetch), <strong>contracts</strong> (tender/award searches),
            <strong>forums</strong> (Twitter/X searches), <strong>insiders</strong> (insider searches),
            <strong>earnings</strong> (fallback only — primary is free page scraping).
        </div>
    </div>"""

    # Sources table — group by category, show paid/free badge per source
    cat_blocks = ""
    for cat in ("Prices", "News", "Forums", "Insiders", "Earnings"):
        items = by_cat.get(cat, [])
        if not items:
            continue
        rows_html = ""
        for s in items:
            cls = _age_class(s["last"])
            age = _human_age(s["last"])
            badge = ('<span class="src-badge paid">PAID</span>'
                     if s.get("paid") else
                     '<span class="src-badge free">FREE</span>')
            rows_html += (
                f'<tr class="src-{cls}">'
                f'<td>{badge} {_esc(s["name"])}</td>'
                f'<td><span class="src-dot src-{cls}-dot"></span> {age}</td>'
                f'<td>{s["count"]} {_esc(s["unit"])}</td>'
                f'</tr>'
            )
        cat_blocks += f"""
        <div class="er-source-cat">
            <div class="er-source-cat-title">{cat}</div>
            <table class="er-source-table">
                <thead><tr><th>Source</th><th>Last fetch</th><th>Volume</th></tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>"""

    sources_section = f"""
    <div class="er-card er-card-wide">
        <div class="er-card-title">🌐 Source Sites</div>
        <div class="er-source-grid">{cat_blocks}</div>
    </div>"""

    # Errors section
    if errors:
        errors_html = "<pre class='er-errors'>" + _esc("\n".join(errors)) + "</pre>"
    else:
        errors_html = '<div class="muted">No recent errors logged.</div>'
    errors_card = f"""
    <div class="er-card er-card-wide">
        <div class="er-card-title">⚠ Recent Errors / Warnings</div>
        {errors_html}
    </div>"""

    # Page shell
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Engine Room — Emerging Edge</title>
<style>
:root {{
    --bg: #0f1117; --surface: #1a1d27; --surface2: #232735;
    --border: #2a2d3a; --text: #e1e5ee; --text-muted: #8890a0;
    --accent: #6c8cff; --green: #34d399; --orange: #f97316;
    --red: #ff6b6b;
}}
* {{ box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, sans-serif;
    background: var(--bg); color: var(--text); margin: 0;
    font-size: 14px; line-height: 1.5;
}}
.header {{
    background: var(--surface); border-bottom: 1px solid var(--border);
    padding: 0.8rem 1.2rem; display: flex;
    align-items: center; justify-content: space-between;
}}
.header h1 {{ margin: 0; font-size: 1.15rem; font-weight: 700; }}
.header h1 span {{ color: var(--accent); }}
.header a {{
    color: var(--accent); text-decoration: none;
    border: 1px solid var(--accent); border-radius: 999px;
    padding: 0.2rem 0.7rem; font-size: 0.75rem; font-weight: 600;
    margin-left: 0.5rem;
}}
.container {{ padding: 1.2rem; max-width: 1400px; margin: 0 auto; }}
.er-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    gap: 1rem; margin-bottom: 1rem;
}}
.er-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 1rem 1.2rem;
}}
.er-card-wide {{ grid-column: 1 / -1; }}
.er-card-title {{
    font-size: 0.95rem; font-weight: 700; margin-bottom: 0.7rem;
    color: var(--text); border-bottom: 1px solid var(--border);
    padding-bottom: 0.5rem;
}}
.er-row {{
    display: flex; justify-content: space-between;
    padding: 0.3rem 0; font-size: 0.85rem;
}}
.er-label {{ color: var(--text-muted); }}
.status-ok {{ color: var(--green); font-weight: 600; }}
.status-warn {{ color: var(--orange); font-weight: 600; }}
.status-bad {{ color: var(--red); font-weight: 600; }}
.muted {{ color: var(--text-muted); font-size: 0.8rem; }}
.er-recent {{ margin-top: 0.6rem; padding-top: 0.6rem; border-top: 1px solid var(--border); }}
.er-mini-row {{ font-size: 0.75rem; padding: 0.15rem 0; color: var(--text-muted); }}
.er-mini-row code {{ color: var(--accent); margin-right: 0.4rem; }}
.er-source-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(380px, 1fr));
    gap: 1.2rem;
}}
.er-source-cat-title {{
    font-size: 0.78rem; text-transform: uppercase; color: var(--text-muted);
    letter-spacing: 0.05em; margin-bottom: 0.4rem; font-weight: 600;
}}
.er-source-table {{ width: 100%; border-collapse: collapse; font-size: 0.78rem; }}
.er-source-table th {{
    text-align: left; color: var(--text-muted); padding: 0.4rem 0.5rem;
    border-bottom: 1px solid var(--border); font-weight: 600;
    font-size: 0.7rem; text-transform: uppercase;
}}
.er-source-table td {{
    padding: 0.35rem 0.5rem; border-bottom: 1px solid var(--border);
}}
.er-source-table tr:last-child td {{ border-bottom: none; }}
.src-badge {{
    display: inline-block; font-size: 0.6rem; font-weight: 700;
    padding: 0.1rem 0.35rem; border-radius: 3px; margin-right: 0.4rem;
    letter-spacing: 0.04em;
}}
.src-badge.paid {{ background: rgba(255,107,107,0.15); color: var(--red); border: 1px solid rgba(255,107,107,0.3); }}
.src-badge.free {{ background: rgba(52,211,153,0.12); color: var(--green); border: 1px solid rgba(52,211,153,0.3); }}
.er-progress-wrap {{ margin-bottom: 0.8rem; }}
.er-progress-bar {{
    width: 100%; height: 14px; background: var(--bg);
    border: 1px solid var(--border); border-radius: 7px;
    overflow: hidden;
}}
.er-progress-fill {{ height: 100%; transition: width 0.3s; }}
.er-progress-labels {{
    display: flex; justify-content: space-between;
    margin-top: 0.4rem; font-size: 0.78rem;
}}
.er-serper-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 0.5rem 1.5rem; margin-bottom: 1rem;
    padding-bottom: 0.8rem; border-bottom: 1px solid var(--border);
}}
.er-serper-stat {{ display: flex; justify-content: space-between; font-size: 0.82rem; }}
.er-serper-breakdown {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 1.2rem; margin-top: 0.4rem;
}}
.er-cat-row {{
    display: grid; grid-template-columns: 80px 1fr 30px;
    align-items: center; gap: 0.4rem; font-size: 0.75rem; padding: 0.15rem 0;
}}
.er-cat-label {{ color: var(--text); }}
.er-cat-bar {{
    height: 6px; background: var(--bg); border: 1px solid var(--border);
    border-radius: 3px; overflow: hidden;
}}
.er-cat-fill {{ display: block; height: 100%; background: var(--accent); }}
.er-cat-count {{ text-align: right; color: var(--text-muted); font-variant-numeric: tabular-nums; }}
.src-dot {{
    display: inline-block; width: 8px; height: 8px; border-radius: 50%;
    margin-right: 0.4rem;
}}
.src-fresh-dot {{ background: var(--green); }}
.src-warm-dot {{ background: var(--orange); }}
.src-stale-dot {{ background: var(--red); }}
.src-stale td {{ color: var(--text-muted); }}
.er-errors {{
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 6px; padding: 0.8rem; font-size: 0.72rem;
    color: var(--text-muted); white-space: pre-wrap; word-break: break-all;
    max-height: 400px; overflow-y: auto; font-family: ui-monospace, monospace;
}}
</style>
</head>
<body>
<div class="header">
    <div>
        <h1><span>Emerging Edge</span> Engine Room</h1>
    </div>
    <div>
        <a href="/portfolio">Portfolio</a>
        <a href="/monitor">Monitor</a>
    </div>
</div>
<div class="container">
    <div class="er-grid">
        {server_card}
        {backup_card}
    </div>
    <div class="er-grid">
        {serper_card}
    </div>
    <div class="er-grid">
        {sources_section}
    </div>
    <div class="er-grid">
        {errors_card}
    </div>
</div>
</body>
</html>"""


def save_engine_room_html(db: Database, config: dict) -> str:
    """Generate and write the engine room HTML. Returns the file path."""
    digest_dir = config.get("digest_dir", "./digests")
    os.makedirs(digest_dir, exist_ok=True)
    content = generate_engine_room_html(db, config)
    filepath = os.path.join(digest_dir, "engine_room.html")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    return filepath
