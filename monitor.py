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
import http.server
import json
import logging
import os
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
from dashboard import save_html, open_html
from portfolio import (import_transactions_csv, save_portfolio_html,
                       generate_portfolio_html, compute_reinvest_shortfall,
                       compute_convert_shortfall)
from engine_room import save_engine_room_html


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
    """
    port = args.port if hasattr(args, "port") and args.port else 8878

    # Generate initial dashboard
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    digest_dir = config.get("digest_dir", "./digests")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    abs_digest_dir = os.path.join(script_dir, digest_dir) if not os.path.isabs(digest_dir) else digest_dir

    def _invalidate_monitor_cache(dir_path: str):
        """Delete cached daily HTML so the next /monitor request regenerates."""
        try:
            for f in os.listdir(dir_path):
                if f.startswith("daily_") and f.endswith(".html"):
                    try:
                        os.remove(os.path.join(dir_path, f))
                    except OSError:
                        pass
        except FileNotFoundError:
            pass

    filepath = save_html(db, config, today)
    print(f"🌐 Initial dashboard: {filepath}")

    # Track state — progress is a dict updated in real-time by background threads
    state = {
        "last_refresh": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "refreshing": False,
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

        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)

            if parsed.path == "/":
                # The public / starts on the portfolio page.
                self.send_response(302)
                self.send_header("Location", "/portfolio")
                self.end_headers()
                return

            if parsed.path in ("/monitor", "/emergingedge"):
                # Serve today's dashboard (the "Monitor"). Regenerate on each
                # request so watchlist additions show up immediately.
                t = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                filepath = os.path.join(abs_digest_dir, f"daily_{t}.html")
                try:
                    save_html(db, config, t)
                except Exception as e:
                    traceback.print_exc()
                    self._reconnect_db()
                    try:
                        save_html(db, config, t)
                    except Exception as e2:
                        traceback.print_exc()
                        # Fall back to the newest existing daily file
                        try:
                            existing = sorted(
                                f for f in os.listdir(abs_digest_dir)
                                if f.startswith("daily_") and f.endswith(".html"))
                            if existing:
                                filepath = os.path.join(abs_digest_dir, existing[-1])
                            else:
                                self.send_error(500, f"Monitor unavailable: {e2}")
                                return
                        except Exception as e3:
                            self.send_error(500, f"Monitor unavailable: {e3}")
                            return
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.end_headers()
                    with open(filepath, "rb") as f:
                        self.wfile.write(f.read())
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

            if parsed.path == "/api/refresh":
                if state["refreshing"]:
                    self._json_response({"status": "busy", "message": "Refresh already in progress"})
                    return

                # Check for force flag in POST body
                force = False
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    if length > 0:
                        body = json.loads(self.rfile.read(length))
                        force = body.get("force", False)
                except Exception:
                    pass

                if force:
                    # Temporarily set staleness to 0 to force re-fetch
                    import fetchers
                    fetchers.STALE_NEWS_HOURS = 0
                    fetchers.STALE_CONTRACTS_HOURS = 0
                    fetchers.STALE_INSIDER_HOURS = 0
                    fetchers.STALE_FORUM_HOURS = 0

                state["refreshing"] = True
                state["progress"] = {"step": "starting", "ticker": "", "done": 0,
                                     "total": len(get_active_stocks(db, config)), "error": ""}
                msg = "Force-fetching all data..." if force else "Fetching new data (skipping fresh)..."
                self._json_response({"status": "started", "message": msg})

                def do_refresh():
                    from fetchers import (fetch_news, fetch_contracts, fetch_earnings,
                                          fetch_forums, fetch_prices, fetch_insiders)
                    stocks = get_active_stocks(db, config)
                    prog = state["progress"]
                    prog["total"] = len(stocks)
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
                        print(f"✅ Refresh complete at {state['last_refresh']}")
                    except Exception as e:
                        prog["error"] = str(e)[:200]
                        print(f"❌ Refresh failed: {e}")
                    finally:
                        state["refreshing"] = False
                        # Restore staleness thresholds if they were overridden
                        import fetchers as _f
                        _f.STALE_NEWS_HOURS = 48
                        _f.STALE_CONTRACTS_HOURS = 168
                        _f.STALE_INSIDER_HOURS = 168
                        _f.STALE_FORUM_HOURS = 168

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

                def do_price_refresh():
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
                    added = db.add_user_stock(body)
                    # Invalidate cached dashboard HTML so the new stock shows up.
                    _invalidate_monitor_cache(abs_digest_dir)
                    count = len(db.get_user_stocks())
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

    # Kill any existing server on this port before starting
    import subprocess as _sp
    try:
        pids = _sp.check_output(["lsof", "-ti", f":{port}"], text=True).strip()
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

    # Open in browser
    import webbrowser
    webbrowser.open(url)

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
        }
        commands[args.command](args, config, db)
    finally:
        db.close()


if __name__ == "__main__":
    main()
