"""
portfolio.py — Portfolio tracker for Emerging Edge.

Tracks buy/sell transactions, computes holdings, and generates
a self-contained HTML page with a portfolio value chart.

Input: CSV file with columns: date,ticker,exchange,type,shares,price,currency
Output: digests/portfolio.html with Chart.js line chart
"""

from __future__ import annotations

import csv
import html as html_mod
import json
import os
import webbrowser
from datetime import datetime, timedelta
from typing import Optional

from db import Database


# ---------------------------------------------------------------------------
# CSV Import
# ---------------------------------------------------------------------------

def import_transactions_csv(filepath: str, db: Database, config: dict) -> int:
    """
    Parse a CSV file and import transactions into the database.
    Returns count of new transactions imported.

    CSV format:
        date,ticker,exchange,type,shares,price,currency
        2024-01-15,MATRIX,KLSE,BUY,10000,1.25,MYR
    """
    # Build valid ticker set from active stocks (config + user_stocks)
    from fetchers import get_active_stocks
    valid_tickers = {(s["ticker"], s["exchange"]) for s in get_active_stocks(db, config)}

    imported = 0
    skipped = 0
    errors = []

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):  # line 2 (after header)
            try:
                txn_date = row["date"].strip()
                ticker = row["ticker"].strip().upper()
                exchange = row["exchange"].strip().upper()
                txn_type = row["type"].strip().upper()
                shares = float(row["shares"].strip())
                price = float(row["price"].strip())
                currency = row["currency"].strip().upper()

                # Validate
                if txn_type not in ("BUY", "SELL", "DIVIDEND", "REINVEST"):
                    errors.append(f"  Line {i}: invalid type '{txn_type}' (must be BUY, SELL, DIVIDEND, or REINVEST)")
                    continue

                if (ticker, exchange) not in valid_tickers:
                    errors.append(f"  Line {i}: unknown ticker {ticker}/{exchange}")
                    continue

                stored = db.insert_transaction(
                    ticker=ticker, exchange=exchange,
                    txn_type=txn_type, shares=shares,
                    price=price, currency=currency,
                    txn_date=txn_date)

                if stored:
                    imported += 1
                else:
                    skipped += 1

            except (KeyError, ValueError) as e:
                errors.append(f"  Line {i}: {e}")

    print(f"✅ Imported {imported} transactions ({skipped} duplicates skipped)")
    if errors:
        print(f"⚠️  {len(errors)} errors:")
        for e in errors[:10]:
            print(e)
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")

    return imported


# ---------------------------------------------------------------------------
# Holdings Computation
# ---------------------------------------------------------------------------

def compute_holdings(db: Database, config: dict):
    """
    Walk all transactions in date order and compute current holdings,
    cash balances, and external capital deposits.

    Cash accounting model:
      - BUY is always fresh external capital: it adds shares and basis,
        and the full cost is recorded as an external deposit at the txn
        date. It does NOT touch cash, even if cash is available. Use
        REINVEST if you want to consume cash.
      - REINVEST consumes cash in the transaction currency, adds shares
        and basis just like a BUY. Any shortfall against available cash
        falls through to an external deposit (user error guard).
      - SELL credits cash with proceeds.
      - DIVIDEND credits cash with the payout (also tracked in the
        position's `dividends` field for per-holding display).

    Uses weighted-average cost method for per-position basis.

    Returns a tuple (holdings, cash, deposits) where:
      holdings: list of dicts (same shape as before, plus total_gain/total_return_pct)
      cash:     dict of currency -> running balance
      deposits: list of {date, currency, amount} external capital injections
    """
    txns = db.get_all_transactions()
    from fetchers import get_active_stocks
    _all_stocks = get_active_stocks(db, config)
    # Index by ticker AND by code — Bursa Malaysia and similar exchanges
    # use numeric codes (5301) in transactions but the catalog often
    # registers the same security under an alphabetic ticker (CTOS)
    # with `code: 5301`. Without code-fallback, holdings would render
    # the bare numeric instead of the company name.
    stock_map = {s["ticker"]: s for s in _all_stocks}
    for s in _all_stocks:
        c = (s.get("code") or "").strip()
        if c and c not in stock_map:
            stock_map[c] = s

    # Normalize transaction tickers to their canonical form. The
    # catalog can simultaneously contain a config-defined "MATRIX"
    # (code=5236) and a stale user-added "5236" (code=5236) — same
    # security, two labels. Without this, a BUY of MATRIX and a SELL
    # of 5236 leave both as ghost positions.
    code_to_canon = {}
    for s in _all_stocks:
        c = (s.get("code")   or "").strip()
        tk = (s.get("ticker") or "").strip()
        if c and tk and c != tk:
            code_to_canon[c] = tk
    for t in txns:
        t["ticker"] = code_to_canon.get(t["ticker"], t["ticker"])

    # Accumulate per ticker
    positions = {}  # ticker -> {shares, total_cost, dividends_received, currency, exchange}
    cash = {}       # currency -> running balance
    deposits = []   # external capital events: {date, currency, amount}

    for t in txns:
        tk = t["ticker"]
        cur = t["currency"]
        # CONVERT does not reference a security; handle it standalone.
        if t["txn_type"] == "CONVERT":
            from_cur = cur
            from_amount = t["shares"]
            to_cur = t["to_currency"] or cur
            to_amount = t["to_amount"] or 0.0
            cash.setdefault(from_cur, 0.0)
            cash.setdefault(to_cur, 0.0)
            have = cash[from_cur]
            if have >= from_amount:
                cash[from_cur] = have - from_amount
            else:
                shortfall = from_amount - have
                cash[from_cur] = 0.0
                deposits.append({"date": t["txn_date"], "currency": from_cur,
                                 "amount": shortfall})
            cash[to_cur] += to_amount
            continue

        if tk not in positions:
            positions[tk] = {
                "shares": 0.0, "total_cost": 0.0, "dividends": 0.0,
                "lifetime_bought_cost": 0.0,
                "lifetime_sells_proceeds": 0.0,
                "currency": cur, "exchange": t["exchange"]
            }
        pos = positions[tk]
        cash.setdefault(cur, 0.0)

        if t["txn_type"] == "BUY":
            # Always fresh external capital. Never touches cash.
            cost = t["shares"] * t["price"]
            pos["total_cost"] += cost
            pos["shares"] += t["shares"]
            pos["lifetime_bought_cost"] += cost
            deposits.append({"date": t["txn_date"], "currency": cur, "amount": cost})
        elif t["txn_type"] == "REINVEST":
            # Cash-funded buy. Debits cash; shortfall falls through to a
            # new external deposit (guard against over-reinvestment).
            cost = t["shares"] * t["price"]
            pos["shares"] += t["shares"]
            pos["total_cost"] += cost
            pos["lifetime_bought_cost"] += cost
            have = cash.get(cur, 0.0)
            if have >= cost:
                cash[cur] = have - cost
            else:
                shortfall = cost - have
                cash[cur] = 0.0
                deposits.append({"date": t["txn_date"], "currency": cur, "amount": shortfall})
        elif t["txn_type"] == "SELL":
            proceeds = t["shares"] * t["price"]
            if pos["shares"] > 0:
                avg = pos["total_cost"] / pos["shares"] if pos["shares"] else 0
                pos["shares"] -= t["shares"]
                pos["total_cost"] = avg * pos["shares"]
            pos["lifetime_sells_proceeds"] += proceeds
            cash[cur] = cash.get(cur, 0.0) + proceeds
        elif t["txn_type"] == "DIVIDEND":
            # shares = shares held, price = dividend per share
            amount = t["shares"] * t["price"]
            pos["dividends"] += amount
            cash[cur] = cash.get(cur, 0.0) + amount

    # Build holdings with current prices
    holdings = []
    for tk, pos in positions.items():
        is_sold_out = pos["shares"] <= 1e-9
        if is_sold_out and pos.get("lifetime_bought_cost", 0) <= 0:
            continue

        if is_sold_out:
            avg_cost = (pos["lifetime_bought_cost"] /
                        max(_lifetime_bought_shares(tk, txns), 1e-9))
            current_price = 0.0
            market_value = 0.0
            total_invested = pos["lifetime_bought_cost"]
            dividends = pos["dividends"]
            realized_gain = (pos["lifetime_sells_proceeds"]
                             - pos["lifetime_bought_cost"])
            price_gain = realized_gain
            price_return_pct = (
                (realized_gain / pos["lifetime_bought_cost"] * 100)
                if pos["lifetime_bought_cost"] > 0 else 0
            )
            total_gain = realized_gain + dividends
            total_return_pct = (
                (total_gain / pos["lifetime_bought_cost"] * 100)
                if pos["lifetime_bought_cost"] > 0 else 0
            )
        else:
            avg_cost = pos["total_cost"] / pos["shares"] if pos["shares"] else 0
            # Try every known alias (alpha ticker + numeric code +
            # yahoo_ticker root) so a stale stockanalysis row doesn't
            # mask a fresh klsescreener row (CHB vs 0291 was the
            # original case).
            _info = stock_map.get(tk, {}) or {}
            _yh = (_info.get("yahoo_ticker") or "").split(".")[0]
            _aliases = [a for a in {tk, _info.get("code"), _info.get("ticker"), _yh} if a]
            price_data = db.get_latest_price_any(_aliases, pos["exchange"])
            current_price = price_data["price"] if price_data else 0

            market_value = pos["shares"] * current_price
            total_invested = pos["total_cost"]
            dividends = pos["dividends"]

            price_gain = market_value - total_invested
            price_return_pct = (price_gain / total_invested * 100) if total_invested > 0 else 0

            total_gain = price_gain + dividends
            total_return_pct = (total_gain / total_invested * 100) if total_invested > 0 else 0

        stock_info = stock_map.get(tk, {})
        holdings.append({
            "ticker": tk,
            "exchange": pos["exchange"],
            "currency": pos["currency"],
            "name": stock_info.get("name", tk),
            "shares": pos["shares"],
            "avg_cost": avg_cost,
            "current_price": current_price,
            "market_value": market_value,
            "total_invested": total_invested,
            "dividends": dividends,
            "gain_loss": price_gain,
            "gain_pct": price_return_pct,
            "total_gain": total_gain,
            "total_return_pct": total_return_pct,
            "is_sold_out": is_sold_out,
        })

    # Sort live first (by market value desc), sold-out positions at the bottom
    holdings.sort(key=lambda h: (h.get("is_sold_out", False),
                                  -h["market_value"]))
    return holdings, cash, deposits


def weighted_buy_fx(db, ticker: str, currency: str, txns):
    """Cost-weighted average FX rate across EVERY purchase of a ticker.

    The displayed "Buy FX" used to be the rate on the first buy date
    alone, which ignored every follow-on purchase and was inconsistent
    with the Avg-cost column right next to it (that one IS weighted
    across all buys). The rate we want is the one that reproduces the
    actual USD outlay:

        weighted = Σ(local cost) / Σ(local cost / fx_on_that_buy_date)

    i.e. cost-weighted in USD terms, so a big early buy and a small
    later one move it proportionally. Rates are quoted local-per-USD,
    so USD cost = local cost / rate.

    Includes REINVEST as well as BUY, matching how total_cost /
    lifetime_bought_cost (and therefore avg_cost) are accumulated.
    Returns None when no usable purchase/rate data exists, so callers
    can fall back to their previous behaviour."""
    if not currency or currency == "USD":
        return 1.0
    total_local = 0.0
    total_usd = 0.0
    for t in txns:
        if t.get("ticker") != ticker:
            continue
        if (t.get("txn_type") or "").upper() not in ("BUY", "REINVEST"):
            continue
        # Guard against a mixed-currency position: only weight purchases
        # actually denominated in this holding's currency.
        t_curr = (t.get("currency") or "").upper()
        if t_curr and t_curr != currency.upper():
            continue
        cost = float(t.get("shares") or 0) * float(t.get("price") or 0)
        if cost <= 0:
            continue
        fx = db.get_fx_rate(currency, t.get("txn_date"))
        if not fx or fx <= 0:
            continue
        total_local += cost
        total_usd += cost / fx
    if total_usd > 0:
        return total_local / total_usd
    return None


def _lifetime_bought_shares(ticker: str, txns) -> float:
    """Sum of share counts across all BUY/REINVEST transactions."""
    total = 0.0
    for t in txns:
        if t.get("ticker") != ticker:
            continue
        if t.get("txn_type") in ("BUY", "REINVEST"):
            total += float(t.get("shares") or 0)
    return total


def _walk_cash_before(db: Database, target_id: int, target_date: str) -> dict:
    """
    Walk all transactions strictly before (target_date, target_id) and
    return the per-currency cash balance at that instant.
    Used by the shortfall helpers below.
    """
    txns = db.get_all_transactions()  # ordered by txn_date ASC, id ASC
    cash: dict = {}
    for t in txns:
        if t["id"] == target_id:
            continue
        # Strictly earlier: earlier date, OR same date with lower id
        if t["txn_date"] > target_date:
            break
        if t["txn_date"] == target_date and t["id"] > target_id:
            continue

        tt = t["txn_type"]
        if tt == "CONVERT":
            from_cur = t["currency"]
            from_amount = t["shares"]
            to_cur = t["to_currency"] or from_cur
            to_amount = t["to_amount"] or 0.0
            cash.setdefault(from_cur, 0.0)
            cash.setdefault(to_cur, 0.0)
            have = cash[from_cur]
            if have >= from_amount:
                cash[from_cur] = have - from_amount
            else:
                cash[from_cur] = 0.0  # shortfall would become a deposit
            cash[to_cur] += to_amount
            continue

        cur = t["currency"]
        cash.setdefault(cur, 0.0)
        if tt == "BUY":
            pass  # BUY doesn't touch cash (fresh external capital)
        elif tt == "REINVEST":
            cost = t["shares"] * t["price"]
            have = cash[cur]
            cash[cur] = max(0.0, have - cost)
        elif tt == "SELL":
            cash[cur] += t["shares"] * t["price"]
        elif tt == "DIVIDEND":
            cash[cur] += t["shares"] * t["price"]
    return cash


def compute_reinvest_shortfall(db: Database, txn_id: int) -> float:
    """
    For a REINVEST transaction, compute how much of its cost exceeded
    available cash in its currency at the time of the transaction.
    Returns 0.0 if txn is not a REINVEST, not found, or cash covered it.
    """
    target = db.conn.execute(
        "SELECT * FROM portfolio_transactions WHERE id = ?", (txn_id,)
    ).fetchone()
    if not target or target["txn_type"].upper() != "REINVEST":
        return 0.0
    target_cost = target["shares"] * target["price"]
    cash = _walk_cash_before(db, txn_id, target["txn_date"])
    have = cash.get(target["currency"], 0.0)
    return max(0.0, target_cost - have)


def compute_convert_shortfall(db: Database, txn_id: int) -> float:
    """
    For a CONVERT transaction, compute how much of the from-amount
    exceeded available cash in the from-currency at the time of the
    transaction. Returns 0.0 if not a CONVERT, not found, or covered.
    """
    target = db.conn.execute(
        "SELECT * FROM portfolio_transactions WHERE id = ?", (txn_id,)
    ).fetchone()
    if not target or target["txn_type"].upper() != "CONVERT":
        return 0.0
    from_amount = target["shares"]
    cash = _walk_cash_before(db, txn_id, target["txn_date"])
    have = cash.get(target["currency"], 0.0)
    return max(0.0, from_amount - have)


# ---------------------------------------------------------------------------
# FX Rates
# ---------------------------------------------------------------------------

def _fx_fallback_rates() -> dict:
    """All rates vs USD from open.er-api.com (free, keyless, 166
    currencies incl. MUR/UZS/XOF/KGS that most FX APIs skip). Used when
    Yahoo's FX quotes 429 — which they do persistently, leaving rates
    stale for months and any unmapped currency stuck at $0. One call
    covers every currency at once. Returns {} on failure."""
    import urllib.request as _ur, json as _json
    try:
        req = _ur.Request("https://open.er-api.com/v6/latest/USD",
                          headers={"User-Agent": "Mozilla/5.0"})
        with _ur.urlopen(req, timeout=15) as r:
            data = _json.loads(r.read())
        if data.get("result") == "success":
            return data.get("rates") or {}
    except Exception:
        pass
    return {}


# Currencies the portfolio supports end-to-end: offered in the CONVERT
# dropdown AND guaranteed a stored FX rate. Keep these as ONE list —
# when the dropdown and the rate-fetcher drew from separate lists, a
# currency could be selectable while having no rate, which is how a
# holding ends up displaying $0.00.
SUPPORTED_CURRENCIES = ["USD", "MYR", "NGN", "ZAR", "XOF", "UZS", "SGD",
                        "KGS", "KZT", "MUR", "PHP", "MNT", "GBP", "EUR",
                        "SEK", "AUD"]


def fetch_and_store_fx_rates(db: Database, config: dict):
    """Fetch current FX rates (Yahoo first, er-api fallback) into fx_snapshots."""
    from fetchers import _fetch_price_yahoo

    # Collect currencies from portfolio transactions
    txns = db.get_all_transactions()
    # Union with SUPPORTED_CURRENCIES so a currency offered in the
    # CONVERT dropdown always has a rate, even before the portfolio
    # holds anything denominated in it. Also union the currencies of every
    # WATCHED stock: the monitor spans far more markets than the portfolio
    # (37 currencies vs 15), and without a rate their market caps can't be
    # shown in USD. The fallback fetches all 166 rates in one request, so
    # the extra coverage is free.
    currencies = {t["currency"] for t in txns} | set(SUPPORTED_CURRENCIES)
    try:
        for _s in db.get_user_stocks():
            _c = (_s.get("currency") or "").strip()
            if _c:
                currencies.add(_c)
    except Exception:
        pass
    # And the currencies the PRICE FEED actually reports, which is not the
    # same set: London quotes come back as GBX (pence) while the stock rows
    # say GBP, so without this every LSE name had no usable rate and its
    # market cap silently failed to convert.
    try:
        for _r in db.conn.execute(
                "SELECT DISTINCT currency FROM price_snapshots "
                "WHERE currency IS NOT NULL AND currency <> ''"):
            _c = (_r["currency"] or "").strip()
            if _c:
                currencies.add(_c)
    except Exception:
        pass

    today = datetime.utcnow().strftime("%Y-%m-%d")

    _FX_MAP = {
        "MYR": "MYR=X", "NGN": "NGN=X", "UZS": "UZS=X",
        "XOF": "XOF=X", "KGS": "KGS=X", "SGD": "SGD=X",
        "MUR": "MUR=X", "PHP": "PHP=X", "KZT": "KZT=X",
        "MNT": "MNT=X",   # Mongolian tugrik (MSE)
        "ZAc": "ZAR=X", "ZAC": "ZAR=X", "ZAR": "ZAR=X",
    }
    # Base ISO code for the fallback lookup (ZAc = ZAR cents).
    # Sub-unit quote conventions: JSE quotes in rand cents, LSE in
    # pence. Both need the major-unit rate x100.
    _FX_BASE = {"ZAc": "ZAR", "ZAC": "ZAR",
                "GBX": "GBP", "GBp": "GBP", "GBX ": "GBP"}
    _CENTS = ("ZAc", "ZAC", "GBX", "GBp")
    fallback: dict | None = None   # fetched lazily, once per run

    for curr in currencies:
        if curr == "USD":
            db.insert_fx_rate("USD", 1.0, today)
            continue

        # Skip if we already have today's rate. Avoids hammering Yahoo
        # at every page render — Yahoo FX endpoints 429 the same way as
        # equity quotes, and on a fresh boot this loop would otherwise
        # spend ~12s per currency timing out.
        existing = db.conn.execute(
            "SELECT 1 FROM fx_snapshots WHERE currency = ? AND snapshot_at = ?",
            (curr, today),
        ).fetchone()
        if existing:
            continue

        rate = None
        pair = _FX_MAP.get(curr)
        if pair:
            r = _fetch_price_yahoo(pair, bulk=True)
            if r:
                rate = r[0]
        if rate is None:
            # Yahoo throttled or currency not in the map — er-api covers
            # everything (this is also how brand-new currencies work
            # without touching _FX_MAP).
            if fallback is None:
                fallback = _fx_fallback_rates()
            rate = fallback.get(_FX_BASE.get(curr, curr).upper())
        if rate:
            # Sources give the MAJOR unit per USD (ZAR, GBP); these
            # currencies are quoted in the sub-unit, so x100. Without the
            # GBX case every London stock had no rate at all, and their
            # market caps silently failed to convert and rendered blank.
            if curr in _CENTS:
                rate = rate * 100
            db.insert_fx_rate(curr, rate, today)


def backfill_historical_prices(db: Database, config: dict):
    """
    Fetch historical daily prices from Yahoo Finance for all stocks
    that have portfolio transactions. Fills gaps in price_snapshots
    going back to the earliest transaction date.

    Only fetches from Yahoo (stocks with yahoo_ticker). For stocks
    without Yahoo, we can't backfill — they'll have gaps.
    """
    import json as _json
    import urllib.request as _urllib
    import urllib.parse as _urlparse

    txns = db.get_all_transactions()
    if not txns:
        return

    # Find earliest transaction date
    earliest = min(t["txn_date"] for t in txns)
    # Tickers in portfolio
    portfolio_tickers = {t["ticker"] for t in txns}

    from fetchers import get_active_stocks
    _all_stocks = get_active_stocks(db, config)
    stock_map = {s["ticker"]: s for s in _all_stocks}
    for s in _all_stocks:
        c = (s.get("code") or "").strip()
        if c and c not in stock_map:
            stock_map[c] = s

    for ticker in portfolio_tickers:
        stock = stock_map.get(ticker, {})
        yahoo_tk = stock.get("yahoo_ticker", "")
        if not yahoo_tk:
            continue

        # Check if we already have enough history
        row = db.conn.execute(
            "SELECT MIN(snapshot_at) as earliest FROM price_snapshots WHERE ticker = ?",
            (ticker,)).fetchone()
        existing_earliest = row["earliest"] if row and row["earliest"] else None

        if existing_earliest and existing_earliest <= earliest:
            continue  # already have data back to the buy date

        # Fetch from Yahoo — use range that covers earliest to now
        days_back = (datetime.utcnow() - datetime.strptime(earliest, "%Y-%m-%d")).days + 30
        if days_back > 365 * 2:
            period = "5y"
        elif days_back > 365:
            period = "2y"
        elif days_back > 180:
            period = "1y"
        else:
            period = "6mo"

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{_urlparse.quote(yahoo_tk)}?range={period}&interval=1d"
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
        req = _urllib.Request(url, headers=headers)

        try:
            with _urllib.urlopen(req, timeout=20) as resp:
                data = _json.loads(resp.read())

            result = data.get("chart", {}).get("result", [])
            if not result:
                continue

            timestamps = result[0].get("timestamp", [])
            quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
            closes = quotes.get("close", [])
            meta = result[0].get("meta", {})
            currency = meta.get("currency", stock.get("currency", ""))

            count = 0
            prev_close = None
            for ts, close in zip(timestamps, closes):
                if close is None:
                    continue
                snap_date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                if snap_date < earliest:
                    prev_close = close
                    continue

                chg_pct = 0.0
                if prev_close and prev_close > 0:
                    chg_pct = round(((close - prev_close) / prev_close) * 100, 2)

                db.insert_price(
                    ticker=ticker, exchange=stock.get("exchange", ""),
                    price=round(close, 4), change_pct=chg_pct,
                    currency=currency,
                    source_url=f"https://finance.yahoo.com/quote/{yahoo_tk}",
                    snapshot_date=snap_date)
                prev_close = close
                count += 1

            if count > 0:
                print(f"  📊 Backfilled {count} historical prices for {ticker}")

        except Exception as e:
            print(f"  ⚠️  Historical prices failed for {ticker}: {e}")

    # For UZSE stocks: fetch from stockscope.uz Firestore API
    for ticker in portfolio_tickers:
        stock = stock_map.get(ticker, {})
        if stock.get("exchange") != "UZSE":
            continue

        # Check if we already have enough history
        row = db.conn.execute(
            "SELECT MIN(snapshot_at) as earliest FROM price_snapshots WHERE ticker = ? AND source_url != 'interpolated'",
            (ticker,)).fetchone()
        existing_earliest = row["earliest"] if row and row["earliest"] else None
        if existing_earliest and existing_earliest <= earliest:
            continue

        ss_ticker = stock.get("stockscope_ticker", ticker)
        try:
            import urllib.request as _u2
            import json as _j2
            api_url = f"https://firestore.googleapis.com/v1/projects/uz-finance/databases/(default)/documents/uzse_listings/{ss_ticker}/price_history?pageSize=500"
            req = _u2.Request(api_url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
            with _u2.urlopen(req, timeout=20) as resp:
                api_data = _j2.loads(resp.read())

            docs = api_data.get("documents", [])
            if docs:
                hist_field = docs[0].get("fields", {}).get("history", {})
                if "mapValue" in hist_field:
                    prices_map = hist_field["mapValue"]["fields"]
                    count = 0
                    prev_price = None
                    for date_str in sorted(prices_map.keys()):
                        if date_str < earliest:
                            val = prices_map[date_str]
                            prev_price = float(str(val.get("doubleValue", val.get("integerValue", 0))))
                            continue
                        val = prices_map[date_str]
                        price = float(str(val.get("doubleValue", val.get("integerValue", 0))))
                        chg = round(((price - prev_price) / prev_price) * 100, 2) if prev_price and prev_price > 0 else 0
                        db.insert_price(
                            ticker=ticker, exchange="UZSE",
                            price=price, change_pct=chg,
                            currency=stock.get("currency", "UZS"),
                            source_url="stockscope.uz",
                            snapshot_date=date_str)
                        prev_price = price
                        count += 1
                    if count > 0:
                        print(f"  📊 Backfilled {count} stockscope prices for {ticker}")
        except Exception as e:
            print(f"  ⚠️  Stockscope history failed for {ticker}: {e}")

    # For stocks without Yahoo data, interpolate from buy price to first known price
    # This fills the gap so the chart doesn't jump
    for ticker in portfolio_tickers:
        stock = stock_map.get(ticker, {})
        exchange = stock.get("exchange", "")
        currency = stock.get("currency", "")

        # Find the buy price and date for this ticker
        # REINVEST is a purchase too — buying with cash already in the
        # account. Counting only BUY left reinvested positions with no
        # price between their purchase and the first live quote, so the
        # portfolio valued them at ZERO for those days: the cash left
        # the balance but nothing replaced it, showing up as a dip on
        # the buy date and a matching jump when the first price landed.
        buy_txns = [t for t in txns if t["ticker"] == ticker
                    and (t.get("txn_type") or "").upper() in ("BUY", "REINVEST")]
        if not buy_txns:
            continue
        first_buy = buy_txns[0]
        buy_date = first_buy["txn_date"]
        buy_price = first_buy["price"]

        # Check earliest price snapshot
        row = db.conn.execute(
            "SELECT MIN(snapshot_at) as earliest FROM price_snapshots WHERE ticker = ?",
            (ticker,)).fetchone()
        first_snap = row["earliest"] if row and row["earliest"] else None

        if not first_snap or first_snap <= buy_date:
            continue  # already covered

        # Get the first known price
        row2 = db.conn.execute(
            "SELECT price FROM price_snapshots WHERE ticker = ? ORDER BY snapshot_at ASC LIMIT 1",
            (ticker,)).fetchone()
        first_price = row2["price"] if row2 else buy_price

        # Interpolate daily from buy_date to first_snap
        start = datetime.strptime(buy_date, "%Y-%m-%d")
        end = datetime.strptime(first_snap, "%Y-%m-%d")
        total_days = (end - start).days
        # A ONE-day gap still needs filling: buy on the 19th with the first
        # live quote on the 20th left the 19th with no price at all, so the
        # position showed as zero for that day — the cash had gone but
        # nothing replaced it. total_days == 1 writes exactly one row, at
        # the purchase price, for the buy date itself.
        if total_days < 1:
            continue

        count = 0
        for d in range(total_days):
            dt = start + timedelta(days=d)
            day_str = dt.strftime("%Y-%m-%d")
            # Skip weekends
            if dt.weekday() >= 5:
                continue
            # Linear interpolation
            frac = d / total_days
            price = buy_price + (first_price - buy_price) * frac
            db.insert_price(
                ticker=ticker, exchange=exchange,
                price=round(price, 4), change_pct=0.0,
                currency=currency,
                source_url="interpolated",
                snapshot_date=day_str)
            count += 1

        if count > 0:
            print(f"  📈 Interpolated {count} prices for {ticker} ({buy_date} → {first_snap})")

    # Also backfill FX rates
    _backfill_fx_rates(db, config, earliest)


def _backfill_fx_rates(db: Database, config: dict, earliest: str):
    """Backfill historical FX rates from Yahoo for portfolio currencies."""
    import json as _json
    import urllib.request as _urllib

    txns = db.get_all_transactions()
    currencies = {t["currency"] for t in txns} - {"USD"}

    _FX_MAP = {
        "MYR": "MYR=X", "NGN": "NGN=X", "UZS": "UZS=X",
        "XOF": "XOF=X", "KGS": "KGS=X", "SGD": "SGD=X",
        "MUR": "MUR=X", "PHP": "PHP=X", "KZT": "KZT=X",
        "MNT": "MNT=X",   # Mongolian tugrik (MSE)
        "ZAc": "ZAR=X", "ZAC": "ZAR=X", "ZAR": "ZAR=X",
    }

    days_back = (datetime.utcnow() - datetime.strptime(earliest, "%Y-%m-%d")).days + 30
    period = "2y" if days_back > 365 else "1y" if days_back > 180 else "6mo"

    for curr in currencies:
        pair = _FX_MAP.get(curr)
        if not pair:
            continue

        # Check if we already have history
        row = db.conn.execute(
            "SELECT MIN(snapshot_at) as earliest FROM fx_snapshots WHERE currency = ?",
            (curr,)).fetchone()
        if row and row["earliest"] and row["earliest"] <= earliest:
            continue

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{pair}?range={period}&interval=1d"
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
        req = _urllib.Request(url, headers=headers)

        try:
            with _urllib.urlopen(req, timeout=20) as resp:
                data = _json.loads(resp.read())

            result = data.get("chart", {}).get("result", [])
            if not result:
                continue

            timestamps = result[0].get("timestamp", [])
            closes = result[0]["indicators"]["quote"][0].get("close", [])

            count = 0
            for ts, rate in zip(timestamps, closes):
                if rate is None:
                    continue
                snap_date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                if snap_date < earliest:
                    continue
                actual_rate = (rate * 100
                               if curr in ("ZAc", "ZAC", "GBX", "GBp")
                               else rate)
                db.insert_fx_rate(curr, actual_rate, snap_date)
                count += 1

            if count > 0:
                print(f"  💱 Backfilled {count} FX rates for {curr}")

        except Exception as e:
            print(f"  ⚠️  FX backfill failed for {curr}: {e}")


def _to_usd(amount: float, currency: str, db: Database,
             date: str = None) -> float:
    """Convert an amount to USD using stored FX rate."""
    if currency == "USD":
        return amount
    if date is None:
        date = datetime.utcnow().strftime("%Y-%m-%d")
    rate = db.get_fx_rate(currency, date)
    if rate and rate > 0:
        return amount / rate
    return 0.0


# ---------------------------------------------------------------------------
# Portfolio History (for chart)
# ---------------------------------------------------------------------------

def compute_portfolio_history(db: Database, config: dict) -> list[dict]:
    """
    Compute daily portfolio value in USD for each date we have price data.

    Returns list of {date, total_usd} sorted by date.
    """
    txns = db.get_all_transactions()
    if not txns:
        return []

    # Same canonical-ticker normalization as compute_holdings.
    from fetchers import get_active_stocks
    _all_stocks = get_active_stocks(db, config)
    code_to_canon = {}
    for s in _all_stocks:
        c = (s.get("code")   or "").strip()
        tk = (s.get("ticker") or "").strip()
        if c and tk and c != tk:
            code_to_canon[c] = tk
    for t in txns:
        t["ticker"] = code_to_canon.get(t["ticker"], t["ticker"])

    # Get all unique snapshot dates from price_snapshots
    rows = db.conn.execute(
        "SELECT DISTINCT snapshot_at FROM price_snapshots ORDER BY snapshot_at ASC"
    ).fetchall()
    snapshot_dates = [r["snapshot_at"] for r in rows]

    if not snapshot_dates:
        return []

    # For each date, replay transactions up to that date under cash accounting.
    # Portfolio-level cost basis = cumulative external deposits (in USD,
    # at deposit-date FX). Portfolio-level value = holdings value + cash value.
    history = []

    for snap_date in snapshot_dates:
        # Build positions, cash, and deposits as of this date.
        positions = {}  # ticker -> {shares, total_cost_usd, dividends_usd, currency, exchange}
        cash = {}       # currency -> running balance in that currency
        cumulative_deposits_usd = 0.0

        for t in txns:
            if t["txn_date"] > snap_date:
                break
            tk = t["ticker"]
            cur = t["currency"]

            # CONVERT does not reference a security; handle it standalone.
            if t["txn_type"] == "CONVERT":
                from_cur = cur
                from_amount = t["shares"]
                to_cur = t["to_currency"] or cur
                to_amount = t["to_amount"] or 0.0
                cash.setdefault(from_cur, 0.0)
                cash.setdefault(to_cur, 0.0)
                have = cash[from_cur]
                if have >= from_amount:
                    cash[from_cur] = have - from_amount
                else:
                    shortfall_local = from_amount - have
                    shortfall_usd = _to_usd(shortfall_local, from_cur, db, t["txn_date"])
                    cash[from_cur] = 0.0
                    cumulative_deposits_usd += shortfall_usd
                cash[to_cur] += to_amount
                continue

            if tk not in positions:
                positions[tk] = {"shares": 0.0, "total_cost_usd": 0.0,
                                 "dividends_usd": 0.0,
                                 "currency": cur, "exchange": t["exchange"]}
            cash.setdefault(cur, 0.0)

            if t["txn_type"] == "BUY":
                # Always fresh external capital.
                cost_local = t["shares"] * t["price"]
                cost_usd = _to_usd(cost_local, cur, db, t["txn_date"])
                positions[tk]["shares"] += t["shares"]
                positions[tk]["total_cost_usd"] += cost_usd
                cumulative_deposits_usd += cost_usd
            elif t["txn_type"] == "REINVEST":
                # Cash-funded buy; shortfall becomes a deposit.
                cost_local = t["shares"] * t["price"]
                cost_usd = _to_usd(cost_local, cur, db, t["txn_date"])
                positions[tk]["shares"] += t["shares"]
                positions[tk]["total_cost_usd"] += cost_usd
                have = cash.get(cur, 0.0)
                if have >= cost_local:
                    cash[cur] = have - cost_local
                else:
                    shortfall_local = cost_local - have
                    shortfall_usd = cost_usd * (shortfall_local / cost_local) if cost_local > 0 else 0
                    cash[cur] = 0.0
                    cumulative_deposits_usd += shortfall_usd
            elif t["txn_type"] == "SELL":
                pos = positions[tk]
                if pos["shares"] > 0:
                    avg_usd = pos["total_cost_usd"] / pos["shares"]
                    pos["shares"] -= t["shares"]
                    pos["total_cost_usd"] = avg_usd * pos["shares"]
                cash[cur] = cash.get(cur, 0.0) + t["shares"] * t["price"]
            elif t["txn_type"] == "DIVIDEND":
                amount_local = t["shares"] * t["price"]
                div_usd = _to_usd(amount_local, cur, db, t["txn_date"])
                positions[tk]["dividends_usd"] += div_usd
                cash[cur] = cash.get(cur, 0.0) + amount_local

        # Compute holdings market value at this date (USD)
        holdings_value_usd = 0.0
        per_stock = {}
        per_stock_cost = {}
        for tk, pos in positions.items():
            if pos["shares"] <= 0:
                per_stock_cost[tk] = round(pos["total_cost_usd"], 2)
                continue
            row = db.conn.execute(
                """SELECT price FROM price_snapshots
                   WHERE ticker = ? AND exchange = ? AND snapshot_at <= ?
                   ORDER BY snapshot_at DESC LIMIT 1""",
                (tk, pos["exchange"], snap_date)).fetchone()
            if row:
                val_usd = _to_usd(pos["shares"] * row["price"], pos["currency"], db, snap_date)
                holdings_value_usd += val_usd
                per_stock[tk] = round(val_usd, 2)
            per_stock_cost[tk] = round(pos["total_cost_usd"], 2)

        # Cash value in USD at snap_date
        cash_usd = sum(_to_usd(bal, c, db, snap_date) for c, bal in cash.items() if bal)

        total_usd = holdings_value_usd + cash_usd
        cost_basis_usd = cumulative_deposits_usd

        if total_usd > 0 or cost_basis_usd > 0:
            history.append({
                "date": snap_date,
                "total_usd": round(total_usd, 2),
                "cost_basis_usd": round(cost_basis_usd, 2),
                "cash_usd": round(cash_usd, 2),
                "holdings_usd": round(holdings_value_usd, 2),
                "stocks": per_stock,
                "stocks_cost": per_stock_cost,
            })

    return history


# ---------------------------------------------------------------------------
# HTML Generation
# ---------------------------------------------------------------------------

def _esc(text) -> str:
    return html_mod.escape(str(text)) if text else ""


def _fmt_money(amount: float, decimals: int = 2) -> str:
    """Format a number with commas and fixed decimals."""
    if abs(amount) >= 1000:
        return f"{amount:,.{decimals}f}"
    return f"{amount:.{decimals}f}"


def _xirr(cashflows: list) -> float | None:
    """Annualized money-weighted IRR (XIRR) for dated USD cash flows.

    `cashflows` is a list of (YYYY-MM-DD, amount) where contributions
    are negative (cash leaving the investor) and the terminal portfolio
    value is positive. Returns the annual rate as a fraction (0.27 =
    +27%/yr) or None if it can't be solved.

    Unlike CAGR — which assumes a single lump sum from start to end —
    XIRR weights every deposit by how long it has actually been
    invested, which is the correct annualized return when capital is
    added over time. Solved by bisection (NPV is monotone-decreasing in
    r for a normal early-out / terminal-in stream).
    """
    from datetime import date as _date
    flows = []
    for d, amt in cashflows:
        try:
            y, m, dd = (int(x) for x in str(d)[:10].split("-"))
            flows.append((_date(y, m, dd), float(amt)))
        except Exception:
            continue
    if len(flows) < 2:
        return None
    if not (any(a < 0 for _, a in flows) and any(a > 0 for _, a in flows)):
        return None
    t0 = min(d for d, _ in flows)
    times = [((d - t0).days / 365.25, a) for d, a in flows]

    def npv(r: float) -> float:
        s = 0.0
        for t, a in times:
            base = 1.0 + r
            if base <= 0:
                return float("inf")
            s += a / (base ** t)
        return s

    lo, hi = -0.9999, 10.0
    f_lo, f_hi = npv(lo), npv(hi)
    tries = 0
    while f_lo * f_hi > 0 and hi < 1e7 and tries < 50:
        hi *= 2
        f_hi = npv(hi)
        tries += 1
    if f_lo != f_lo or f_hi != f_hi or f_lo * f_hi > 0:  # NaN or same sign
        return None
    for _ in range(200):
        mid = (lo + hi) / 2
        f_mid = npv(mid)
        if abs(f_mid) < 1e-6:
            return mid
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2


def generate_portfolio_html(db: Database, config: dict) -> str:
    """Build the portfolio tracking HTML page."""
    import os as _os
    _logout_link = (
        '<a href="/logout" class="nav-link">Sign out</a>'
        if _os.environ.get("MULTI_USER", "").lower() in ("1","true","yes")
        else ''
    )


    # Backfill historical prices + FX rates if needed (first run only)
    backfill_historical_prices(db, config)
    # Ensure today's FX rates are current — but never block the page
    # render on Yahoo (which 429s heavily). If we already have a rate
    # for every currency from the last 7 days, skip the fetch entirely.
    # Otherwise kick the fetch into a background thread so the page
    # returns immediately; next render picks up the fresh rates.
    try:
        txns = db.get_all_transactions()
        _ccys = {t["currency"] for t in txns} - {"USD"}
        _missing = []
        for _c in _ccys:
            _hit = db.conn.execute(
                "SELECT 1 FROM fx_snapshots WHERE currency = ? "
                "AND snapshot_at >= date('now','-7 days') LIMIT 1",
                (_c,),
            ).fetchone()
            if not _hit:
                _missing.append(_c)
        if _missing:
            import threading as _th
            _th.Thread(
                target=fetch_and_store_fx_rates,
                args=(db, config),
                daemon=True,
            ).start()
    except Exception:
        # If anything goes wrong checking, fall back to the old
        # synchronous path so we still get FX rates eventually.
        try:
            fetch_and_store_fx_rates(db, config)
        except Exception:
            pass

    holdings, cash, deposits = compute_holdings(db, config)

    # User-managed list of fully-sold tickers to hide from the holdings
    # table. Stored as JSON in app_settings; user toggles via ✕ on each
    # sold-out row + "Show N hidden" link below the table.
    try:
        _hidden_raw = db.get_setting("hidden_sold_tickers", "[]")
        hidden_sold_tickers = set(json.loads(_hidden_raw)) if _hidden_raw else set()
    except Exception:
        hidden_sold_tickers = set()
    _before = len(holdings)
    holdings = [h for h in holdings
                 if not (h.get("is_sold_out")
                          and h["ticker"] in hidden_sold_tickers)]
    hidden_sold_count = _before - len(holdings)
    # Per-currency cash balances + USD equivalents — passed to JS so the
    # CONVERT form can show "Available: 1,234 SGD (≈$925)" hints and
    # one-click "use this balance" chips. Without this the user has to
    # guess amounts because the form fields are blank by default.
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    cash_balances_for_convert = []
    for cur, bal in cash.items():
        if not bal or abs(bal) < 0.0001:
            continue
        rate = db.get_fx_rate(cur, today_str)
        usd_equiv = (bal / rate) if rate and rate > 0 else None
        cash_balances_for_convert.append({
            "currency": cur,
            "balance": round(float(bal), 2),
            "usd_equiv": round(usd_equiv, 2) if usd_equiv is not None else None,
        })
    cash_balances_for_convert.sort(
        key=lambda b: -(b["usd_equiv"] or 0))
    cash_balances_json = json.dumps(cash_balances_for_convert)
    # Latest-known FX rate per currency (CUR per 1 USD), used by the
    # CONVERT form's auto-fill of the To-amount: when the user picks a
    # From chip + selects a To currency, the To-amount is computed as
    #   amount_to = (amount_from / rate_from) * rate_to
    fx_currencies = {row["currency"] for row in db.conn.execute(
        "SELECT DISTINCT currency FROM fx_snapshots").fetchall()}
    fx_rates_for_convert = {"USD": 1.0}
    for cur in fx_currencies:
        rate = db.get_fx_rate(cur, today_str)
        if rate and rate > 0:
            fx_rates_for_convert[cur] = float(rate)
    fx_rates_json = json.dumps(fx_rates_for_convert)
    holding_labels = db.get_holding_labels()
    history = compute_portfolio_history(db, config)
    txns = db.get_all_transactions()
    from fetchers import get_active_stocks
    _all_stocks = get_active_stocks(db, config)
    stock_map = {s["ticker"]: s for s in _all_stocks}
    for s in _all_stocks:
        c = (s.get("code") or "").strip()
        if c and c not in stock_map:
            stock_map[c] = s

    # Summary stats (cash accounting)
    # Total invested = sum of external capital deposits, each converted to USD
    # at the deposit date's FX rate.
    total_invested_usd = sum(
        _to_usd(d["amount"], d["currency"], db, d["date"]) for d in deposits
    )
    # Holdings market value in USD (at current FX).
    holdings_value_usd = sum(_to_usd(h["market_value"], h["currency"], db) for h in holdings)
    # Cash value in USD (at current FX).
    cash_usd = sum(_to_usd(bal, cur, db) for cur, bal in cash.items() if bal)
    # Per-holding dividends received (informational; already reflected in cash).
    total_dividends_usd = sum(_to_usd(h["dividends"], h["currency"], db) for h in holdings)
    # Current portfolio value = market value + cash. This is the figure that
    # includes reinvested dividends, sell proceeds, and anything else.
    current_value_usd = holdings_value_usd + cash_usd
    total_return_usd = current_value_usd - total_invested_usd
    total_return_pct = (total_return_usd / total_invested_usd * 100) if total_invested_usd > 0 else 0
    price_return_usd = holdings_value_usd - total_invested_usd  # kept for back-compat below

    # Since-inception IRR (annualized money-weighted return). Each external
    # deposit is a dated outflow (negative); today's portfolio value is the
    # terminal inflow (positive). This annualizes and weights each dollar by
    # how long it's been invested — the proper "CAGR with cash flows".
    from datetime import datetime as _dt_irr
    _irr_flows = [
        (d["date"], -_to_usd(d["amount"], d["currency"], db, d["date"]))
        for d in deposits
    ]
    if current_value_usd > 0:
        _irr_flows.append((_dt_irr.utcnow().strftime("%Y-%m-%d"), current_value_usd))
    inception_irr = _xirr(_irr_flows)  # fraction/yr, or None
    _irr_days = 0
    if deposits:
        try:
            _d0 = min(str(d["date"])[:10] for d in deposits)
            _irr_days = (_dt_irr.utcnow() - _dt_irr.strptime(_d0, "%Y-%m-%d")).days
        except Exception:
            _irr_days = 0

    # Best / worst by total return (price + dividends)
    best = max(holdings, key=lambda h: h["total_return_pct"]) if holdings else None
    worst = min(holdings, key=lambda h: h["total_return_pct"]) if holdings else None

    # Chart data — include cost basis at buy date as the starting point
    # so the chart shows the full gain from entry price
    chart_dates = [h["date"] for h in history]
    chart_values_raw = [h["total_usd"] for h in history]

    # Build cost basis series from history (steps up on each buy)
    chart_cost_basis = [h["cost_basis_usd"] for h in history]

    # Prepend first transaction date if before price history
    if txns and holdings:
        first_txn_date = txns[0]["txn_date"]
        if not chart_dates or first_txn_date < chart_dates[0]:
            first_cost = _to_usd(txns[0]["shares"] * txns[0]["price"], txns[0]["currency"], db, first_txn_date)
            chart_dates.insert(0, first_txn_date)
            chart_values_raw.insert(0, round(first_cost, 2))
            chart_cost_basis.insert(0, round(first_cost, 2))

    chart_labels = json.dumps(chart_dates)
    chart_values = json.dumps(chart_values_raw)
    cost_basis_values = json.dumps(chart_cost_basis)

    # Percentage return chart: ((value - cost_basis) / cost_basis) * 100
    chart_pct_values = json.dumps([
        round(((v - c) / c) * 100, 2) if c > 0 else 0
        for v, c in zip(chart_values_raw, chart_cost_basis)
    ])
    chart_pct_baseline = json.dumps([0] * len(chart_dates))

    # Per-stock chart data for click-to-filter. Also includes per-day
    # local-currency price + currency code so the tooltip can show
    # "1.41 MYR" alongside the USD value when one stock is selected.
    all_tickers = sorted({t["ticker"] for t in txns})

    _snapshots_by_ticker: dict = {}
    for tk in all_tickers:
        rows = db.conn.execute(
            "SELECT snapshot_at, price, currency FROM price_snapshots "
            "WHERE ticker = ? ORDER BY snapshot_at ASC",
            (tk,),
        ).fetchall()
        _snapshots_by_ticker[tk] = [
            {"date": r["snapshot_at"][:10], "price": r["price"],
             "currency": r["currency"]}
            for r in rows
        ]

    def _local_price_on(ticker: str, target_date: str) -> tuple:
        snaps = _snapshots_by_ticker.get(ticker, [])
        last = None
        for s in snaps:
            if s["date"] <= target_date:
                last = s
            else:
                break
        if last is None:
            return (None, "")
        return (float(last["price"]) if last["price"] is not None else None,
                last.get("currency") or "")

    per_stock_data = {}
    for tk in all_tickers:
        values = []
        costs = []
        local_by_date = {}
        currency_for_stock = ""
        for i, d in enumerate(chart_dates):
            h_match = next((h for h in history if h["date"] == d), None)
            if h_match:
                values.append(h_match.get("stocks", {}).get(tk, 0))
                costs.append(h_match.get("stocks_cost", {}).get(tk, 0))
            else:
                values.append(0)
                costs.append(0)
            lp, cur = _local_price_on(tk, d)
            if lp is not None:
                local_by_date[d] = round(lp, 4)
            if cur and not currency_for_stock:
                currency_for_stock = cur
        pcts = [round(((v - c) / c) * 100, 2) if c > 0 else 0
                for v, c in zip(values, costs)]
        per_stock_data[tk] = {
            "values": values, "cost": costs, "pct": pcts,
            "localByDate": local_by_date,
            "currency": currency_for_stock,
        }

    per_stock_json = json.dumps(per_stock_data)

    # Compute weights and sort by weight (largest first)
    for h in holdings:
        usd_val = _to_usd(h["market_value"], h["currency"], db)
        h["usd_value"] = usd_val
        h["weight"] = (usd_val / current_value_usd * 100) if current_value_usd > 0 else 0
    holdings.sort(key=lambda h: h["weight"], reverse=True)

    # Smart price formatting: no decimals for >= 100, 2 decimals for 1-100, 3 for <1
    def _fmt_local_price(price):
        if price >= 100:
            return f"{price:,.0f}"
        elif price >= 1:
            return f"{price:,.2f}"
        return f"{price:.3f}"

    # Holdings table rows — both USD and percentage modes
    holdings_rows = []
    # Per-stock dividend % of invested basis, for JS to add when showing
    # cost-basis return (ALL range or window contains buy date).
    stock_div_pct = {}
    for h in holdings:
        gain_cls = "gain-pos" if h["gain_loss"] >= 0 else "gain-neg"
        curr = h["currency"]
        is_sold = h.get("is_sold_out", False)

        # FX rates: current, and the cost-weighted average across ALL
        # purchases (not just the first buy — that ignored follow-on
        # buys and disagreed with the weighted Avg-cost column).
        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        fx_now = db.get_fx_rate(curr, today_str) if curr != "USD" else 1.0
        first_buy = next((t for t in txns
                          if t["ticker"] == h["ticker"]
                          and t["txn_type"] == "BUY"), None)
        buy_date_str = first_buy["txn_date"] if first_buy else today_str
        fx_at_buy = weighted_buy_fx(db, h["ticker"], curr, txns)
        if fx_at_buy is None:
            # No usable purchase/rate data — fall back to the first buy.
            fx_at_buy = (db.get_fx_rate(curr, buy_date_str)
                         if curr != "USD" else 1.0)

        fx_now_str = f"{fx_now:,.2f}" if fx_now else "—"
        fx_buy_str = f"{fx_at_buy:,.2f}" if fx_at_buy else "—"

        if fx_now and fx_at_buy and fx_at_buy > 0:
            fx_chg = ((fx_now - fx_at_buy) / fx_at_buy) * 100
            fx_chg_cls = "gain-neg" if fx_chg > 0 else "gain-pos" if fx_chg < 0 else ""
            fx_chg_str = f'<span class="{fx_chg_cls}">{fx_chg:+.1f}%</span>'
        else:
            fx_chg_str = ""

        # Local currency return = price change only
        local_return_pct = h["gain_pct"]
        local_cls = "gain-pos" if local_return_pct >= 0 else "gain-neg"

        # USD price return — for sold-out positions market value is 0,
        # so use realized local return as USD-return proxy.
        # Convert the (multi-buy) local cost basis at the SAME
        # cost-weighted rate shown as Buy FX. By construction
        # total_local / weighted_fx == Σ(cost_i / fx_i), i.e. the actual
        # USD outlay — whereas converting the whole basis at the first
        # buy's rate misstated it (and hence the USD return %) whenever
        # follow-on buys happened at a different rate.
        if fx_at_buy and fx_at_buy > 0:
            invested_usd = h["total_invested"] / fx_at_buy
        else:
            invested_usd = _to_usd(h["total_invested"], curr, db, buy_date_str)
        if is_sold:
            usd_return_pct = h["gain_pct"]
        else:
            usd_return_pct = ((h["usd_value"] - invested_usd) / invested_usd * 100) if invested_usd > 0 else 0
        usd_cls = "gain-pos" if usd_return_pct >= 0 else "gain-neg"

        # Per-payment breakdown for the Dividends cell's hover. The cell
        # shows a LIFETIME total, which reads like a mismatch next to a
        # single payment in the transactions list (CTOS 5301: two
        # payments, 34.20 + 64.20, displayed as 98.40). Listing the
        # individual payments on hover makes the total self-explanatory.
        _div_txns = [
            t for t in txns
            if t.get("ticker") == h["ticker"]
            and (t.get("txn_type") or "").upper() == "DIVIDEND"
        ]
        if len(_div_txns) > 1:
            _lines = []
            for t in _div_txns:
                _sh = float(t.get("shares") or 0)
                _rate = float(t.get("price") or 0)
                _lines.append(f"{t.get('txn_date')}: {_sh:,.0f} × {_rate:g} "
                              f"= {curr} {_fmt_money(_sh * _rate)}")
            _lines.append(f"Total: {curr} {_fmt_money(h['dividends'])}")
            dividends_tip = f"{len(_div_txns)} payments\n" + "\n".join(_lines)
        elif _div_txns:
            t = _div_txns[0]
            dividends_tip = (f"1 payment — {t.get('txn_date')}: "
                             f"{float(t.get('shares') or 0):,.0f} × "
                             f"{float(t.get('price') or 0):g}")
        else:
            dividends_tip = ""

        # USD total return = price return + dividends received (in USD)
        dividends_usd = _to_usd(h["dividends"], curr, db) if h["dividends"] else 0.0
        div_pct_of_basis = (dividends_usd / invested_usd * 100) if invested_usd > 0 else 0
        stock_div_pct[h["ticker"]] = round(div_pct_of_basis, 4)
        usd_total_return_pct = usd_return_pct + div_pct_of_basis
        usd_total_cls = "gain-pos" if usd_total_return_pct >= 0 else "gain-neg"

        # Per-row ✕ to hide a fully-exited position from the table.
        sold_remove_btn = (
            f'<span class="pct-only sold-remove-btn" style="display:none" '
            f'onclick="event.stopPropagation(); hideSoldHolding(\'{_esc(h["ticker"])}\');" '
            f'title="Remove this fully-exited position from the table">✕</span>'
            if h.get("is_sold_out") else ""
        )

        holdings_rows.append(f"""
        <tr class="holding-row" data-ticker="{_esc(h['ticker'])}" onclick="filterStock('{_esc(h['ticker'])}')">
            <td style="cursor:pointer">
                <span class="stock-name-full"><strong>{_esc(h['name'])}</strong> <span class="muted">{_esc(h['ticker'])}</span></span>
                <span class="stock-name-hidden" style="display:none"><strong>Undisclosed</strong></span>
                <span class="pct-only hide-toggle" style="display:none" onclick="event.stopPropagation(); toggleUndisclosed(this, '{_esc(h['ticker'])}');" title="Toggle visibility">👁</span>
                {sold_remove_btn}
            </td>
            <td class="usd-only">{h['shares']:,.0f}</td>
            <td class="usd-only">{_esc(curr)} {_fmt_local_price(h['avg_cost'])}</td>
            <td class="usd-only">{_esc(curr)} {_fmt_local_price(h['current_price'])}</td>
            <td class="usd-only">${_fmt_money(h['usd_value'])}</td>
            <td class="pct-only" style="display:none">{h['weight']:.1f}%</td>
            <td class="pct-only" style="display:none">{_esc(curr)} {_fmt_local_price(h['avg_cost'])}</td>
            <td class="pct-only" style="display:none">{_esc(curr)} {_fmt_local_price(h['current_price'])}</td>
            <td class="pct-only" style="display:none">
                <span class="muted">Now:</span> {fx_now_str}<br>
                <span class="muted">Buy:</span> {fx_buy_str} {fx_chg_str}
            </td>
            <td class="{local_cls}">{local_return_pct:+.1f}%</td>
            <td class="{usd_cls}" data-return-usd="{_esc(h['ticker'])}">{usd_return_pct:+.1f}%</td>
            <td class="usd-only{' has-div-tip' if dividends_tip else ''}"{f' title="{_esc(dividends_tip)}"' if dividends_tip else ''}>{_esc(h['currency'])} {_fmt_money(h['dividends'])}</td>
            <td class="{usd_total_cls}" data-return-total="{_esc(h['ticker'])}">{usd_total_return_pct:+.1f}%</td>
            <td class="pct-only" style="display:none"><select class="status-select" data-ticker="{_esc(h['ticker'])}" onchange="setHoldingLabel(this)"><option value="">—</option><option value="NEW"{" selected" if holding_labels.get(h["ticker"]) == "NEW" else ""}>NEW</option><option value="ADD"{" selected" if holding_labels.get(h["ticker"]) == "ADD" else ""}>ADD</option><option value="REDUCED"{" selected" if holding_labels.get(h["ticker"]) == "REDUCED" else ""}>REDUCED</option><option value="SOLD"{" selected" if (holding_labels.get(h["ticker"]) or ("SOLD" if is_sold else "")) == "SOLD" else ""}>SOLD OUT</option></select></td>
        </tr>""")

    # Cash row in the holdings table (USD mode only).
    # Shows the cash balance as a pseudo-holding so the user sees where
    # sell proceeds and dividends went. Not clickable / not filterable.
    if cash_usd > 0:
        cash_entries = [(cur, bal) for cur, bal
                        in sorted((c, b) for c, b in cash.items() if c) if bal]
        if len(cash_entries) == 1:
            only_cur, only_bal = cash_entries[0]
            cash_shares_display = f"{_esc(only_cur)} {_fmt_local_price(only_bal)}"
        else:
            # Multi-currency: show breakdown on hover, total count in cell.
            cash_shares_display = f"{len(cash_entries)} ccy"
        cash_breakdown_tip = ", ".join(
            f"{c} {_fmt_money(b)}" for c, b in cash_entries
        )
        cash_weight = (cash_usd / current_value_usd * 100) if current_value_usd > 0 else 0
        holdings_rows.append(f"""
        <tr class="holding-row cash-row" data-ticker="__CASH__" title="{_esc(cash_breakdown_tip)}">
            <td><strong>Cash</strong> <span class="muted">proceeds &amp; dividends</span></td>
            <td class="usd-only">{cash_shares_display}</td>
            <td class="usd-only muted">—</td>
            <td class="usd-only muted">—</td>
            <td class="usd-only">${_fmt_money(cash_usd)}</td>
            <td class="pct-only" style="display:none">{cash_weight:.1f}%</td>
            <td class="pct-only" style="display:none muted">—</td>
            <td class="pct-only" style="display:none muted">—</td>
            <td class="pct-only" style="display:none muted">—</td>
            <td class="muted">—</td>
            <td class="muted">—</td>
            <td class="usd-only muted">—</td>
            <td class="muted">—</td>
            <td class="pct-only" style="display:none"></td>
        </tr>""")

    # Transaction log rows
    txn_rows = []
    _txn_cls = {"BUY": "txn-buy", "SELL": "txn-sell", "DIVIDEND": "txn-div",
                "REINVEST": "txn-reinvest", "CONVERT": "txn-convert"}

    def _txn_ticker_display(ticker: str) -> str:
        """Render the ticker cell. For purely-numeric tickers (KLSE,
        HKSE) append the company name in muted text — "2062" alone
        means nothing to a reader, "2062 Harbour-Link" is recognizable."""
        esc = _esc(ticker)
        if not ticker or not ticker.replace(".", "").isdigit():
            return esc
        name = (stock_map.get(ticker, {}) or {}).get("name") or ""
        for suffix in (" Berhad", " Bhd", " Bhd.", " Limited", " Ltd",
                       " Ltd.", " Holdings", " Group", " Co.", " Company"):
            if name.endswith(suffix):
                name = name[: -len(suffix)].rstrip()
        if not name:
            return esc
        return (esc
                + ' <span class="muted" style="font-weight:400;font-size:0.7rem">'
                + _esc(name[:24]) + '</span>')

    for t in reversed(txns):  # most recent first
        type_cls = _txn_cls.get(t["txn_type"], "")
        ticker_display = _txn_ticker_display(t["ticker"])
        to_currency = t.get("to_currency") or ""
        to_amount = t.get("to_amount") or 0.0
        if t["txn_type"] == "CONVERT":
            from_amount = t["shares"]
            from_cur = t["currency"]
            rate = (to_amount / from_amount) if from_amount else 0
            ticker_display = f'<span class="muted">—</span>'
            detail = (f'{_esc(from_cur)} {_fmt_money(from_amount)} → '
                      f'{_esc(to_currency)} {_fmt_money(to_amount)} '
                      f'<span class="muted">(rate {rate:,.4f})</span>')
        elif t["txn_type"] == "DIVIDEND":
            amount = t["shares"] * t["price"]
            detail = f'{_esc(t["currency"])} {_fmt_money(amount)} ({t["shares"]:,.0f} × {t["price"]})'
        else:
            detail = f'{t["shares"]:,.0f} @ {_esc(t["currency"])} {_fmt_money(t["price"], 3)}'
        txn_rows.append(f"""
        <tr class="{type_cls}" id="txn-row-{t['id']}"
            data-id="{t['id']}"
            data-date="{_esc(t['txn_date'])}"
            data-ticker="{_esc(t['ticker'])}"
            data-exchange="{_esc(t['exchange'])}"
            data-type="{_esc(t['txn_type'])}"
            data-shares="{t['shares']}"
            data-price="{t['price']}"
            data-currency="{_esc(t['currency'])}"
            data-to-currency="{_esc(to_currency)}"
            data-to-amount="{to_amount}">
            <td>{_esc(t['txn_date'])}</td>
            <td>{ticker_display}</td>
            <td>{_esc(t['txn_type'])}</td>
            <td>{detail}</td>
            <td style="white-space:nowrap">
                <span class="edit-btn" onclick="editTxn({t['id']})" title="Edit">✎</span>
                <span class="del-btn" onclick="deleteTxn({t['id']})" title="Delete">✕</span>
            </td>
        </tr>""")

    return_cls = "stat-pos" if total_return_usd >= 0 else "stat-neg"

    # Build dynamic sections as strings to avoid nested f-string issues
    empty_msg = "" if holdings else (
        '<div class="welcome">'
        '<h2 style="margin-top:0">👋 Welcome to Emerging Edge</h2>'
        '<p style="color:var(--text-muted);font-size:0.92rem;line-height:1.55;max-width:720px">'
        'Track your frontier and emerging markets portfolio alongside the latest '
        'news, prices, earnings reports and insider transactions — all in one place. '
        'Start by adding your first position using the form below. '
        'Click <a href="/monitor" style="color:var(--accent)">Monitor</a> at the top to add '
        'stocks to your watchlist without recording a transaction.'
        '</p>'
        '<div class="welcome-txn-guide">'
        '<div class="welcome-txn-title">Transaction types</div>'
        '<dl>'
        '<dt><span class="welcome-txn-tag welcome-txn-buy">BUY</span></dt>'
        '<dd>Record a purchase of shares. Adds to your position and counts '
        'as fresh external capital in the portfolio.</dd>'
        '<dt><span class="welcome-txn-tag welcome-txn-sell">SELL</span></dt>'
        '<dd>Record a sale. Reduces your position and credits the sale '
        'proceeds to your cash balance for that currency.</dd>'
        '<dt><span class="welcome-txn-tag welcome-txn-div">DIVIDEND</span></dt>'
        '<dd>Record a cash dividend received. Credits your cash balance '
        'and is tracked as income; your share count stays the same.</dd>'
        '<dt><span class="welcome-txn-tag welcome-txn-rei">REINVEST</span></dt>'
        '<dd>Buy more shares using cash already in your account (from '
        'prior dividends or sale proceeds) — no new external capital.</dd>'
        '<dt><span class="welcome-txn-tag welcome-txn-con">CONVERT</span></dt>'
        '<dd>Move cash between currency buckets at an explicit rate '
        '(e.g. convert USD to MYR before a Malaysian purchase).</dd>'
        '</dl>'
        '<div class="welcome-hint">'
        'Power users: import a CSV with '
        '<code>python3 monitor.py portfolio import transactions.csv</code>'
        '</div>'
        '</div>'
        '</div>'
    )

    stats_html = ""
    if holdings:
        div_note = f' <span class="muted" style="font-size:0.7rem">(incl. ${_fmt_money(total_dividends_usd)} dividends)</span>' if total_dividends_usd > 0 else ""

        # Cash card: show total cash in USD with per-currency breakdown as tooltip.
        # sorted() on (currency, balance) pairs crashes if any currency key
        # is None/empty (e.g. a CONVERT row with a missing to_currency) —
        # Python can't compare None < str. Filter those out defensively so
        # one bad transaction can't take down the whole page render.
        cash_breakdown = ", ".join(
            f"{cur} {_fmt_money(bal)}"
            for cur, bal in sorted((c, b) for c, b in cash.items() if c)
            if bal
        ) or "no cash"
        cash_title = f"Cash by currency: {cash_breakdown}"
        cash_card = (
            f'<div class="stat-card usd-only" title="{_esc(cash_title)}">'
            f'<div class="label">Cash</div>'
            f'<div class="value" id="stat-cash">${_fmt_money(cash_usd)}</div></div>'
        )

        # Since-inception IRR card. Annualized money-weighted return —
        # the proper way to express a multi-deposit track record as a
        # single annual rate (a CAGR that accounts for cash flows). Only
        # meaningful with real capital and enough history; a tiny basis
        # or a <30-day record annualizes into absurd 4-digit rates.
        _irr_pct = inception_irr * 100 if inception_irr is not None else None
        if inception_irr is None or total_invested_usd < 1 or _irr_days < 30:
            _irr_card = ""
        elif abs(_irr_pct) > 1000:
            _irr_card = (
                '<div class="stat-card" title="Track record too short or '
                'capital base too small to annualize a meaningful IRR yet.">'
                '<div class="label">Since-Inception IRR</div>'
                '<div class="value" id="stat-irr" style="opacity:.6">—</div></div>'
            )
        else:
            _irr_cls = "stat-pos" if _irr_pct >= 0 else "stat-neg"
            _short = 0 < _irr_days < 365
            _irr_title = (
                "Annualized money-weighted return since your first deposit "
                "(XIRR): each contribution is weighted by how long it has "
                "been invested. Unlike a plain CAGR it accounts for deposits "
                "made over time."
                + (f" Note: only ~{_irr_days} days of history, so this annual "
                   "rate is extrapolated from a short track record."
                   if _short else "")
            )
            # Single card only — hiding it removes the box entirely (clean
            # for screenshots). Restore via the "+ show IRR" link by the
            # chart title.
            _irr_card = (
                f'<div class="stat-card" id="irr-card" title="{_esc(_irr_title)}">'
                f'<span class="irr-hide" onclick="toggleIrr(event)" '
                f'title="Hide this stat">&times;</span>'
                f'<div class="label">Since-Inception IRR{" *" if _short else ""}</div>'
                f'<div class="value {_irr_cls}" id="stat-irr">{_irr_pct:+.1f}%/yr</div></div>'
            )

        stats_html = (
            '<div class="stats">'
            f'<div class="stat-card usd-only"><div class="label">Total Invested</div>'
            f'<div class="value" id="stat-invested">${_fmt_money(total_invested_usd)}</div></div>'
            f'<div class="stat-card usd-only"><div class="label">Current Value</div>'
            f'<div class="value" id="stat-current">${_fmt_money(current_value_usd)}</div></div>'
            + cash_card +
            f'<div class="stat-card"><div class="label" id="stat-return-label">Total Return</div>'
            f'<div class="value {return_cls}" id="stat-return">'
            f'<span class="usd-only">${_fmt_money(total_return_usd)} </span>'
            f'({total_return_pct:+.1f}%){div_note}</div></div>'
            + _irr_card +
            f'<div class="stat-card"><div class="label" id="stat-holdings-label">Holdings</div>'
            f'<div class="value" id="stat-holdings-value">{sum(1 for h in holdings if not h.get("is_sold_out"))} stocks</div></div>'
            '</div>'
        )

    performers_html = ""
    if best and worst and len(holdings) > 1:
        performers_html = (
            '<div class="performers">'
            '<div class="performer"><div class="label">Best Performer</div>'
            f'<div class="stock gain-pos" id="best-performer">{_esc(best["name"])} ({_esc(best["ticker"])}) {best["total_return_pct"]:+.1f}%</div></div>'
            '<div class="performer"><div class="label">Worst Performer</div>'
            f'<div class="stock gain-neg" id="worst-performer">{_esc(worst["name"])} ({_esc(worst["ticker"])}) {worst["total_return_pct"]:+.1f}%</div></div>'
            '</div>'
        )

    # Donut (allocation) chart — build data for Chart.js doughnut.
    # Per-ticker donut colors — matched to logo dominant color.
    # Fallback palette for tickers not in the map.
    _DONUT_TICKER_COLORS = {
        "URTS":     "#2d8e4e",   # green (commodity exchange logo)
        "TIGO":     "#1a3a6b",   # deep navy blue (Millicom/Tigo)
        "CBSK":     "#6cc830",   # bright green (Chilonzor logo)
        "SKBSHUT":  "#e82030",   # red (SKB Shutters logo)
        "ETIT":     "#4a90b5",   # teal blue (Ecobank)
        "MATRIX":   "#a07848",   # warm brown/gold (Matrix Concepts)
        "HMKB":     "#1a6858",   # dark teal (Hamkorbank)
        "WEMABANK": "#9a18a0",   # purple/magenta (Wema Bank)
        "VEON":     "#e8c820",   # golden yellow (VEON)
        # US-listed Colombia ADRs — brand colors, picked to read as the
        # companies and to stay distinct from each other (yellow vs blue).
        "CIB":      "#1a1a1a",   # Bancolombia / Grupo Cibest — black wordmark
        "AVAL":     "#0046ad",   # Grupo Aval — corporate blue
        "2062":     "#1878f0",   # bright blue (Harbour-Link wave logo)
        "HARBOUR":  "#1878f0",   # alphabetic alias for 2062
        # Critical Holdings logo is half red, half grey → 50/50 blend
        # of the logo's red (#d81818) and grey (#787878).
        "0291":     "#a84848",   # legacy numeric ticker
        "CHB":      "#a84848",   # alphabetic canonical ticker
        # Mongolian holdings — colours sampled from the actual logo
        # artwork in logos/ (dominant non-background pixel), then lifted
        # slightly so they read on the dark chart card.
        "KHAN":     "#1b5e20",   # Khan Bank — forest-green clover
        "MSE":      "#1c5aa0",   # Mongolian Stock Exchange — royal blue
        "QPAY":     "#22335c",   # QPay — dark navy "Q"
        "CASH":     "#555555",   # neutral gray
    }
    _DONUT_FALLBACK = [
        '#6c8cff', '#4ecdc4', '#ff6b6b', '#ffd93d', '#a78bfa',
        '#f97316', '#34d399', '#f472b6', '#60a5fa', '#facc15',
    ]
    donut_html = ""
    # Display overrides for the donut chart's leader-line labels.
    # Numeric tickers (KLSE/HKSE) are unreadable on a chart — map them
    # to a short alphabetic nickname. The real ticker is still used for
    # filtering, click-through, logo lookup, etc.
    _DONUT_DISPLAY_OVERRIDES = {
        "2062": "HARBOUR",   # Harbour-Link Group Berhad (KLSE)
        "0291": "CRITICAL",  # Critical Holdings Berhad (KLSE)
    }

    def _donut_display_label(stock_ticker: str, stock_name: str) -> str:
        ov = _DONUT_DISPLAY_OVERRIDES.get(stock_ticker)
        if ov:
            return ov
        if stock_ticker and stock_ticker.replace(".", "").isdigit():
            first_word = (stock_name or "").split()[:1]
            if first_word:
                return first_word[0].upper()[:10]
        return stock_ticker

    if holdings:
        donut_labels = []
        donut_weights = []
        donut_colors = []
        donut_tickers = []
        donut_display_tickers = []
        _fb_idx = 0
        for i, h in enumerate(holdings):
            # Skip 0-weight holdings — sold-out positions stay in the
            # holdings table for the realized-return view but shouldn't
            # claim a slice of a market-value pie.
            if (h.get("weight") or 0) < 0.01 or h.get("is_sold_out"):
                continue
            donut_labels.append(h["name"])
            donut_tickers.append(h["ticker"])
            donut_display_tickers.append(
                _donut_display_label(h["ticker"], h.get("name", "")))
            donut_weights.append(round(h["weight"], 2))
            clr = _DONUT_TICKER_COLORS.get(h["ticker"])
            if not clr:
                clr = _DONUT_FALLBACK[_fb_idx % len(_DONUT_FALLBACK)]
                _fb_idx += 1
            donut_colors.append(clr)
        # Add cash slice if there's cash
        if cash_usd > 0:
            cash_wt = (cash_usd / current_value_usd * 100) if current_value_usd > 0 else 0
            donut_labels.append("Cash")
            donut_tickers.append("CASH")
            donut_display_tickers.append("CASH")
            donut_weights.append(round(cash_wt, 2))
            donut_colors.append('#555')

        donut_data_json = json.dumps(donut_weights)
        donut_labels_json = json.dumps(donut_labels)
        donut_colors_json = json.dumps(donut_colors)
        donut_tickers_json = json.dumps(donut_tickers)
        donut_display_tickers_json = json.dumps(donut_display_tickers)

        # Build a logo URL map — use locally served logos from /logos/ path
        # with cache-busting via file mtime
        _logo_dir = os.path.join(os.path.dirname(__file__) or ".", "logos")
        _available_logos = {}  # TICKER -> filename
        if os.path.isdir(_logo_dir):
            for lf in os.listdir(_logo_dir):
                ext = os.path.splitext(lf)[1].lower()
                if ext in ('.png', '.jpg', '.jpeg', '.svg', '.webp'):
                    tk_upper = os.path.splitext(lf)[0].upper()
                    # Prefer png over other formats if multiple exist
                    if tk_upper not in _available_logos or ext == '.png':
                        _available_logos[tk_upper] = lf
        donut_logos = []
        for tk in donut_tickers:
            fname = _available_logos.get(tk.upper())
            if fname:
                fpath = os.path.join(_logo_dir, fname)
                mtime = int(os.path.getmtime(fpath))
                donut_logos.append(f"/logos/{fname}?v={mtime}")
            else:
                donut_logos.append("")
        donut_logos_json = json.dumps(donut_logos)

        # Build the logo-management modal body (one row per holding).
        # Shows current logo (from /logos/{TICKER}.{ext}?v=<mtime>) or a
        # placeholder initials circle, plus a file input to upload a new one.
        _logo_mgr_rows = ""
        for i, h in enumerate(holdings):
            tk = h["ticker"]
            if tk.upper() in _available_logos:
                fname = _available_logos[tk.upper()]
                fpath = os.path.join(_logo_dir, fname)
                try:
                    mtime = int(os.path.getmtime(fpath))
                except OSError:
                    mtime = 0
                thumb_html = (
                    f'<img src="/logos/{_esc(fname)}?v={mtime}" '
                    f'class="logo-mgr-thumb" alt="{_esc(tk)}">'
                )
            else:
                initials = tk[:2] if len(tk) >= 2 else tk[:1]
                color = _DONUT_TICKER_COLORS.get(tk, _DONUT_FALLBACK[i % len(_DONUT_FALLBACK)])
                thumb_html = (
                    f'<div class="logo-mgr-thumb logo-mgr-placeholder" '
                    f'style="background:{color}">{_esc(initials)}</div>'
                )
            _logo_mgr_rows += (
                f'<div class="logo-mgr-row" data-ticker="{_esc(tk)}">'
                f'  <div class="logo-mgr-left">'
                f'    {thumb_html}'
                f'    <div>'
                f'      <div class="logo-mgr-name">{_esc(h["name"])}</div>'
                f'      <div class="logo-mgr-meta">{_esc(tk)} · {_esc(h["exchange"])}</div>'
                f'    </div>'
                f'  </div>'
                f'  <label class="logo-mgr-upload-btn">'
                f'    Upload image'
                f'    <input type="file" accept="image/png,image/jpeg,image/svg+xml,image/webp,image/gif" '
                f'style="display:none" onchange="uploadLogo(this, \'{_esc(tk)}\')">'
                f'  </label>'
                f'</div>'
            )

        donut_html = (
            '<div class="donut-section pct-only" style="display:none">'
            '<div class="donut-section-header">'
            '<div class="section-title" style="margin:0">Allocation</div>'
            '<button class="manage-logos-btn" onclick="openLogoManager()">🖼 Manage logos</button>'
            '</div>'
            '<div class="donut-chart-box"><canvas id="allocationChart"></canvas></div>'
            '</div>'
            # Logo manager modal (hidden by default)
            '<div id="logo-mgr-modal" class="logo-mgr-overlay" style="display:none" '
            'onclick="if (event.target===this) closeLogoManager()">'
            '<div class="logo-mgr-card">'
            '<div class="logo-mgr-header">'
            '<h3 style="margin:0">Manage Stock Logos</h3>'
            '<span class="logo-mgr-close" onclick="closeLogoManager()">✕</span>'
            '</div>'
            '<p class="muted" style="font-size:0.78rem;margin:0 0 0.8rem">'
            'Upload a custom logo for any holding. Supported: PNG, JPG, SVG, WEBP, GIF (max 2 MB). '
            'Changes appear on the donut chart after reload.'
            '</p>'
            f'<div class="logo-mgr-list">{_logo_mgr_rows}</div>'
            '</div>'
            '</div>'
        )

    banner_html = '<div class="stock-banner" id="stock-banner"><span id="banner-text"></span><span class="close-x" onclick="filterStock(activeStock)">✕</span></div>'

    chart_html = ""
    if history or (txns and holdings):
        chart_html = banner_html + (
            '<div class="chart-container">'
            '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;flex-wrap:wrap;gap:0.3rem">'
            '<div style="display:flex;align-items:center;gap:0.6rem">'
            '<div class="chart-title" style="margin:0">Portfolio Value (USD)</div>'
            '<button id="irr-restore" class="irr-restore" style="display:none" '
            'onclick="toggleIrr(event)" title="Show the Since-Inception IRR stat">'
            '+ show IRR</button>'
            '</div>'
            '<div class="time-range-pills">'
            '<button class="range-pill" onclick="setRange(\'1M\')">1M</button>'
            '<button class="range-pill" onclick="setRange(\'QTD\')">QTD</button>'
            '<button class="range-pill" onclick="setRange(\'YTD\')">YTD</button>'
            '<button class="range-pill active" onclick="setRange(\'ALL\')">All</button>'
            '<button class="range-pill" onclick="setRange(\'CUSTOM\')">Custom</button>'
            '<span id="custom-range-fields" style="display:none;gap:0.3rem;align-items:center;margin-left:0.4rem;font-size:0.8rem;color:var(--text-muted)">'
            'from <input type="date" id="custom-start" onchange="onCustomDateChange()">'
            'to <input type="date" id="custom-end" onchange="onCustomDateChange()">'
            '</span>'
            '</div></div>'
            '<canvas id="portfolioChart"></canvas></div>'
        )

    holdings_html = ""
    if holdings:
        # Only shown in % view (where the columns the user wants for
        # their monthly Substack updates live).
        share_toolbar = (
            '<div class="holdings-share-bar pct-only" style="display:none">'
            '<button class="holdings-share-btn" onclick="copyHoldingsAsMarkdown(this)" '
            'title="Copy a Markdown table of the visible holdings">'
            '📋 Copy as Markdown'
            '</button>'
            '<button class="holdings-share-btn" onclick="saveHoldingsAsImage(this)" '
            'title="Save the whole holdings table as a PNG">'
            '📸 Save as image'
            '</button>'
            '<span class="holdings-share-hint muted">'
            'Best for monthly portfolio updates'
            '</span>'
            '</div>'
        )
        holdings_html = (
            share_toolbar
            + '<div class="section-title">Holdings</div><div class="table-wrap" id="holdings-table-wrap"><table>'
            '<thead><tr><th>Stock</th><th class="usd-only">Shares</th><th class="usd-only">Avg Cost</th>'
            '<th class="usd-only">Price Today</th><th class="usd-only">Value (USD)</th>'
            '<th class="pct-only" style="display:none">Weight</th>'
            '<th class="pct-only" style="display:none">Avg Cost</th>'
            '<th class="pct-only" style="display:none">Price Today</th>'
            '<th class="pct-only" style="display:none">USD/Local FX</th>'
            '<th>Price Return (Local)</th><th>Price Return (USD)</th><th class="usd-only">Dividends</th><th>Total Return (USD)</th>'
            '<th class="pct-only" style="display:none">Status</th></tr></thead><tbody>'
            + "".join(holdings_rows)
            + '</tbody></table></div>'
            + (
                f'<div class="pct-only sold-hidden-toggle" style="display:none">'
                f'{hidden_sold_count} sold-out position{"s" if hidden_sold_count != 1 else ""} hidden — '
                f'<a href="#" onclick="event.preventDefault(); showAllSoldHoldings();">Show all</a>'
                f'</div>'
                if hidden_sold_count > 0 else ''
            )
            + '<a id="holdings-image-download" style="display:none"></a>'
        )

    # Currency options for the CONVERT From/To dropdowns. One base list
    # plus whatever currencies actually appear in the portfolio (cash
    # balances + transactions), so adding a stock in a new currency
    # (e.g. MUR for Mauritius) automatically surfaces it here instead
    # of requiring another hardcoded-list edit. `first` sets the
    # default selection by putting that code at the top.
    def _convert_currency_options(first: str) -> str:
        base = list(SUPPORTED_CURRENCIES)
        seen_portfolio = {str(c).upper() for c in cash.keys() if c}
        try:
            for t in db.get_all_transactions():
                for c in (t.get("currency"), t.get("to_currency")):
                    if c:
                        seen_portfolio.add(str(c).upper())
        except Exception:
            pass
        # ZAc is the JSE cents convention — the convert form deals in ZAR.
        seen_portfolio.discard("ZAC")
        opts = base + sorted(c for c in seen_portfolio if c not in base)
        opts.remove(first)
        opts.insert(0, first)
        return "".join(f'<option value="{c}">{c}</option>' for c in opts)

    add_form = (
        '<div class="add-txn-form usd-only" id="add-txn-form">'
        '<div class="field"><label>Date</label>'
        f'<input type="date" id="txn-date" value="{datetime.utcnow().strftime("%Y-%m-%d")}"></div>'
        '<div class="field" style="flex:1;min-width:180px;position:relative"><label>Stock</label>'
        '<input type="text" id="txn-stock-search" placeholder="Type a name or ticker..." '
        'autocomplete="off" oninput="onTxnStockSearch(this.value)" onfocus="onTxnStockSearch(this.value)" '
        'style="width:100%">'
        '<div id="txn-stock-results" class="txn-autocomplete-results"></div>'
        # Hidden fields populated by the autocomplete selection
        '<input type="hidden" id="txn-selected-ticker">'
        '<input type="hidden" id="txn-selected-exchange">'
        '<input type="hidden" id="txn-selected-currency">'
        '</div>'
        '<div class="field"><label>Type</label>'
        '<select id="txn-type" onchange="toggleConvertFields()">'
        '<option>BUY</option><option>SELL</option>'
        '<option>DIVIDEND</option><option>REINVEST</option>'
        '<option>CONVERT</option></select></div>'
        '<div class="field txn-security-field"><label>Shares</label>'
        '<input type="number" id="txn-shares" step="any" placeholder="0"></div>'
        '<div class="field txn-security-field"><label>Price</label>'
        '<input type="number" id="txn-price" step="any" placeholder="0.00"></div>'
        # CONVERT-specific fields (hidden unless type=CONVERT)
        # Cash-balance chips: clickable shortcuts that pre-fill From
        # currency + amount with the user's actual balance. Solves the
        # "I don't know how much I have to convert" problem.
        '<div class="field txn-convert-field txn-convert-balances" style="display:none;flex-basis:100%;flex-direction:column;align-items:flex-start">'
        '<label>Available cash <span class="muted" style="font-weight:400;font-size:0.7rem">(click to fill From)</span></label>'
        '<div id="txn-cash-chips" class="txn-cash-chips"></div>'
        '</div>'
        '<div class="field txn-convert-field" style="display:none">'
        '<label>From</label>'
        '<div style="display:flex;gap:0.3rem;align-items:center">'
        '<select id="txn-from-currency" style="width:55px" onchange="onFromCurrencyChange()">'
        + _convert_currency_options("USD") +
        '</select>'
        '<input type="number" id="txn-from-amount" step="any" placeholder="amount" style="width:90px" oninput="recomputeToAmount()">'
        '<button type="button" class="txn-from-max" onclick="setFromMax()" '
        'title="Fill From amount with the full available balance">all</button>'
        '</div>'
        '<div id="txn-from-balance" class="txn-balance-hint"></div>'
        '</div>'
        '<div class="field txn-convert-field" style="display:none">'
        '<label>To</label>'
        '<div style="display:flex;gap:0.3rem;align-items:center">'
        '<select id="txn-to-currency" style="width:55px" onchange="onToCurrencyChange()">'
        + _convert_currency_options("MYR") +
        '</select>'
        '<input type="number" id="txn-to-amount" step="any" placeholder="amount" style="width:90px" oninput="onToAmountManualEdit()">'
        '</div>'
        '<div id="txn-to-rate-hint" class="txn-balance-hint"></div>'
        '</div>'
        '<button class="add-txn-btn" onclick="addTransaction()">+ Add</button>'
        '</div>'
    )

    txn_table = ""
    if txn_rows:
        txn_table = (
            '<div class="table-wrap"><table>'
            '<thead><tr><th>Date</th><th>Ticker</th><th>Type</th>'
            '<th>Details</th><th></th></tr></thead><tbody>'
            + "".join(txn_rows)
            + '</tbody></table></div>'
        )

    txns_html = (
        '<div class="usd-only">'
        '<div class="section-title">Transactions</div>'
        + add_form + txn_table +
        '</div>'
    )

    chart_js = ""
    if history or (txns and holdings):
        # Donut chart JS with leader-line labels (Fiscal AI style)
        donut_js = ""
        if holdings:
            donut_js = """
// Allocation donut chart with leader-line labels
try {
const _donutCtx = document.getElementById('allocationChart');
if (_donutCtx) {
    const _ctx2d = _donutCtx.getContext('2d');
    const donutLabels = """ + donut_labels_json + """;
    const donutData = """ + donut_data_json + """;
    const donutColors = """ + donut_colors_json + """;
    const donutTickers = """ + donut_tickers_json + """;
    // Visible labels on the leader line — same as donutTickers but with
    // numeric KLSE/HKSE codes mapped to readable nicknames.
    const donutDisplayTickers = """ + donut_display_tickers_json + """;
    const donutLogos = """ + donut_logos_json + """;

    // Preload logo images
    const logoImages = {};
    let logosLoading = 0;
    donutLogos.forEach((url, i) => {
        if (!url) return;
        logosLoading++;
        const img = new Image();
        img.crossOrigin = 'anonymous';
        img.onload = function() {
            logoImages[donutTickers[i]] = img;
            logosLoading--;
            if (logosLoading <= 0 && window._donutChart) window._donutChart.update();
        };
        img.onerror = function() { logosLoading--; };
        img.src = url;
    });

    // The donut renders on a dark card, so very dark slice colors (e.g.
    // CIB's black wordmark) make their leader line, logo ring, and slice
    // edge vanish into the background. Lift near-black colors to a muted
    // light gray for strokes/lines only — the slice fill stays true black.
    function _donutLum(c) {
        const m = /^#?([0-9a-f]{6})$/i.exec(c || '');
        if (!m) return 255;
        const n = parseInt(m[1], 16);
        return 0.2126*((n>>16)&255) + 0.7152*((n>>8)&255) + 0.0722*(n&255);
    }
    function _safeLineColor(c) { return _donutLum(c) < 60 ? '#9aa0ad' : c; }

    // Leader-line label plugin (Fiscal AI style — straight radial lines)
    const labelPlugin = {
        id: 'donutLeaderLabels',
        afterDraw(chart) {
            const { ctx } = chart;
            const meta = chart.getDatasetMeta(0);
            if (!meta.data.length) return;

            // Compute raw label positions along radial direction
            const items = [];
            meta.data.forEach((arc, i) => {
                const mid = (arc.startAngle + arc.endAngle) / 2;
                const oR = arc.outerRadius;
                const cx = arc.x, cy = arc.y;
                // Edge point (just outside donut)
                const eX = cx + Math.cos(mid) * (oR + 3);
                const eY = cy + Math.sin(mid) * (oR + 3);
                // Label anchor — just outside the arc. Kept tight so the
                // logo/text sit close to their slice rather than being
                // flung out to the card edges.
                const labelDist = oR + 30;
                const lX = cx + Math.cos(mid) * labelDist;
                const lY = cy + Math.sin(mid) * labelDist;
                const isRight = Math.cos(mid) >= 0;
                items.push({ i, mid, eX, eY, lX, lY, isRight, cx, cy, oR,
                             labelY: lY,
                             // ticker stays as the REAL ticker — used
                             // for logo lookup, click filter, initials.
                             // displayLabel is the visible text only
                             // (e.g. "HARBOUR" instead of "2062").
                             ticker: donutTickers[i],
                             displayLabel: donutDisplayTickers[i],
                             pct: donutData[i] });
            });

            // Side is normally just the sign of cos(mid), which keeps every
            // label on the side its slice actually sits — the shortest,
            // non-crossing leader line. The one exception is a slice
            // sitting essentially AT 12 o'clock (cash, at 0%, here): its
            // side is arbitrary, and defaulting it to the left piles it on
            // top of the labels already crowding the upper-left.
            //
            // So only genuinely ambiguous labels get reassigned, and they
            // go to whichever side is less busy up top. Reassigning
            // anything further round (as blanket alternation did) drags a
            // label across the chart and its leader line crosses its
            // neighbours' — exactly what we want to avoid.
            const VERT_EPS = 0.10;
            const isTop = it => Math.sin(it.mid) < 0;
            const ambiguous = items.filter(
                it => isTop(it) && Math.abs(Math.cos(it.mid)) < VERT_EPS);
            if (ambiguous.length) {
                const nearTop = it => isTop(it) && Math.abs(Math.cos(it.mid)) < 0.55;
                let nLeft  = items.filter(it => nearTop(it) && !it.isRight &&
                                Math.abs(Math.cos(it.mid)) >= VERT_EPS).length;
                let nRight = items.filter(it => nearTop(it) && it.isRight &&
                                Math.abs(Math.cos(it.mid)) >= VERT_EPS).length;
                // Move only the SINGLE label closest to vertical. Reassigning
                // more than that starts shifting labels whose slice clearly
                // belongs to one side (MSE sits just left of 12 o'clock, so
                // it should stay left and read as pointing straight up),
                // which lengthens their leader lines and crosses neighbours.
                ambiguous
                    .sort((a, b) => Math.abs(Math.cos(a.mid)) -
                                    Math.abs(Math.cos(b.mid)))
                    .slice(0, 1)
                    .forEach(it => { it.isRight = (nRight <= nLeft); });
            }

            const oRef = items[0].oR;
            const cxRef = items[0].cx, cyRef = items[0].cy;
            const baseR = oRef + 30;
            const spacing = 36;               // two-line label height + breathing room
            // Smallest horizontal offset from centre. Enough that the two
            // sides stay clear of each other at 12/6 o'clock (their logos
            // end up ~48px apart and their text runs outwards), but small
            // enough that a label at the top reads as pointing straight up.
            const minDx = 22;
            const marginY = 20;
            const minY = marginY, maxY = chart.height - marginY;

            function resolveColumn(group) {
                group.sort((a, b) => a.labelY - b.labelY);
                for (let pass = 0; pass < 40; pass++) {
                    let moved = false;
                    for (let j = 1; j < group.length; j++) {
                        const gap = group[j].labelY - group[j-1].labelY;
                        if (gap < spacing) {
                            const shift = (spacing - gap) / 2;
                            group[j-1].labelY -= shift;
                            group[j].labelY += shift;
                            moved = true;
                        }
                    }
                    if (!moved) break;
                }
                group.forEach(it => {
                    it.labelY = Math.min(Math.max(it.labelY, minY), maxY);
                });
                // Settle again after clamping, walking down from the top.
                for (let pass = 0; pass < 20; pass++) {
                    let moved = false;
                    for (let j = 1; j < group.length; j++) {
                        if (group[j].labelY - group[j-1].labelY < spacing) {
                            group[j].labelY = Math.min(group[j-1].labelY + spacing, maxY);
                            moved = true;
                        }
                    }
                    if (!moved) break;
                }
            }
            resolveColumn(items.filter(it => !it.isRight));
            resolveColumn(items.filter(it => it.isRight));

            // Every label sits on the same circle. An earlier version
            // pushed alternate labels further out to vary the distance,
            // but because the vertical position was already fixed by the
            // de-overlap pass, a bigger radius only widened dx — the label
            // slid sideways instead of moving outwards, which is what made
            // URTS and WEMABANK's lines long and tilted.
            items.forEach(it => {
                const { i, eX, eY, isRight, labelY, ticker, displayLabel, pct, cx, cy, oR } = it;
                const color = donutColors[i];
                // Put the label back on its own ray at the resolved height:
                // the horizontal offset that lands on a circle of radius
                // it.r. Floored at minDx so a label near 12 or 6 o'clock
                // never sits dead-centre, where the two sides would meet.
                const dy = labelY - cy;
                // Horizontal offset that lands on the label circle at this
                // height. Floored at minDx so a label near 12 or 6 o'clock
                // doesn't sit dead-centre, and capped near the offset the
                // slice's own direction implies — without the cap, a label
                // whose height was nudged towards the centre line gets a
                // large dx and swings out sideways on a long, slanted
                // leader line (QPAY tilting left, WEMABANK reaching far
                // left) even though its slice is nowhere near there.
                const natural = Math.abs(Math.cos(it.mid)) * it.r;
                // Never place a label NEARER the centre than its own slice
                // points. When a label is nudged past the top of the label
                // circle the circle term collapses to 0 and it used to fall
                // back to minDx — so a left-side slice pointing 49px out got
                // a label only 22px out, and its leader line kicked back to
                // the RIGHT, which looks wrong on the left of the chart.
                const lo = Math.max(minDx, natural);
                const hi = Math.max(natural + 14, minDx);
                const dx = Math.min(
                    Math.max(Math.sqrt(Math.max(it.r * it.r - dy * dy, 0)), lo), hi);
                const finalX = cx + (isRight ? 1 : -1) * dx;

                ctx.save();
                // Straight line from donut edge to label
                ctx.beginPath();
                ctx.moveTo(eX, eY);
                ctx.lineTo(finalX, labelY);
                ctx.strokeStyle = _safeLineColor(color);
                ctx.lineWidth = 1.5;
                ctx.stroke();

                // Check if this stock is marked as undisclosed
                const hidden = (typeof isUndisclosed === 'function') && isUndisclosed(ticker);

                // Logo circle at end of line
                const logoR = 12;
                const logoX = finalX + (isRight ? logoR + 2 : -logoR - 2);
                const logoImg = logoImages[ticker];
                if (!hidden && logoImg) {
                    ctx.beginPath();
                    ctx.arc(logoX, labelY, logoR, 0, Math.PI * 2);
                    ctx.closePath();
                    ctx.save();
                    ctx.clip();
                    ctx.drawImage(logoImg, logoX - logoR, labelY - logoR, logoR*2, logoR*2);
                    ctx.restore();
                    ctx.beginPath();
                    ctx.arc(logoX, labelY, logoR, 0, Math.PI * 2);
                    ctx.strokeStyle = _safeLineColor(color);
                    ctx.lineWidth = 1.5;
                    ctx.stroke();
                } else {
                    // Filled circle with initials (or ? for undisclosed)
                    ctx.beginPath();
                    ctx.arc(logoX, labelY, logoR, 0, Math.PI * 2);
                    ctx.fillStyle = hidden ? '#555' : color;
                    ctx.fill();
                    // Outline so a near-black fill is delineated from the
                    // dark background.
                    if (!hidden && _donutLum(color) < 60) {
                        ctx.strokeStyle = '#9aa0ad';
                        ctx.lineWidth = 1;
                        ctx.stroke();
                    }
                    ctx.fillStyle = '#fff';
                    ctx.font = 'bold 9px -apple-system, sans-serif';
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';
                    const initials = hidden ? '?' : (ticker === 'CASH' ? '$' : ticker.substring(0, 2));
                    ctx.fillText(initials, logoX, labelY);
                }

                // Ticker + percentage text
                const textX = logoX + (isRight ? logoR + 6 : -logoR - 6);
                ctx.textAlign = isRight ? 'left' : 'right';
                ctx.fillStyle = '#e1e5ee';
                ctx.font = 'bold 12px -apple-system, sans-serif';
                const visible = hidden ? 'Undisclosed' : (displayLabel || ticker);
                ctx.fillText(visible, textX, labelY - 6);
                ctx.fillStyle = '#e1e5ee';
                ctx.font = 'bold 12px -apple-system, sans-serif';
                ctx.fillText(pct.toFixed(1) + '%', textX, labelY + 9);
                ctx.restore();
            });
        }
    };

    window._donutChart = new Chart(_ctx2d, {
        type: 'doughnut',
        data: {
            labels: donutLabels,
            datasets: [{
                data: donutData,
                backgroundColor: donutColors,
                // Dark slices (e.g. CIB black) get a light edge so the
                // boundary against the dark card background is visible;
                // all others keep the subtle dark gap between slices.
                borderColor: donutColors.map(c =>
                    _donutLum(c) < 60 ? 'rgba(255,255,255,0.5)' : 'rgba(26,29,39,0.6)'),
                borderWidth: 1,
                hoverBorderColor: '#fff',
                hoverBorderWidth: 2,
            }]
        },
        plugins: [labelPlugin],
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '42%',
            layout: { padding: { top: 50, bottom: 60, left: 135, right: 135 } },
            plugins: {
                legend: { display: false },
                tooltip: { enabled: false }
            },
            onClick: function(evt, elements) {
                if (elements.length > 0) {
                    const idx = elements[0].index;
                    const ticker = donutTickers[idx];
                    if (ticker !== 'CASH') filterStock(ticker);
                }
            }
        }
    });
}
} catch(e) { console.error('Donut chart error:', e); }
"""
        chart_js = donut_js + """
const chartLabels = """ + chart_labels + """;
const usdData = """ + chart_values + """;
const usdBaseline = """ + cost_basis_values + """;
const pctData = """ + chart_pct_values + """;
const pctBaseline = """ + chart_pct_baseline + """;

const ctx = document.getElementById('portfolioChart').getContext('2d');
const chart = new Chart(ctx, {
    type: 'line',
    data: {
        labels: chartLabels,
        datasets: [{
            label: 'Portfolio Value (USD)',
            data: usdData,
            borderColor: '#6c8cff',
            backgroundColor: 'rgba(108, 140, 255, 0.1)',
            fill: true, tension: 0.3, pointRadius: 3, pointHoverRadius: 6,
        }, {
            label: 'Cost Basis (USD)',
            data: usdBaseline,
            borderColor: '#8b8fa3', borderDash: [5, 5], borderWidth: 1,
            pointRadius: 0, fill: false,
        }]
    },
    options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
            legend: { display: true, labels: { color: '#8b8fa3', font: { size: 11 }, usePointStyle: true, pointStyle: 'line',
                generateLabels: function(chart) {
                    return chart.data.datasets.map(function(ds, i) {
                        return {
                            text: ds.label,
                            fontColor: '#8b8fa3',
                            strokeStyle: ds.borderColor,
                            fillStyle: 'transparent',
                            lineDash: ds.borderDash || [],
                            lineWidth: ds.borderWidth || 2,
                            pointStyle: 'line',
                            hidden: !chart.isDatasetVisible(i),
                            datasetIndex: i
                        };
                    });
                }
            } },
            tooltip: { callbacks: {
                label: function(c) {
                    const isPct = document.body.classList.contains('pct-mode');
                    const main = isPct
                        ? c.parsed.y.toFixed(1) + '%'
                        : '$' + c.parsed.y.toLocaleString(undefined, {minimumFractionDigits: 2});
                    return main;
                },
                // Append a second tooltip line with the stock's price
                // in its local currency on that date — only when a
                // single stock is selected. Look up by DATE STRING,
                // not array index, because the date range filter
                // (1M/3M/etc.) slices chart data so index 0 no longer
                // corresponds to history start.
                afterLabel: function(c) {
                    if (!activeStock) return '';
                    if (c.datasetIndex !== 0) return '';
                    const sd = perStockData[activeStock];
                    if (!sd || !sd.localByDate || !sd.currency) return '';
                    const label = c.chart && c.chart.data && c.chart.data.labels
                        ? c.chart.data.labels[c.dataIndex] : null;
                    if (!label) return '';
                    const lp = sd.localByDate[label];
                    if (lp === null || lp === undefined) return '';
                    let formatted;
                    if (lp >= 10000) formatted = lp.toLocaleString(undefined, {maximumFractionDigits: 0});
                    else if (lp >= 100) formatted = lp.toFixed(0);
                    else if (lp >= 1) formatted = lp.toFixed(2);
                    else formatted = lp.toFixed(4);
                    return 'Price: ' + formatted + ' ' + sd.currency;
                }
            }}
        },
        scales: {
            x: { type: 'time', time: { unit: 'week', tooltipFormat: 'dd MMM yyyy' },
                 grid: { color: '#2d3040' }, ticks: { color: '#8b8fa3', maxRotation: 45 } },
            y: { grid: { color: '#2d3040' }, ticks: { color: '#8b8fa3',
                callback: function(v) {
                    if (document.body.classList.contains('pct-mode'))
                        return v.toFixed(0) + '%';
                    return '$' + v.toLocaleString();
                }
            }}
        }
    }
});

function setChartMode(pct) {
    const stock = activeStock;
    if (stock) {
        chart.data.datasets[0].label = pct ? stock + ' Return (%)' : stock + ' Value (USD)';
        chart.data.datasets[1].label = pct ? 'Baseline (0%)' : stock + ' Cost Basis (USD)';
    } else {
        chart.data.datasets[0].label = pct ? 'Return (%)' : 'Portfolio Value (USD)';
        chart.data.datasets[1].label = pct ? 'Baseline (0%)' : 'Cost Basis (USD)';
    }
    applyChartFilters();
}

function toggleMode() {
    const body = document.body;
    const btn = document.getElementById('mode-toggle');
    body.classList.toggle('pct-mode');
    const isPct = body.classList.contains('pct-mode');
    btn.textContent = isPct ? 'Show $' : 'Show %';
    updateChartTitle();
    setChartMode(isPct);
}

// Light / dark mode toggle — class on <html> so the pre-paint inline
// script can apply it before <body> is parsed (no FOUC).
function _applyThemeIcon() {
    const btn = document.getElementById('theme-toggle');
    if (!btn) return;
    btn.textContent = document.documentElement.classList.contains('light-mode') ? '☀️' : '🌙';
}
function toggleTheme(skipSave) {
    const light = !document.documentElement.classList.contains('light-mode');
    document.documentElement.classList.toggle('light-mode', light);
    if (!skipSave) localStorage.setItem('ee-theme', light ? 'light' : 'dark');
    _applyThemeIcon();
    try { if (window._donutChart) window._donutChart.update(); } catch (_) {}
    try { if (window.chart && typeof window.chart.update === 'function') window.chart.update(); } catch (_) {}
}
(function _restoreTheme() {
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _applyThemeIcon);
    } else {
        _applyThemeIcon();
    }
})();

// Since-Inception IRR show/hide. Persists in localStorage so the
// choice survives reloads. The card collapses to a small "show ›"
// ghost the user can click to bring it back.
function _applyIrrHidden() {
    const hidden = localStorage.getItem('pf-irr-hidden') === '1';
    const card = document.getElementById('irr-card');
    const restore = document.getElementById('irr-restore');
    // display:none fully removes the card from the stats row (no gap),
    // so a screenshot is clean. The restore link lives by the chart title.
    if (card)    card.style.display    = hidden ? 'none' : '';
    if (restore) restore.style.display = hidden ? '' : 'none';
}
function toggleIrr(e) {
    if (e) e.stopPropagation();
    const cur = localStorage.getItem('pf-irr-hidden') === '1';
    localStorage.setItem('pf-irr-hidden', cur ? '0' : '1');
    _applyIrrHidden();
}
(function _restoreIrr() {
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _applyIrrHidden);
    } else {
        _applyIrrHidden();
    }
})();

// Time range filtering
let currentRange = 'ALL';
let customStart = null;
let customEnd = null;

function getStartDate(range) {
    const now = new Date();
    if (range === '1M') {
        const d = new Date(now);
        d.setMonth(d.getMonth() - 1);
        return d.toISOString().slice(0, 10);
    } else if (range === 'QTD') {
        const q = Math.floor(now.getMonth() / 3) * 3;
        return new Date(now.getFullYear(), q, 1).toISOString().slice(0, 10);
    } else if (range === 'YTD') {
        return now.getFullYear() + '-01-01';
    } else if (range === 'CUSTOM') {
        return customStart;
    }
    return null; // ALL
}

function getEndDate(range) {
    if (range === 'CUSTOM') return customEnd;
    return null;  // other ranges run up to today / latest snapshot
}

function filterByRange(labels, values, costBasis, startDate, endDate) {
    if (!startDate && !endDate) return { labels, values, cost: costBasis };
    const filteredLabels = [];
    const filteredValues = [];
    const filteredCost = [];
    for (let i = 0; i < labels.length; i++) {
        if ((!startDate || labels[i] >= startDate) &&
            (!endDate   || labels[i] <= endDate)) {
            filteredLabels.push(labels[i]);
            filteredValues.push(values[i]);
            filteredCost.push(costBasis[i]);
        }
    }
    return { labels: filteredLabels, values: filteredValues, cost: filteredCost };
}

function onCustomDateChange() {
    customStart = document.getElementById('custom-start').value || null;
    customEnd   = document.getElementById('custom-end').value   || null;
    if (currentRange === 'CUSTOM') applyChartFilters();
}

function recalcPct(values, costBasis) {
    return values.map((v, i) => {
        const c = costBasis[i];
        return c > 0 ? Math.round(((v - c) / c) * 100 * 100) / 100 : 0;
    });
}

// Modified-Dietz % series rebased to start of the filtered range.
// Day 0 → 0%, last day → period return matching the stat-box value.
function recalcPctRebased(values, costBasis) {
    if (!values.length) return [];
    const v0 = values[0] || 0;
    const c0 = costBasis[0] || 0;
    const startGain = v0 - c0;
    return values.map((v, i) => {
        const c = costBasis[i];
        const periodDeposits = c - c0;
        const gain = (v - c) - startGain;
        const denom = v0 + 0.5 * periodDeposits;
        if (denom <= 0) return 0;
        return Math.round((gain / denom) * 100 * 100) / 100;
    });
}

function setRange(range) {
    currentRange = range;
    document.querySelectorAll('.range-pill').forEach(p => p.classList.remove('active'));
    document.querySelector('.range-pill[onclick*=\"' + range + '\"]').classList.add('active');

    // Show/hide custom date fields
    const customBox = document.getElementById('custom-range-fields');
    if (customBox) {
        customBox.style.display = (range === 'CUSTOM') ? 'inline-flex' : 'none';
    }
    // On first activation of CUSTOM, seed defaults: start = 3 months ago, end = today
    if (range === 'CUSTOM') {
        const startInput = document.getElementById('custom-start');
        const endInput   = document.getElementById('custom-end');
        if (startInput && !startInput.value) {
            const now = new Date();
            const ago = new Date(now); ago.setMonth(ago.getMonth() - 3);
            startInput.value = ago.toISOString().slice(0, 10);
            endInput.value   = now.toISOString().slice(0, 10);
            customStart = startInput.value;
            customEnd   = endInput.value;
        }
    }
    applyChartFilters();
}

function applyChartFilters() {
    const startDate = getStartDate(currentRange);
    const isPct = document.body.classList.contains('pct-mode');

    let srcValues, srcCost;

    if (activeStock && perStockData[activeStock]) {
        const sd = perStockData[activeStock];
        srcValues = sd.values;
        srcCost = sd.cost;
    } else {
        srcValues = usdData;
        srcCost = usdBaseline;
    }

    let effectiveStart = startDate;

    // For individual stocks, skip to first non-zero value if it's after range start
    if (activeStock && perStockData[activeStock]) {
        const sd = perStockData[activeStock];
        let buyIdx = sd.values.findIndex(v => v > 0);
        if (buyIdx >= 0) {
            const buyDate = chartLabels[buyIdx];
            if (!effectiveStart || buyDate > effectiveStart) {
                effectiveStart = buyDate;
            }
        }
    }

    const endDate = getEndDate(currentRange);
    const filtered = filterByRange(chartLabels, srcValues, srcCost, effectiveStart, endDate);
    // Portfolio-level non-ALL ranges: rebase to 0% at period start so
    // the chart matches the stat-box period return.
    const filteredPct = (activeStock || currentRange === 'ALL')
        ? recalcPct(filtered.values, filtered.cost)
        : recalcPctRebased(filtered.values, filtered.cost);
    const filteredPctBase = filtered.labels.map(() => 0);

    chart.data.labels = filtered.labels;

    if (isPct) {
        chart.data.datasets[0].data = filteredPct;
        chart.data.datasets[1].data = filteredPctBase;
    } else {
        chart.data.datasets[0].data = filtered.values;
        chart.data.datasets[1].data = filtered.cost;
    }
    chart.update();

    // Update summary stats based on range
    updateStats(filtered, filteredPct);
}

function updateStats(filtered, filteredPct) {
    const statReturn = document.getElementById('stat-return');
    const statLabel = document.getElementById('stat-return-label');
    const statInvested = document.getElementById('stat-invested');
    const statCurrent = document.getElementById('stat-current');
    if (!statReturn || !filtered.values.length) return;

    const startVal = filtered.values[0] || 0;
    const startCost = filtered.cost[0] || 0;
    const endVal = filtered.values[filtered.values.length - 1] || 0;
    const endCost = filtered.cost[filtered.cost.length - 1] || 0;

    const labels = { '1M': '1M Return', 'QTD': 'QTD Return', 'YTD': 'YTD Return',
                     'ALL': 'Total Return', 'CUSTOM': 'Custom Return' };
    statLabel.textContent = labels[currentRange] || 'Total Return';

    // Total Invested and Current Value always show actual figures
    if (statInvested) statInvested.textContent = '$' + endCost.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});
    if (statCurrent) statCurrent.textContent = '$' + endVal.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});

    let gain, pct;

    if (currentRange === 'ALL') {
        // Total return vs total cost basis
        gain = endVal - endCost;
        pct = endCost > 0 ? (gain / endCost) * 100 : 0;
    } else {
        // Period return excluding new deposits
        if (activeStock) {
            gain = endVal - endCost;
            pct = endCost > 0 ? (gain / endCost) * 100 : 0;
        } else {
            // Portfolio level — Modified Dietz (mid-period weighting
            // on intra-period deposits). Dividing pure gain by the
            // tiny start-of-period balance massively over-prints
            // when most deposits happen mid-period.
            const endGain = endVal - endCost;
            const startGain = startVal - startCost;
            gain = endGain - startGain;
            const periodDeposits = endCost - startCost;
            const denom = startVal + 0.5 * periodDeposits;
            if (denom > 0) {
                pct = (gain / denom) * 100;
            } else if (endCost > 0) {
                gain = endVal - endCost;
                pct = (gain / endCost) * 100;
            } else {
                pct = 0;
            }
        }
    }

    const cls = pct >= 0 ? 'stat-pos' : 'stat-neg';
    statReturn.className = 'value ' + cls;
    const usdStr = '<span class="usd-only">$' + gain.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) + ' </span>';
    statReturn.innerHTML = usdStr + '(' + pct.toFixed(1) + '%)';

    // Update per-stock returns in holdings table
    updateHoldingReturns();

    // Update best/worst performers
    updatePerformers();
}

function updateHoldingReturns() {
    const startDate = getStartDate(currentRange);

    Object.keys(perStockData).forEach(ticker => {
        // Sold-out positions: keep the server-rendered realized return.
        // chart's endVal is 0 → generic calc would print -100% for a
        // winning trade.
        if (typeof soldOutTickers !== 'undefined' && soldOutTickers.has(ticker)) {
            return;
        }
        const sd = perStockData[ticker];

        // Find the first date this stock has a non-zero value (= purchase date)
        let stockStartIdx = 0;
        for (let i = 0; i < sd.values.length; i++) {
            if (sd.values[i] > 0) { stockStartIdx = i; break; }
        }
        const stockBuyDate = chartLabels[stockStartIdx];

        // Use the later of: range start date or stock buy date
        const effectiveStart = startDate && startDate > stockBuyDate ? startDate : stockBuyDate;
        const endDate = getEndDate(currentRange);
        const filtered = filterByRange(chartLabels, sd.values, sd.cost, effectiveStart, endDate);

        if (!filtered.values.length) return;

        const startVal = filtered.values[0];
        const startCost = filtered.cost[0];
        const endVal = filtered.values[filtered.values.length - 1];
        // Use the cumulative lifetime cost (cost basis at the END of
        // the window) so multiple buys don't look like profit. The
        // first-day cost only reflects the first lot.
        const endCost = filtered.cost[filtered.cost.length - 1];

        // For ALL: return vs cost basis. For time ranges: return vs start value
        // If stock was bought after range start, use cost basis
        let usdPct;
        if (currentRange === 'ALL' || (startDate && stockBuyDate >= startDate)) {
            // Use cost basis — stock was bought within or before this period
            usdPct = endCost > 0 ? ((endVal - endCost) / endCost) * 100 : 0;
        } else {
            // Use start-of-period value
            usdPct = startVal > 0 ? ((endVal - startVal) / startVal) * 100 : 0;
        }

        const localPct = usdPct;

        // Update price return cell
        const usdCell = document.querySelector('[data-return-usd="' + ticker + '"]');
        if (usdCell) {
            usdCell.textContent = (usdPct >= 0 ? '+' : '') + usdPct.toFixed(1) + '%';
            usdCell.className = usdPct >= 0 ? 'gain-pos' : 'gain-neg';
            usdCell.setAttribute('data-return-usd', ticker);
        }

        // Update total return cell = price return + dividends received.
        // Dividends are lifetime (don't scale with chart range), so we only
        // add them when the displayed price return is itself cost-basis.
        const totalCell = document.querySelector('[data-return-total="' + ticker + '"]');
        if (totalCell) {
            const usingCostBasis = (currentRange === 'ALL' || (startDate && stockBuyDate >= startDate));
            const divPct = (usingCostBasis && stockDivPct[ticker]) ? stockDivPct[ticker] : 0;
            const totalPct = usdPct + divPct;
            totalCell.textContent = (totalPct >= 0 ? '+' : '') + totalPct.toFixed(1) + '%';
            totalCell.className = totalPct >= 0 ? 'gain-pos' : 'gain-neg';
            totalCell.setAttribute('data-return-total', ticker);
        }
    });
}

// Stock name map for display
const stockNames = """ + json.dumps({h["ticker"]: h["name"] for h in holdings}) + """;
// Dividends received as % of invested basis (USD), per ticker.
// Added to price return when displaying total return on cost-basis views.
const stockDivPct = """ + json.dumps(stock_div_pct) + """;
// Sold-out tickers — chart endVal is 0 for these, so the generic
// (endVal-startCost)/startCost calc would print -100% even for a
// winning trade. The server-rendered cell already shows the realized
// return; skip the JS override.
const soldOutTickers = new Set(""" + json.dumps([h["ticker"] for h in holdings if h.get("is_sold_out")]) + """);
const totalHoldings = """ + str(sum(1 for h in holdings if not h.get("is_sold_out"))) + """;

function updatePerformers() {
    const bestEl = document.getElementById('best-performer');
    const worstEl = document.getElementById('worst-performer');
    if (!bestEl || !worstEl) return;

    const startDate = getStartDate(currentRange);
    let bestTicker = null, bestPct = -Infinity;
    let worstTicker = null, worstPct = Infinity;

    Object.keys(perStockData).forEach(ticker => {
        // Sold-out positions: chart endVal is 0, read realized return
        // from the server-rendered cell instead.
        if (typeof soldOutTickers !== 'undefined' && soldOutTickers.has(ticker)) {
            // Rank by TOTAL return (incl. dividends) — read the total cell.
            const cell = document.querySelector('[data-return-total="' + ticker + '"]');
            if (!cell) return;
            const txt = (cell.textContent || '').trim().replace('%','').replace('+','');
            const pct = parseFloat(txt);
            if (!isFinite(pct)) return;
            if (pct > bestPct)  { bestPct = pct; bestTicker = ticker; }
            if (pct < worstPct) { worstPct = pct; worstTicker = ticker; }
            return;
        }
        const sd = perStockData[ticker];

        // Find first non-zero (buy date)
        let buyIdx = sd.values.findIndex(v => v > 0);
        if (buyIdx < 0) return;
        const buyDate = chartLabels[buyIdx];
        const effectiveStart = startDate && startDate > buyDate ? startDate : buyDate;
        const endDate = getEndDate(currentRange);

        const filtered = filterByRange(chartLabels, sd.values, sd.cost, effectiveStart, endDate);
        if (!filtered.values.length) return;

        const startVal = filtered.values[0];
        const startCost = filtered.cost[0];
        const endVal = filtered.values[filtered.values.length - 1];
        const endCost = filtered.cost[filtered.cost.length - 1];

        let pct;
        const usingCostBasis = (currentRange === 'ALL' || (startDate && buyDate >= startDate));
        if (usingCostBasis) {
            // Lifetime cost basis (see updateHoldingReturns comment).
            pct = endCost > 0 ? ((endVal - endCost) / endCost) * 100 : 0;
        } else {
            pct = startVal > 0 ? ((endVal - startVal) / startVal) * 100 : 0;
        }
        // Rank by TOTAL return: add dividends received (lifetime, so only
        // on cost-basis views — same rule as updateHoldingReturns()).
        if (usingCostBasis && stockDivPct[ticker]) pct += stockDivPct[ticker];

        if (pct > bestPct) { bestPct = pct; bestTicker = ticker; }
        if (pct < worstPct) { worstPct = pct; worstTicker = ticker; }
    });

    if (bestTicker) {
        const name = stockNames[bestTicker] || bestTicker;
        bestEl.textContent = name + ' (' + bestTicker + ') ' + (bestPct >= 0 ? '+' : '') + bestPct.toFixed(1) + '%';
        bestEl.className = 'stock ' + (bestPct >= 0 ? 'gain-pos' : 'gain-neg');
    }
    if (worstTicker) {
        const name = stockNames[worstTicker] || worstTicker;
        worstEl.textContent = name + ' (' + worstTicker + ') ' + (worstPct >= 0 ? '+' : '') + worstPct.toFixed(1) + '%';
        worstEl.className = 'stock ' + (worstPct >= 0 ? 'gain-pos' : 'gain-neg');
    }
}

function toggleUndisclosed(el, ticker) {
    const row = el.closest('.holding-row');
    row.classList.toggle('undisclosed');
    el.classList.toggle('is-hidden');
    el.textContent = row.classList.contains('undisclosed') ? '👁‍🗨' : '👁';

    // Also hide in the banner and chart title if this stock is selected
    if (activeStock === ticker) {
        updateChartTitle();
        updateStockIndicator();
    }
    // Redraw donut chart to update labels
    if (window._donutChart) window._donutChart.update();
}

function isUndisclosed(ticker) {
    const row = document.querySelector('.holding-row[data-ticker="' + ticker + '"]');
    return row && row.classList.contains('undisclosed');
}

function getDisplayName(ticker) {
    if (isUndisclosed(ticker) && document.body.classList.contains('pct-mode')) {
        return 'Undisclosed Holding';
    }
    return stockNames[ticker] || ticker;
}

function updateStockIndicator() {
    const label = document.getElementById('stat-holdings-label');
    const value = document.getElementById('stat-holdings-value');
    const banner = document.getElementById('stock-banner');
    const bannerText = document.getElementById('banner-text');

    if (activeStock) {
        const name = getDisplayName(activeStock);
        if (label) { label.textContent = 'Selected'; }
        if (value) { value.textContent = name; value.style.fontSize = '1rem'; }
        if (banner) { banner.classList.add('visible'); }
        if (bannerText) { bannerText.textContent = 'Showing: ' + name; }
    } else {
        if (label) { label.textContent = 'Holdings'; }
        if (value) { value.textContent = totalHoldings + ' stocks'; value.style.fontSize = ''; }
        if (banner) { banner.classList.remove('visible'); }
    }
}

// Per-stock chart data
const perStockData = """ + per_stock_json + """;
let activeStock = null;

function updateChartTitle() {
    const isPct = document.body.classList.contains('pct-mode');
    const title = document.querySelector('.chart-title');
    if (activeStock) {
        const name = getDisplayName(activeStock);
        title.textContent = isPct
            ? name + ' Return (%)'
            : name + ' Value (USD)';
    } else {
        title.textContent = isPct ? 'Portfolio Return (%)' : 'Portfolio Value (USD)';
    }
}

function filterStock(ticker) {
    const rows = document.querySelectorAll('.holding-row');
    const isPct = document.body.classList.contains('pct-mode');

    if (activeStock === ticker) {
        // Deselect — show all
        activeStock = null;
        rows.forEach(r => r.classList.remove('active-stock'));
        setChartMode(isPct);
    } else {
        // Select this stock
        activeStock = ticker;
        rows.forEach(r => {
            r.classList.toggle('active-stock', r.dataset.ticker === ticker);
        });
        chart.data.datasets[0].label = isPct ? ticker + ' Return (%)' : ticker + ' Value (USD)';
        chart.data.datasets[1].label = isPct ? 'Baseline (0%)' : ticker + ' Cost Basis (USD)';
        applyChartFilters();
    }
    updateChartTitle();
    updateStockIndicator();
}
"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#0f1117">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🌍</text></svg>">
<link rel="manifest" href="/manifest.json">
<title>Emerging Edge Portfolio</title>
<script>
try {{ if (localStorage.getItem('ee-theme') === 'light')
       document.documentElement.classList.add('light-mode'); }} catch (_) {{}}
</script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
:root {{
    --bg: #0f1117; --surface: #1a1d27; --surface2: #232733;
    --border: #2d3040; --text: #e2e4ea; --text-muted: #8b8fa3;
    --accent: #6c8cff; --red: #ff8095; --green: #4ddb8a;
    --green-dim: rgba(77,219,138,0.12); --red-dim: rgba(255,128,149,0.12);
}}
html.light-mode {{
    --bg: #f7f8fb; --surface: #ffffff; --surface2: #eef0f5;
    --border: #d6d9e0; --text: #1c1f2c; --text-muted: #5a6075;
    --accent: #3b5bdb; --red: #d12d4a; --green: #1e9560;
    --green-dim: rgba(30,149,96,0.10); --red-dim: rgba(209,45,74,0.10);
    color-scheme: light;
}}
.theme-toggle {{
    background: transparent; border: 1px solid var(--border);
    color: var(--text-muted); font-size: 0.95rem;
    padding: 0.18rem 0.42rem; border-radius: 6px;
    cursor: pointer; line-height: 1; transition: all 0.15s;
    margin-right: 0.5rem;
}}
.theme-toggle:hover {{ color: var(--text); background: var(--surface2); }}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Inter', system-ui, sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.6;
}}
.header {{
    background: var(--surface); border-bottom: 1px solid var(--border);
    padding: 1rem 2rem; display: flex; align-items: center;
    justify-content: space-between; flex-wrap: wrap; gap: 1rem;
}}
.header h1 {{ font-size: 1.2rem; font-weight: 700; }}
.header h1 span {{ color: var(--accent); }}
.nav-link {{
    color: var(--accent); text-decoration: none; font-size: 0.8rem;
    font-weight: 600; padding: 0.3rem 0.8rem; border-radius: 999px;
    border: 1px solid var(--accent); transition: background 0.15s;
}}
.nav-link:hover {{ background: var(--accent); color: #fff; }}
.container {{ max-width: 1200px; margin: 0 auto; padding: 1.5rem 2rem; }}

/* Stats */
.stats {{ display: flex; gap: 1.5rem; flex-wrap: wrap; margin-bottom: 1.5rem; }}
.stat-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 0.8rem 1.2rem; flex: 1; min-width: 150px;
}}
.stat-card .label {{ font-size: 0.7rem; text-transform: uppercase;
    letter-spacing: 0.06em; color: var(--text-muted); }}
.stat-card .value {{ font-size: 1.3rem; font-weight: 700; }}
.stat-pos {{ color: var(--green); }}
.stat-neg {{ color: var(--red); }}
#irr-card {{ position: relative; }}
.irr-hide {{
    position: absolute; top: 4px; right: 7px;
    font-size: 0.85rem; line-height: 1; color: var(--text-muted);
    cursor: pointer; opacity: 0.4; transition: opacity 0.15s;
}}
.irr-hide:hover {{ opacity: 0.9; }}
.irr-restore {{
    background: none; border: 1px dashed var(--border); color: var(--text-muted);
    border-radius: 6px; padding: 0.15rem 0.5rem; font-size: 0.72rem;
    cursor: pointer; opacity: 0.7; transition: opacity 0.15s;
}}
.irr-restore:hover {{ opacity: 1; color: var(--text); }}

/* Chart */
.chart-container {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 1rem; margin-bottom: 1.5rem;
    position: relative; height: 350px;
}}
.chart-title {{ font-size: 0.85rem; font-weight: 700; margin-bottom: 0.5rem; }}

/* Tables */
.section-title {{
    font-size: 0.95rem; font-weight: 700; margin: 1.5rem 0 0.75rem;
    display: flex; align-items: center; gap: 0.5rem;
}}
table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
th {{
    text-align: left; padding: 0.5rem 0.6rem; font-size: 0.68rem;
    text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-muted);
    border-bottom: 1px solid var(--border);
}}
td {{ padding: 0.5rem 0.6rem; border-bottom: 1px solid var(--border); }}
tr:hover td {{ background: var(--surface2); }}
.muted {{ color: var(--text-muted); font-size: 0.7rem; }}
/* Dividends cell carries a per-payment breakdown on hover — the number
   shown is a lifetime total, so hint that there's more behind it. */
.has-div-tip {{ cursor: help; text-decoration: underline dotted 1px;
                text-underline-offset: 3px; }}
.gain-pos {{ color: var(--green); font-weight: 700; }}
.gain-neg {{ color: var(--red); font-weight: 700; }}
.txn-buy td:nth-child(4) {{ color: var(--green); }}
.txn-sell td:nth-child(4) {{ color: var(--red); }}
.txn-div td:nth-child(3) {{ color: var(--accent); }}
.txn-reinvest td:nth-child(3) {{ color: #b48cff; }}
.del-btn, .edit-btn {{
    cursor: pointer; color: var(--text-muted); font-size: 0.8rem;
    padding: 0.2rem 0.4rem; border-radius: 4px;
}}
.del-btn:hover {{ color: var(--red); background: var(--red-dim); }}
.edit-btn:hover {{ color: var(--accent); background: var(--surface); }}
.edit-row input, .edit-row select {{
    background: var(--surface); color: var(--text);
    border: 1px solid var(--border); border-radius: 4px;
    padding: 0.2rem 0.3rem; font-size: 0.85rem; width: 100%;
    box-sizing: border-box;
}}
.edit-row .edit-shares, .edit-row .edit-price {{ width: 5rem; }}
.edit-row .edit-currency {{ width: 4rem; }}

/* Add transaction form */
.add-txn-form {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 0.8rem; margin-top: 0.75rem;
    display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: end;
}}
.add-txn-form .field {{ display: flex; flex-direction: column; gap: 0.2rem; }}
.add-txn-form label {{ font-size: 0.6rem; text-transform: uppercase; color: var(--text-muted); letter-spacing: 0.05em; }}
.add-txn-form input, .add-txn-form select {{
    background: var(--surface2); border: 1px solid var(--border); color: var(--text);
    border-radius: 4px; padding: 0.3rem 0.5rem; font-size: 0.78rem; width: auto;
}}
.add-txn-form input:focus, .add-txn-form select:focus {{ border-color: var(--accent); outline: none; }}
.add-txn-form input {{ width: 90px; }}
.add-txn-form input[type="date"] {{ width: 140px; }}
.add-txn-form #txn-stock-search {{ width: 240px; }}
.add-txn-form select {{ min-width: 80px; }}
/* Autocomplete dropdown for stock search */
.txn-autocomplete-results {{
    display: none; position: absolute; top: 100%; left: 0;
    width: 100%; min-width: 300px; max-height: 280px; overflow-y: auto;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; box-shadow: 0 6px 20px rgba(0,0,0,0.4);
    z-index: 100; margin-top: 0.2rem;
}}
.txn-autocomplete-item {{
    padding: 0.45rem 0.6rem; cursor: pointer;
    border-bottom: 1px solid var(--border); font-size: 0.82rem;
}}
.txn-autocomplete-item:last-child {{ border-bottom: none; }}
.txn-autocomplete-item:hover {{ background: var(--surface2); }}
.add-txn-btn {{
    padding: 0.35rem 0.8rem; border-radius: 4px; border: none;
    background: var(--accent); color: #fff; font-size: 0.78rem;
    font-weight: 600; cursor: pointer;
}}
.add-txn-btn:hover {{ background: #5a7ae6; }}
/* CONVERT helpers — cash-balance chips + Available hint + Use-all button */
.txn-cash-chips {{
    display: flex; flex-wrap: wrap; gap: 0.3rem; max-width: 100%;
}}
.txn-cash-chip {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 0.25rem 0.55rem;
    font-size: 0.72rem; color: var(--text);
    cursor: pointer; white-space: nowrap;
    transition: border-color 0.1s, background 0.1s;
}}
.txn-cash-chip:hover {{
    border-color: var(--accent); background: var(--surface2);
}}
.txn-cash-chip strong {{ color: var(--accent); margin-right: 0.15rem; }}
.txn-from-max {{
    padding: 0.25rem 0.5rem; border-radius: 4px; border: 1px solid var(--border);
    background: var(--surface); color: var(--text-muted);
    font-size: 0.7rem; font-weight: 600; cursor: pointer;
}}
.txn-from-max:hover {{
    border-color: var(--accent); color: var(--accent);
}}
.txn-balance-hint {{
    font-size: 0.7rem; color: var(--text-muted); margin-top: 0.25rem;
    min-height: 1em;
}}
.txn-balance-hint.warn {{ color: #ffb74d; }}
/* Holdings share toolbar — copy-as-markdown + save-as-png */
.holdings-share-bar {{
    display: flex; align-items: center; gap: 0.5rem;
    margin-bottom: 0.5rem; flex-wrap: wrap;
}}
.holdings-share-btn {{
    background: var(--surface); border: 1px solid var(--border);
    color: var(--text); border-radius: 6px;
    padding: 0.35rem 0.7rem; font-size: 0.75rem; font-weight: 600;
    cursor: pointer; transition: border-color 0.1s, background 0.1s;
}}
.holdings-share-btn:hover {{ border-color: var(--accent); background: var(--surface2); }}
.holdings-share-btn.busy {{ opacity: 0.6; cursor: wait; }}
.holdings-share-hint {{ font-size: 0.7rem; }}
/* Per-row ✕ to hide a sold-out position. Only shown in % view. */
.sold-remove-btn {{
    display: inline-block; margin-left: 0.4rem;
    color: var(--text-muted); cursor: pointer;
    font-size: 0.85rem; padding: 0 0.25rem;
    border-radius: 3px; line-height: 1;
}}
.sold-remove-btn:hover {{ color: var(--red); background: var(--surface2); }}
.sold-hidden-toggle {{
    margin-top: 0.5rem; font-size: 0.75rem; color: var(--text-muted);
}}
.sold-hidden-toggle a {{ color: var(--accent); text-decoration: underline; }}
.table-wrap {{ overflow-x: auto; }}

/* Performers */
.performers {{ display: flex; gap: 1rem; margin-bottom: 1.5rem; flex-wrap: wrap; }}
.performer {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 0.6rem 1rem; flex: 1; min-width: 200px;
}}
.performer .label {{ font-size: 0.65rem; text-transform: uppercase; color: var(--text-muted); }}
.performer .stock {{ font-weight: 700; font-size: 0.85rem; }}

/* Mobile */
@media (max-width: 600px) {{
    body {{ font-size: 13px; }}
    .header {{ padding: 0.6rem 0.8rem; }}
    .header h1 {{ font-size: 1rem; }}
    .container {{ padding: 1rem 0.8rem; }}
    .stats {{ gap: 0.8rem; }}
    .stat-card {{ padding: 0.5rem 0.8rem; min-width: 120px; }}
    .stat-card .value {{ font-size: 1rem; }}
    .chart-container {{ height: 250px; padding: 0.6rem; }}
    table {{ font-size: 0.7rem; }}
    th, td {{ padding: 0.35rem 0.4rem; }}
}}

.empty {{ text-align: center; padding: 2rem; color: var(--text-muted); }}
/* Welcome / landing state for empty portfolio */
.welcome {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 2rem 2.4rem;
    margin: 1.2rem 0 1.4rem;
    max-width: 900px;
}}
.welcome h2 {{ font-size: 1.4rem; font-weight: 700; }}
.welcome p {{ margin: 0.8rem 0 1.4rem; }}
.welcome-txn-guide {{
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 8px; padding: 1rem 1.2rem;
}}
.welcome-txn-title {{
    font-size: 0.78rem; text-transform: uppercase;
    letter-spacing: 0.06em; color: var(--text-muted);
    font-weight: 700; margin-bottom: 0.6rem;
}}
.welcome-txn-guide dl {{
    margin: 0;
    display: grid; grid-template-columns: auto 1fr;
    gap: 0.55rem 0.9rem; align-items: start;
}}
.welcome-txn-guide dt {{ font-size: 0.78rem; }}
.welcome-txn-guide dd {{
    margin: 0; font-size: 0.85rem; line-height: 1.45;
    color: var(--text-muted);
}}
.welcome-txn-tag {{
    display: inline-block;
    padding: 0.18rem 0.55rem;
    font-size: 0.7rem; font-weight: 700;
    border-radius: 4px; letter-spacing: 0.04em;
    min-width: 72px; text-align: center;
}}
.welcome-txn-buy  {{ background: rgba(52,211,153,0.15);  color: var(--green);  border: 1px solid rgba(52,211,153,0.4); }}
.welcome-txn-sell {{ background: rgba(255,107,107,0.15); color: var(--red);    border: 1px solid rgba(255,107,107,0.4); }}
.welcome-txn-div  {{ background: rgba(108,140,255,0.15); color: var(--accent); border: 1px solid rgba(108,140,255,0.4); }}
.welcome-txn-rei  {{ background: rgba(249,115,22,0.15);  color: #f97316;       border: 1px solid rgba(249,115,22,0.4);  }}
.welcome-txn-con  {{ background: rgba(167,139,250,0.15); color: #a78bfa;       border: 1px solid rgba(167,139,250,0.4); }}
.welcome-hint {{
    margin-top: 0.9rem; padding-top: 0.8rem;
    border-top: 1px solid var(--border);
    font-size: 0.75rem; color: var(--text-muted);
}}
.welcome-hint code {{
    background: var(--surface2); padding: 0.1rem 0.35rem;
    border-radius: 3px; font-size: 0.72rem;
}}

/* Undisclosed stock toggle */
.hide-toggle {{
    cursor: pointer; font-size: 0.8rem; margin-left: 0.4rem;
    opacity: 0.5; vertical-align: middle;
}}
.hide-toggle:hover {{ opacity: 1; }}
.hide-toggle.is-hidden {{ opacity: 0.3; }}
/* Donut allocation chart — Fiscal AI style with leader-line labels */
.donut-section {{ margin-bottom: 1.2rem; }}
.donut-section-header {{
    display: flex; justify-content: space-between; align-items: center;
    max-width: 720px; margin: 0 auto 0.5rem;
}}
.manage-logos-btn {{
    background: var(--surface2); color: var(--accent);
    border: 1px solid var(--accent); border-radius: 999px;
    padding: 0.25rem 0.8rem; font-size: 0.72rem; font-weight: 600;
    cursor: pointer;
}}
.manage-logos-btn:hover {{ background: var(--accent-dim); }}
.donut-chart-box {{
    width: 100%; max-width: 720px; height: 520px;
    margin: 0 auto;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 0.5rem;
}}
@media (max-width: 600px) {{
    .donut-chart-box {{ height: 340px; }}
}}

/* Logo manager modal */
.logo-mgr-overlay {{
    position: fixed; inset: 0; background: rgba(0,0,0,0.65);
    z-index: 600; display: flex; align-items: flex-start;
    justify-content: center; padding-top: 7vh;
    backdrop-filter: blur(4px);
}}
.logo-mgr-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 1.5rem;
    width: min(640px, 92vw); max-height: 80vh; overflow-y: auto;
    box-shadow: 0 20px 60px rgba(0,0,0,0.5);
}}
.logo-mgr-header {{
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 0.6rem;
}}
.logo-mgr-close {{
    cursor: pointer; font-size: 1.3rem; color: var(--text-muted);
    width: 28px; height: 28px; display: flex;
    align-items: center; justify-content: center; border-radius: 6px;
}}
.logo-mgr-close:hover {{ background: var(--surface2); color: var(--text); }}
.logo-mgr-list {{ display: flex; flex-direction: column; gap: 0.5rem; }}
.logo-mgr-row {{
    display: flex; justify-content: space-between; align-items: center;
    padding: 0.6rem 0.8rem; background: var(--bg);
    border: 1px solid var(--border); border-radius: 8px;
}}
.logo-mgr-left {{ display: flex; align-items: center; gap: 0.8rem; }}
.logo-mgr-thumb {{
    width: 40px; height: 40px; border-radius: 50%; object-fit: cover;
    background: var(--surface2); border: 1px solid var(--border);
    flex-shrink: 0;
}}
.logo-mgr-placeholder {{
    display: flex; align-items: center; justify-content: center;
    color: #fff; font-weight: 700; font-size: 0.78rem;
    letter-spacing: 0.03em; text-transform: uppercase;
    border: none;
}}
.logo-mgr-name {{ font-weight: 600; color: var(--text); font-size: 0.9rem; }}
.logo-mgr-meta {{ color: var(--text-muted); font-size: 0.72rem; }}
.logo-mgr-upload-btn {{
    display: inline-block; padding: 0.35rem 0.9rem;
    background: var(--accent); color: #fff;
    border-radius: 999px; font-size: 0.75rem; font-weight: 600;
    cursor: pointer;
}}
.logo-mgr-upload-btn:hover {{ opacity: 0.9; }}

/* Status label dropdown */
.status-select {{
    background: transparent; color: var(--text-muted);
    border: 1px solid var(--border); border-radius: 4px;
    padding: 0.15rem 0.3rem; font-size: 0.75rem;
    font-weight: 700; cursor: pointer; text-align: center;
}}
.status-select:focus {{ border-color: var(--accent); outline: none; }}
.status-select option {{ background: var(--bg); }}

.cash-row {{ background: rgba(108,140,255,0.05); cursor: default !important; }}
.cash-row:hover {{ background: rgba(108,140,255,0.08) !important; }}
.holding-row.undisclosed .stock-name-full {{ display: none !important; }}
.holding-row.undisclosed .stock-name-hidden {{ display: inline !important; }}
body.pct-mode .holding-row.undisclosed .pct-only:not(.hide-toggle):not(:has(.status-select)) {{ color: transparent !important; }}
body.pct-mode .holding-row.undisclosed .pct-only:not(.hide-toggle):not(:has(.status-select)) * {{ color: transparent !important; }}

/* Selected stock banner */
.stock-banner {{
    display: none; padding: 0.4rem 0.8rem; margin-bottom: 0.75rem;
    background: var(--accent); background: rgba(108,140,255,0.12);
    border: 1px solid var(--accent); border-radius: 8px;
    font-size: 0.82rem; font-weight: 600; color: var(--accent);
    align-items: center; justify-content: space-between;
}}
.stock-banner.visible {{ display: flex; }}
.stock-banner .close-x {{
    cursor: pointer; padding: 0 0.4rem; font-size: 1rem; opacity: 0.7;
}}
.stock-banner .close-x:hover {{ opacity: 1; }}

/* Time range pills */
.time-range-pills {{ display: flex; gap: 0.3rem; }}
.range-pill {{
    padding: 0.2rem 0.6rem; border-radius: 999px;
    font-size: 0.68rem; font-weight: 600;
    background: var(--surface2); color: var(--text-muted);
    border: 1px solid var(--border); cursor: pointer;
}}
.range-pill:hover {{ border-color: var(--accent); color: var(--text); }}
.range-pill.active {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
#custom-range-fields input[type="date"] {{
    background: var(--surface); color: var(--text);
    border: 1px solid var(--border); border-radius: 4px;
    padding: 0.2rem 0.3rem; font-size: 0.8rem;
    margin: 0 0.2rem;
}}

/* Clickable stock rows */
.holding-row {{ cursor: pointer; transition: background 0.1s; }}
.holding-row:hover {{ background: var(--surface2) !important; }}
.holding-row.active-stock {{ background: var(--surface2); border-left: 3px solid var(--accent); }}

/* Mode toggle — hide/show USD vs percentage elements */
body.pct-mode .usd-only {{ display: none !important; }}
body.pct-mode .pct-only {{ display: table-cell !important; }}
body.pct-mode .pct-only.stat-card {{ display: block !important; }}
body.pct-mode .pct-only.donut-section {{ display: block !important; }}

/* ── Toast notifications (shared with monitor) ── */
#toast-container {{
    position: fixed; top: 1rem; right: 1rem; z-index: 9999;
    display: flex; flex-direction: column; gap: 0.5rem;
    pointer-events: none; max-width: min(380px, calc(100vw - 2rem));
}}
.toast {{
    pointer-events: auto; background: var(--surface);
    border: 1px solid var(--border); border-left: 3px solid var(--text-muted);
    border-radius: 8px; padding: 0.75rem 1rem; font-size: 0.85rem;
    color: var(--text); box-shadow: 0 8px 24px rgba(0,0,0,0.35);
    display: flex; align-items: flex-start; gap: 0.6rem;
    animation: toast-in 0.22s ease-out;
}}
.toast.toast-success {{ border-left-color: var(--green); }}
.toast.toast-info    {{ border-left-color: var(--accent); }}
.toast.toast-warning {{ border-left-color: #d6a136; }}
.toast.toast-error   {{ border-left-color: var(--red); }}
.toast.toast-out {{ animation: toast-out 0.18s ease-in forwards; }}
.toast-icon {{ flex: 0 0 auto; font-size: 1rem; line-height: 1.2; }}
.toast-body {{ flex: 1 1 auto; min-width: 0; word-wrap: break-word; }}
.toast-close {{
    flex: 0 0 auto; cursor: pointer; color: var(--text-muted);
    font-size: 1rem; line-height: 1; padding: 0 0.2rem;
}}
.toast-close:hover {{ color: var(--text); }}
@keyframes toast-in {{
    from {{ transform: translateX(120%); opacity: 0; }}
    to   {{ transform: translateX(0);    opacity: 1; }}
}}
@keyframes toast-out {{
    from {{ transform: translateX(0);    opacity: 1; }}
    to   {{ transform: translateX(120%); opacity: 0; }}
}}
.confirm-overlay {{
    position: fixed; inset: 0; background: rgba(0,0,0,0.6);
    z-index: 600; display: flex; align-items: center;
    justify-content: center; backdrop-filter: blur(4px);
}}
.confirm-dialog {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 1.4rem 1.5rem;
    width: min(440px, 92vw); box-shadow: 0 20px 60px rgba(0,0,0,0.5);
}}
.confirm-title {{ font-size: 1rem; font-weight: 700; color: var(--text); margin-bottom: 0.6rem; }}
.confirm-message {{ font-size: 0.85rem; color: var(--text-muted); line-height: 1.5; margin-bottom: 1.2rem; }}
.confirm-actions {{ display: flex; justify-content: flex-end; gap: 0.6rem; }}
.confirm-btn {{
    padding: 0.5rem 1rem; border-radius: 8px; cursor: pointer;
    font-size: 0.82rem; font-weight: 600;
    border: 1px solid var(--border); background: var(--surface2);
    color: var(--text); transition: all 0.15s;
}}
.confirm-btn:hover {{ border-color: var(--accent); }}
.confirm-btn.confirm-btn-danger {{
    background: rgba(220,70,70,0.14); color: #ff7b7b;
    border-color: rgba(220,70,70,0.40);
}}
.confirm-btn.confirm-btn-danger:hover {{
    background: rgba(220,70,70,0.22); border-color: #ff7b7b;
}}
</style>
</head>
<body>

<div class="header">
    <h1><span>Emerging Edge</span> Portfolio</h1>
    <div style="display:flex;gap:0.5rem;align-items:center">
        <button type="button" class="theme-toggle" id="theme-toggle"
                onclick="toggleTheme()" title="Toggle light / dark mode">🌙</button>
        <label class="nav-link" style="cursor:pointer">
            Update CSV <input type="file" id="csv-upload" accept=".csv" style="display:none" onchange="uploadCSV(this)">
        </label>
        <button id="mode-toggle" class="nav-link" onclick="toggleMode()" style="cursor:pointer">Show %</button>
        <a href="/monitor" class="nav-link">📊 Monitor</a>
        {_logout_link}
    </div>
</div>

<div class="container">

{empty_msg}
{stats_html}
{performers_html}
{donut_html}
{chart_html}
{holdings_html}
{txns_html}

</div>

<script>
{chart_js}

// ── In-site toast + confirm helpers ──
function showToast(message, type) {{
    type = type || 'info';
    let container = document.getElementById('toast-container');
    if (!container) {{
        container = document.createElement('div');
        container.id = 'toast-container';
        document.body.appendChild(container);
    }}
    const icons = {{ success: '✓', info: 'ℹ', warning: '⚠', error: '✕' }};
    const toast = document.createElement('div');
    toast.className = 'toast toast-' + type;
    toast.innerHTML = '<span class="toast-icon">' + (icons[type] || 'ℹ') + '</span>' +
                      '<span class="toast-body"></span>' +
                      '<span class="toast-close">×</span>';
    toast.querySelector('.toast-body').textContent = message;
    const dismiss = () => {{
        toast.classList.add('toast-out');
        setTimeout(() => toast.remove(), 200);
    }};
    toast.querySelector('.toast-close').addEventListener('click', dismiss);
    container.appendChild(toast);
    setTimeout(dismiss, 4500);
}}

// Re-show any warning stashed just before a reload (e.g. a reinvest
// that didn't have enough cash). The add/edit handlers reload the page
// immediately, which would wipe an inline toast — so we persist it and
// pop it back up here once the page is ready, visible for the full
// toast duration.
(function _showPendingPortfolioWarning() {{
    function _go() {{
        try {{
            const w = sessionStorage.getItem('pf-pending-warning');
            if (w) {{
                sessionStorage.removeItem('pf-pending-warning');
                showToast(w, 'warning');
            }}
        }} catch (_e) {{}}
    }}
    if (document.readyState === 'loading') {{
        document.addEventListener('DOMContentLoaded', _go);
    }} else {{
        _go();
    }}
}})();

function showConfirm(title, message, opts) {{
    opts = opts || {{}};
    return new Promise(resolve => {{
        const overlay = document.createElement('div');
        overlay.className = 'confirm-overlay';
        overlay.innerHTML =
            '<div class="confirm-dialog">' +
                '<div class="confirm-title"></div>' +
                '<div class="confirm-message"></div>' +
                '<div class="confirm-actions">' +
                    '<button class="confirm-btn" data-role="cancel"></button>' +
                    '<button class="confirm-btn confirm-btn-danger" data-role="ok"></button>' +
                '</div>' +
            '</div>';
        overlay.querySelector('.confirm-title').textContent = title;
        overlay.querySelector('.confirm-message').textContent = message;
        overlay.querySelector('[data-role="cancel"]').textContent = opts.cancelLabel || 'Cancel';
        overlay.querySelector('[data-role="ok"]').textContent = opts.okLabel || 'Confirm';
        const close = (ok) => {{ overlay.remove(); resolve(ok); }};
        overlay.querySelector('[data-role="cancel"]').addEventListener('click', () => close(false));
        overlay.querySelector('[data-role="ok"]').addEventListener('click', () => close(true));
        overlay.addEventListener('click', (e) => {{ if (e.target === overlay) close(false); }});
        document.body.appendChild(overlay);
        overlay.querySelector('[data-role="ok"]').focus();
    }});
}}

// ── Stock search autocomplete for the Add Transaction form ──
let _txnStockSearchTimer = null;
function onTxnStockSearch(query) {{
    if (_txnStockSearchTimer) clearTimeout(_txnStockSearchTimer);
    const container = document.getElementById('txn-stock-results');
    if (!query || query.trim().length < 2) {{
        container.innerHTML = '';
        container.style.display = 'none';
        return;
    }}
    _txnStockSearchTimer = setTimeout(() => {{
        fetch('/api/stock-search?q=' + encodeURIComponent(query))
            .then(r => r.json())
            .then(data => renderTxnStockResults(data.results || []))
            .catch(err => {{
                container.innerHTML = '<div style="padding:0.5rem;color:var(--text-muted)">Search failed</div>';
                container.style.display = 'block';
            }});
    }}, 300);
}}

function renderTxnStockResults(results) {{
    const container = document.getElementById('txn-stock-results');
    if (!results.length) {{
        container.innerHTML = '<div style="padding:0.5rem;color:var(--text-muted);font-size:0.75rem">No matches. Try a longer or more specific search.</div>';
        container.style.display = 'block';
        return;
    }}
    let html = '';
    for (const r of results) {{
        const data = JSON.stringify(r).replace(/"/g, '&quot;');
        html += `<div class="txn-autocomplete-item" data-stock="${{data}}" onclick="selectTxnStock(this)">
            <strong>${{escTxnHtml(r.name)}}</strong>
            <span style="color:var(--text-muted);font-size:0.72rem"> · ${{escTxnHtml(r.ticker)}} · ${{escTxnHtml(r.exchDisp || r.exchange)}} · ${{escTxnHtml(r.currency)}}</span>
        </div>`;
    }}
    container.innerHTML = html;
    container.style.display = 'block';
}}

function escTxnHtml(s) {{
    const div = document.createElement('div');
    div.textContent = s || '';
    return div.innerHTML;
}}

function selectTxnStock(el) {{
    try {{
        const data = JSON.parse(el.dataset.stock.replace(/&quot;/g, '"'));
        document.getElementById('txn-selected-ticker').value = data.ticker || '';
        document.getElementById('txn-selected-exchange').value = data.exchange || '';
        document.getElementById('txn-selected-currency').value = data.currency || '';
        document.getElementById('txn-stock-search').value = (data.name || data.ticker) + ' (' + data.ticker + ' · ' + data.exchange + ')';
        document.getElementById('txn-stock-results').style.display = 'none';
        // Persist to user_stocks so it appears in the monitor
        fetch('/api/watchlist/add', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify(data),
        }}).catch(() => {{}});
        // Move focus to Shares input
        const shares = document.getElementById('txn-shares');
        if (shares) shares.focus();
    }} catch (e) {{
        showToast('Failed to parse selection: ' + e, 'error');
    }}
}}

// Hide dropdown when clicking outside
document.addEventListener('click', (e) => {{
    const search = document.getElementById('txn-stock-search');
    const results = document.getElementById('txn-stock-results');
    if (!search || !results) return;
    if (e.target !== search && !results.contains(e.target)) {{
        results.style.display = 'none';
    }}
}});

function toggleConvertFields() {{
    const type = document.getElementById('txn-type').value;
    const isConvert = (type === 'CONVERT');
    // Security-related fields (stock, shares, price) are hidden for CONVERT.
    document.querySelectorAll('.txn-security-field').forEach(
        el => {{ el.style.display = isConvert ? 'none' : 'flex'; }}
    );
    const stockSearch = document.getElementById('txn-stock-search');
    const stockField = stockSearch ? stockSearch.closest('.field') : null;
    if (stockField) stockField.style.display = isConvert ? 'none' : 'flex';
    // Manual-entry fields stay hidden unless explicitly opened
    // Convert-specific fields visible only for CONVERT. The chips block
    // uses flex-direction:column so we override the generic 'flex' style.
    document.querySelectorAll('.txn-convert-field').forEach(
        el => {{
            if (!isConvert) {{ el.style.display = 'none'; return; }}
            el.style.display = el.classList.contains('txn-convert-balances') ? 'flex' : 'flex';
        }}
    );
    if (isConvert) {{
        renderCashChips();
        onFromCurrencyChange();
    }}
}}

// Cash balances passed from Python — used to render quick-fill chips
// and the "Available: X" hint under the From input.
const _PORTFOLIO_CASH = {cash_balances_json};
// FX rates (CUR per 1 USD, latest known) for auto-filling the To-amount
// using the implied cross rate: to = (from / rate_from) * rate_to.
const _PORTFOLIO_FX = {fx_rates_json};
// Tracks whether the user has manually overridden the To-amount; once
// they do we stop auto-recomputing it so we don't clobber their edit.
let _toAmountManuallySet = false;

function _fmtAmt(n) {{
    if (n === null || n === undefined) return '';
    const abs = Math.abs(n);
    if (abs >= 10000) return Math.round(n).toLocaleString();
    if (abs >= 100) return n.toFixed(0);
    if (abs >= 1) return n.toFixed(2);
    return n.toFixed(4);
}}

function renderCashChips() {{
    const container = document.getElementById('txn-cash-chips');
    if (!container) return;
    if (!_PORTFOLIO_CASH || _PORTFOLIO_CASH.length === 0) {{
        container.innerHTML = '<span class="muted" style="font-size:0.75rem">No cash balances on record</span>';
        return;
    }}
    container.innerHTML = _PORTFOLIO_CASH.map(b => {{
        const usd = b.usd_equiv !== null ? '<span class="muted" style="margin-left:0.3rem">≈$' + _fmtAmt(b.usd_equiv) + '</span>' : '';
        return '<button type="button" class="txn-cash-chip" '
            + 'data-cur="' + b.currency + '" '
            + 'data-bal="' + b.balance + '" '
            + 'onclick="useCashChip(\\'' + b.currency + '\\', ' + b.balance + ')">'
            + '<strong>' + b.currency + '</strong> ' + _fmtAmt(b.balance) + usd
            + '</button>';
    }}).join('');
}}

function useCashChip(currency, balance) {{
    const fromSel = document.getElementById('txn-from-currency');
    const fromAmt = document.getElementById('txn-from-amount');
    if (!fromSel || !fromAmt) return;
    let found = false;
    Array.from(fromSel.options).forEach(o => {{ if (o.value === currency) found = true; }});
    if (!found) {{
        const opt = document.createElement('option');
        opt.value = currency; opt.textContent = currency;
        fromSel.appendChild(opt);
    }}
    fromSel.value = currency;
    fromAmt.value = balance;
    _toAmountManuallySet = false;
    onFromCurrencyChange();
    recomputeToAmount();
    fromAmt.focus();
}}

function setFromMax() {{
    const fromSel = document.getElementById('txn-from-currency');
    const fromAmt = document.getElementById('txn-from-amount');
    if (!fromSel || !fromAmt) return;
    const cur = fromSel.value;
    const entry = (_PORTFOLIO_CASH || []).find(b => b.currency === cur);
    if (!entry) {{
        showToast('No cash balance recorded in ' + cur, 'warning');
        return;
    }}
    fromAmt.value = entry.balance;
    _toAmountManuallySet = false;
    onFromCurrencyChange();
    recomputeToAmount();
}}

function onFromCurrencyChange() {{
    const fromSel = document.getElementById('txn-from-currency');
    const hint = document.getElementById('txn-from-balance');
    if (!fromSel || !hint) return;
    const cur = fromSel.value;
    const entry = (_PORTFOLIO_CASH || []).find(b => b.currency === cur);
    if (!entry) {{
        hint.textContent = 'No balance on record in ' + cur + ' — convert will create new cash';
        hint.classList.add('warn');
    }} else {{
        hint.classList.remove('warn');
        const usd = entry.usd_equiv !== null ? ' (≈$' + _fmtAmt(entry.usd_equiv) + ')' : '';
        hint.textContent = 'Available: ' + _fmtAmt(entry.balance) + ' ' + cur + usd;
    }}
    recomputeToAmount();
}}

function onToCurrencyChange() {{
    _toAmountManuallySet = false;
    recomputeToAmount();
}}

function onToAmountManualEdit() {{
    _toAmountManuallySet = true;
    const hint = document.getElementById('txn-to-rate-hint');
    if (hint) hint.textContent = '';
}}

function recomputeToAmount() {{
    const fromSel = document.getElementById('txn-from-currency');
    const fromAmt = document.getElementById('txn-from-amount');
    const toSel = document.getElementById('txn-to-currency');
    const toAmt = document.getElementById('txn-to-amount');
    const hint = document.getElementById('txn-to-rate-hint');
    if (!fromSel || !fromAmt || !toSel || !toAmt) return;
    if (_toAmountManuallySet) return;
    const fromCur = fromSel.value;
    const toCur = toSel.value;
    const amt = parseFloat(fromAmt.value);
    if (!fromCur || !toCur || !isFinite(amt) || amt === 0) {{
        if (hint) hint.textContent = '';
        return;
    }}
    if (fromCur === toCur) {{
        if (hint) hint.textContent = '';
        return;
    }}
    const rateFrom = _PORTFOLIO_FX[fromCur];
    const rateTo = _PORTFOLIO_FX[toCur];
    if (!rateFrom || !rateTo) {{
        if (hint) {{
            hint.classList.add('warn');
            hint.textContent = 'No FX rate stored for '
                + (!rateFrom ? fromCur : toCur)
                + ' — enter the To-amount manually';
        }}
        return;
    }}
    const computed = (amt / rateFrom) * rateTo;
    let displayed;
    if (computed >= 10000) displayed = computed.toFixed(0);
    else if (computed >= 100) displayed = computed.toFixed(1);
    else if (computed >= 1) displayed = computed.toFixed(2);
    else displayed = computed.toFixed(4);
    _toAmountManuallySet = false;
    toAmt.value = displayed;
    if (hint) {{
        hint.classList.remove('warn');
        const unitRate = rateTo / rateFrom;
        let rateStr;
        if (unitRate >= 1000) rateStr = unitRate.toFixed(0);
        else if (unitRate >= 1) rateStr = unitRate.toFixed(4);
        else rateStr = unitRate.toExponential(3);
        hint.textContent = 'Auto-filled at 1 ' + fromCur + ' = '
            + rateStr + ' ' + toCur + ' — edit to override';
    }}
}}

function addTransaction() {{
    const date = document.getElementById('txn-date').value;
    const txnType = document.getElementById('txn-type').value;

    if (!date) {{ showToast('Please choose a date', 'warning'); return; }}

    let payload;
    if (txnType === 'CONVERT') {{
        const fromCur = document.getElementById('txn-from-currency').value.trim().toUpperCase();
        const fromAmt = document.getElementById('txn-from-amount').value;
        const toCur = document.getElementById('txn-to-currency').value.trim().toUpperCase();
        const toAmt = document.getElementById('txn-to-amount').value;
        if (!fromCur || !toCur || fromAmt === '' || toAmt === '') {{
            showToast('Please fill in from/to currency and both amounts', 'warning');
            return;
        }}
        if (fromCur === toCur) {{
            showToast('From and to currencies must differ', 'warning');
            return;
        }}
        payload = {{
            date: date, type: 'CONVERT',
            currency: fromCur, shares: parseFloat(fromAmt),
            to_currency: toCur, to_amount: parseFloat(toAmt)
        }};
    }} else {{
        // Hidden fields are set by the autocomplete selection
        const ticker = document.getElementById('txn-selected-ticker').value.trim().toUpperCase();
        const exchange = document.getElementById('txn-selected-exchange').value.trim().toUpperCase();
        const currency = document.getElementById('txn-selected-currency').value.trim().toUpperCase();
        if (!ticker || !exchange || !currency) {{
            showToast('Please pick a stock from the search dropdown first', 'warning');
            return;
        }}
        const shares = document.getElementById('txn-shares').value;
        const price = document.getElementById('txn-price').value;
        if (shares === '' || price === '') {{
            showToast('Please fill in shares and price', 'warning');
            return;
        }}
        payload = {{
            date: date, ticker: ticker, exchange: exchange,
            type: txnType, shares: parseFloat(shares),
            price: parseFloat(price), currency: currency
        }};
    }}

    fetch('/api/portfolio/add', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
    }})
    .then(r => r.json())
    .then(data => {{
        if (data.status === 'ok') {{
            // Stash any warning so it survives the reload and is shown
            // AFTER the page comes back — the immediate location.reload()
            // would otherwise wipe the toast before it can be read
            // (e.g. reinvest cash-shortfall notice).
            if (data.warning) {{
                try {{ sessionStorage.setItem('pf-pending-warning', data.warning); }}
                catch (_e) {{}}
            }}
            location.reload();
        }} else {{
            showToast(data.message || 'Error', 'error');
        }}
    }})
    .catch(err => showToast('Network error: ' + err, 'error'));
}}

function editTxn(id) {{
    const row = document.getElementById('txn-row-' + id);
    if (!row) return;
    // Stash original HTML so cancel can restore it
    if (!row.dataset.originalHtml) {{
        row.dataset.originalHtml = row.innerHTML;
    }}
    const d = row.dataset;
    row.classList.add('edit-row');

    if (d.type === 'CONVERT') {{
        row.innerHTML = `
            <td><input type="date" class="edit-date" value="${{d.date}}"></td>
            <td><span class="muted">CASH</span></td>
            <td>CONVERT</td>
            <td>
                <input type="text" class="edit-currency" value="${{d.currency}}" placeholder="From" style="width:3.5rem">
                <input type="number" step="any" class="edit-shares" value="${{d.shares}}" placeholder="From amt" style="width:5rem">
                →
                <input type="text" class="edit-to-currency" value="${{d.toCurrency}}" placeholder="To" style="width:3.5rem">
                <input type="number" step="any" class="edit-to-amount" value="${{d.toAmount}}" placeholder="To amt" style="width:5rem">
            </td>
            <td style="white-space:nowrap">
                <span class="edit-btn" onclick="saveTxnEdit(${{id}})" title="Save">✓</span>
                <span class="del-btn" onclick="cancelTxnEdit(${{id}})" title="Cancel">✕</span>
            </td>`;
        return;
    }}

    const types = ['BUY','SELL','DIVIDEND','REINVEST','CONVERT'];
    const typeOpts = types.map(t => '<option value="'+t+'"'+(t===d.type?' selected':'')+'>'+t+'</option>').join('');
    row.innerHTML = `
        <td><input type="date" class="edit-date" value="${{d.date}}"></td>
        <td>
            <input type="text" class="edit-ticker" value="${{d.ticker}}" style="width:5rem" placeholder="Ticker">
            <input type="text" class="edit-exchange" value="${{d.exchange}}" style="width:5rem;margin-top:0.2rem" placeholder="Exch">
        </td>
        <td><select class="edit-type">${{typeOpts}}</select></td>
        <td>
            <input type="number" step="any" class="edit-shares" value="${{d.shares}}" placeholder="Shares">
            @
            <input type="number" step="any" class="edit-price" value="${{d.price}}" placeholder="Price">
            <input type="text" class="edit-currency" value="${{d.currency}}" placeholder="Cur">
        </td>
        <td style="white-space:nowrap">
            <span class="edit-btn" onclick="saveTxnEdit(${{id}})" title="Save">✓</span>
            <span class="del-btn" onclick="cancelTxnEdit(${{id}})" title="Cancel">✕</span>
        </td>`;
}}

function cancelTxnEdit(id) {{
    const row = document.getElementById('txn-row-' + id);
    if (!row || !row.dataset.originalHtml) return;
    row.innerHTML = row.dataset.originalHtml;
    row.classList.remove('edit-row');
    delete row.dataset.originalHtml;
}}

function saveTxnEdit(id) {{
    const row = document.getElementById('txn-row-' + id);
    if (!row) return;
    const isConvert = (row.dataset.type === 'CONVERT') ||
                      !!row.querySelector('.edit-to-currency');
    let payload;
    if (isConvert) {{
        payload = {{
            id: id,
            date: row.querySelector('.edit-date').value,
            type: 'CONVERT',
            currency: row.querySelector('.edit-currency').value.trim().toUpperCase(),
            shares: parseFloat(row.querySelector('.edit-shares').value),
            to_currency: row.querySelector('.edit-to-currency').value.trim().toUpperCase(),
            to_amount: parseFloat(row.querySelector('.edit-to-amount').value),
        }};
        if (!payload.date || !payload.currency || !payload.to_currency ||
            isNaN(payload.shares) || isNaN(payload.to_amount)) {{
            showToast('Please fill in all fields', 'warning');
            return;
        }}
        if (payload.currency === payload.to_currency) {{
            showToast('From and to currencies must differ', 'warning');
            return;
        }}
    }} else {{
        payload = {{
            id: id,
            date: row.querySelector('.edit-date').value,
            ticker: row.querySelector('.edit-ticker').value,
            exchange: row.querySelector('.edit-exchange').value,
            type: row.querySelector('.edit-type').value,
            shares: parseFloat(row.querySelector('.edit-shares').value),
            price: parseFloat(row.querySelector('.edit-price').value),
            currency: row.querySelector('.edit-currency').value,
        }};
        if (!payload.date || !payload.ticker || !payload.exchange ||
            isNaN(payload.shares) || isNaN(payload.price) || !payload.currency) {{
            showToast('Please fill in all fields', 'warning');
            return;
        }}
    }}
    fetch('/api/portfolio/update', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
    }})
    .then(r => r.json())
    .then(data => {{
        if (data.status === 'ok') {{
            // Stash any warning so it survives the reload and is shown
            // AFTER the page comes back — the immediate location.reload()
            // would otherwise wipe the toast before it can be read
            // (e.g. reinvest cash-shortfall notice).
            if (data.warning) {{
                try {{ sessionStorage.setItem('pf-pending-warning', data.warning); }}
                catch (_e) {{}}
            }}
            location.reload();
        }} else {{
            showToast(data.message || 'Error', 'error');
        }}
    }})
    .catch(err => showToast('Network error: ' + err, 'error'));
}}

function colorStatusSelect(sel) {{
    const colors = {{ 'NEW': '#34d399', 'ADD': '#6c8cff', 'REDUCED': '#f97316', 'SOLD': '#ff6b6b' }};
    sel.style.color = colors[sel.value] || 'var(--text-muted)';
    sel.style.borderColor = colors[sel.value] || 'var(--border)';
}}
function setHoldingLabel(sel) {{
    const ticker = sel.dataset.ticker;
    const label = sel.value;
    colorStatusSelect(sel);
    fetch('/api/portfolio/label', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ ticker: ticker, label: label }})
    }}).catch(err => console.error('Failed to save label:', err));
}}
// Color all status selects on load
document.querySelectorAll('.status-select').forEach(colorStatusSelect);

// ── Hide / show fully-exited (sold-out) positions ──
function hideSoldHolding(ticker) {{
    fetch('/api/portfolio/hide-sold-out', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ ticker: ticker, action: 'hide' }})
    }}).then(r => r.json()).then(d => {{
        if (d.status === 'ok') {{
            showToast(ticker + ' removed from holdings', 'success');
            setTimeout(() => location.reload(), 350);
        }} else {{
            showToast(d.message || 'Failed to hide', 'error');
        }}
    }}).catch(err => showToast('Network error: ' + err, 'error'));
}}

function showAllSoldHoldings() {{
    fetch('/api/portfolio/hide-sold-out', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ action: 'clear' }})
    }}).then(r => r.json()).then(d => {{
        if (d.status === 'ok') {{
            showToast('Sold-out positions restored', 'success');
            setTimeout(() => location.reload(), 350);
        }}
    }}).catch(err => showToast('Network error: ' + err, 'error'));
}}

// ── Holdings: Copy as Markdown ──
function copyHoldingsAsMarkdown(btn) {{
    const wrap = document.getElementById('holdings-table-wrap');
    if (!wrap) return;
    const table = wrap.querySelector('table');
    if (!table) return;
    function visibleCells(rowEl) {{
        return [...rowEl.children].filter(td => {{
            const cs = window.getComputedStyle(td);
            return cs.display !== 'none' && cs.visibility !== 'hidden';
        }});
    }}
    function cellText(td) {{
        const clone = td.cloneNode(true);
        clone.querySelectorAll('.hide-toggle, .stock-name-hidden').forEach(n => n.remove());
        clone.querySelectorAll('select').forEach(sel => {{
            const opt = sel.options[sel.selectedIndex];
            const txt = opt && opt.value ? opt.value : '';
            sel.replaceWith(document.createTextNode(txt));
        }});
        clone.innerHTML = clone.innerHTML.replace(/<br\\s*\\/?>/gi, ' · ');
        return (clone.textContent || '').replace(/\\s+/g, ' ').trim();
    }}
    const headerCells = visibleCells(table.tHead.rows[0]);
    const headers = headerCells.map(cellText);
    const rows = [];
    [...table.tBodies[0].rows].forEach(tr => {{
        const cs = window.getComputedStyle(tr);
        if (cs.display === 'none') return;
        const cells = visibleCells(tr);
        if (cells.length === 0) return;
        rows.push(cells.map(cellText));
    }});
    if (!headers.length || !rows.length) {{
        showToast('Nothing to copy — the holdings table is empty', 'warning');
        return;
    }}
    const md = (
        '| ' + headers.join(' | ') + ' |\\n'
        + '|' + headers.map(() => '---').join('|') + '|\\n'
        + rows.map(r => '| ' + r.join(' | ') + ' |').join('\\n')
        + '\\n'
    );
    const today = new Date().toISOString().slice(0, 10);
    const full = '## Portfolio holdings — ' + today + '\\n\\n' + md;
    navigator.clipboard.writeText(full).then(() => {{
        showToast('Holdings copied as Markdown', 'success');
    }}).catch(err => {{
        showToast('Copy failed: ' + err, 'error');
    }});
}}

// ── Holdings: Save as PNG (full-height screenshot) ──
let _h2cLoading = null;
function _loadHtml2Canvas() {{
    if (window.html2canvas) return Promise.resolve(window.html2canvas);
    if (_h2cLoading) return _h2cLoading;
    _h2cLoading = new Promise((resolve, reject) => {{
        const s = document.createElement('script');
        s.src = 'https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js';
        s.onload = () => resolve(window.html2canvas);
        s.onerror = () => reject(new Error('Could not load html2canvas from CDN'));
        document.head.appendChild(s);
    }});
    return _h2cLoading;
}}

function saveHoldingsAsImage(btn) {{
    const wrap = document.getElementById('holdings-table-wrap');
    if (!wrap) {{ showToast('Holdings table not found', 'error'); return; }}
    btn.classList.add('busy');
    const origLabel = btn.textContent;
    btn.textContent = '⏳ Rendering…';
    _loadHtml2Canvas().then(h2c => {{
        const bg = window.getComputedStyle(document.body).backgroundColor || '#0f1117';
        return h2c(wrap, {{
            backgroundColor: bg, scale: 2, logging: false,
            windowHeight: Math.max(document.body.scrollHeight,
                                    wrap.scrollHeight + 200),
            height: wrap.scrollHeight,
            width: wrap.scrollWidth,
        }});
    }}).then(canvas => {{
        const a = document.getElementById('holdings-image-download');
        const today = new Date().toISOString().slice(0, 10);
        a.href = canvas.toDataURL('image/png');
        a.download = 'holdings-' + today + '.png';
        a.click();
        showToast('Saved holdings-' + today + '.png', 'success');
    }}).catch(err => {{
        showToast('Image render failed: ' + (err && err.message || err), 'error');
    }}).finally(() => {{
        btn.classList.remove('busy');
        btn.textContent = origLabel;
    }});
}}

function deleteTxn(id) {{
    showConfirm('Delete transaction?',
                'This will permanently delete the transaction. This cannot be undone.',
                {{ okLabel: 'Delete', cancelLabel: 'Cancel' }}).then(ok => {{
        if (!ok) return;
        fetch('/api/portfolio/delete', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{ id: id }})
        }})
        .then(r => r.json())
        .then(data => {{
            if (data.status === 'ok') {{
                showToast('Transaction deleted', 'success');
                setTimeout(() => location.reload(), 500);
            }} else {{
                showToast(data.message || 'Error', 'error');
            }}
        }})
        .catch(err => showToast('Network error: ' + err, 'error'));
    }});
}}

// ── Logo manager ──
function openLogoManager() {{
    const m = document.getElementById('logo-mgr-modal');
    if (m) m.style.display = 'flex';
}}
function closeLogoManager() {{
    const m = document.getElementById('logo-mgr-modal');
    if (m) m.style.display = 'none';
}}

function uploadLogo(input, ticker) {{
    const file = input.files[0];
    if (!file) return;
    if (file.size > 2 * 1024 * 1024) {{
        showToast('File too large (max 2 MB)', 'warning');
        input.value = '';
        return;
    }}
    const reader = new FileReader();
    reader.onload = function(e) {{
        // e.target.result is a data URL like "data:image/png;base64,iVBOR..."
        const dataUrl = e.target.result;
        const commaIdx = dataUrl.indexOf(',');
        const contentB64 = commaIdx >= 0 ? dataUrl.substring(commaIdx + 1) : '';
        fetch('/api/logo/upload', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{
                ticker: ticker,
                filename: file.name,
                content_base64: contentB64,
            }})
        }})
        .then(r => r.json())
        .then(data => {{
            if (data.status === 'ok') {{
                // Replace the thumbnail in-place and flash a success state
                const row = input.closest('.logo-mgr-row');
                if (row) {{
                    const oldThumb = row.querySelector('.logo-mgr-thumb');
                    const img = document.createElement('img');
                    img.src = data.path + '?v=' + Date.now();
                    img.className = 'logo-mgr-thumb';
                    img.alt = ticker;
                    if (oldThumb) oldThumb.replaceWith(img);
                }}
                // Reload the page after a short delay so the donut picks up the new logo
                setTimeout(() => location.reload(), 400);
            }} else {{
                showToast('Upload failed: ' + (data.message || 'unknown error'), 'error');
            }}
        }})
        .catch(err => showToast('Upload failed: ' + err, 'error'));
    }};
    reader.readAsDataURL(file);
    input.value = '';  // allow re-uploading the same filename
}}

function uploadCSV(input) {{
    const file = input.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = function(e) {{
        fetch('/api/portfolio/import', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'text/csv' }},
            body: e.target.result
        }})
        .then(r => r.json())
        .then(data => {{
            if (data.status === 'ok') {{
                showToast('Imported ' + data.imported + ' transactions. Reloading...', 'success');
                location.reload();
            }} else {{
                showToast(data.message || 'Error', 'error');
            }}
        }})
        .catch(err => showToast('Upload failed: ' + err, 'error'));
    }};
    reader.readAsText(file);
}}
</script>

</body>
</html>"""


def save_portfolio_html(db: Database, config: dict) -> str:
    """Generate and write the portfolio HTML. Returns the file path."""
    digest_dir = config.get("digest_dir", "./digests")
    os.makedirs(digest_dir, exist_ok=True)

    content = generate_portfolio_html(db, config)
    filepath = os.path.join(digest_dir, "portfolio.html")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath
