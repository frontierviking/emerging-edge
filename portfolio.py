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
    stock_map = {s["ticker"]: s for s in get_active_stocks(db, config)}

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
                "currency": cur, "exchange": t["exchange"]
            }
        pos = positions[tk]
        cash.setdefault(cur, 0.0)

        if t["txn_type"] == "BUY":
            # Always fresh external capital. Never touches cash.
            cost = t["shares"] * t["price"]
            pos["total_cost"] += cost
            pos["shares"] += t["shares"]
            deposits.append({"date": t["txn_date"], "currency": cur, "amount": cost})
        elif t["txn_type"] == "REINVEST":
            # Cash-funded buy. Debits cash; shortfall falls through to a
            # new external deposit (guard against over-reinvestment).
            cost = t["shares"] * t["price"]
            pos["shares"] += t["shares"]
            pos["total_cost"] += cost
            have = cash.get(cur, 0.0)
            if have >= cost:
                cash[cur] = have - cost
            else:
                shortfall = cost - have
                cash[cur] = 0.0
                deposits.append({"date": t["txn_date"], "currency": cur, "amount": shortfall})
        elif t["txn_type"] == "SELL":
            if pos["shares"] > 0:
                avg = pos["total_cost"] / pos["shares"] if pos["shares"] else 0
                pos["shares"] -= t["shares"]
                pos["total_cost"] = avg * pos["shares"]
            cash[cur] = cash.get(cur, 0.0) + t["shares"] * t["price"]
        elif t["txn_type"] == "DIVIDEND":
            # shares = shares held, price = dividend per share
            amount = t["shares"] * t["price"]
            pos["dividends"] += amount
            cash[cur] = cash.get(cur, 0.0) + amount

    # Build holdings with current prices
    holdings = []
    for tk, pos in positions.items():
        if pos["shares"] <= 0:
            continue

        avg_cost = pos["total_cost"] / pos["shares"] if pos["shares"] else 0
        price_data = db.get_latest_price(tk, pos["exchange"])
        current_price = price_data["price"] if price_data else 0

        market_value = pos["shares"] * current_price
        total_invested = pos["total_cost"]
        dividends = pos["dividends"]

        # Price return (capital gains only)
        price_gain = market_value - total_invested
        price_return_pct = (price_gain / total_invested * 100) if total_invested > 0 else 0

        # Total return (price + dividends)
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
        })

    # Sort by market value descending
    holdings.sort(key=lambda h: h["market_value"], reverse=True)
    return holdings, cash, deposits


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

def fetch_and_store_fx_rates(db: Database, config: dict):
    """Fetch current FX rates from Yahoo and store in fx_snapshots."""
    from fetchers import _fetch_price_yahoo

    # Collect currencies from portfolio transactions
    txns = db.get_all_transactions()
    currencies = {t["currency"] for t in txns}

    today = datetime.utcnow().strftime("%Y-%m-%d")

    _FX_MAP = {
        "MYR": "MYR=X", "NGN": "NGN=X", "UZS": "UZS=X",
        "XOF": "XOF=X", "KGS": "KGS=X", "SGD": "SGD=X",
        "ZAc": "ZAR=X", "ZAC": "ZAR=X", "ZAR": "ZAR=X",
    }

    for curr in currencies:
        if curr == "USD":
            db.insert_fx_rate("USD", 1.0, today)
            continue

        pair = _FX_MAP.get(curr)
        if not pair:
            continue

        r = _fetch_price_yahoo(pair)
        if r:
            rate = r[0]
            # ZAc/ZAC: Yahoo gives ZAR per USD, we need cents per USD
            if curr in ("ZAc", "ZAC"):
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
    stock_map = {s["ticker"]: s for s in get_active_stocks(db, config)}

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
        buy_txns = [t for t in txns if t["ticker"] == ticker and t["txn_type"] == "BUY"]
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
        if total_days <= 1:
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
                actual_rate = rate * 100 if curr in ("ZAc", "ZAC") else rate
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


def generate_portfolio_html(db: Database, config: dict) -> str:
    """Build the portfolio tracking HTML page."""

    # Backfill historical prices + FX rates if needed (first run only)
    backfill_historical_prices(db, config)
    # Ensure today's FX rates are current
    fetch_and_store_fx_rates(db, config)

    holdings, cash, deposits = compute_holdings(db, config)
    holding_labels = db.get_holding_labels()
    history = compute_portfolio_history(db, config)
    txns = db.get_all_transactions()
    from fetchers import get_active_stocks
    stock_map = {s["ticker"]: s for s in get_active_stocks(db, config)}

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

    # Per-stock chart data for click-to-filter
    all_tickers = sorted({t["ticker"] for t in txns})
    per_stock_data = {}  # ticker -> {values: [...], cost: [...], pct: [...]}
    for tk in all_tickers:
        values = []
        costs = []
        for i, d in enumerate(chart_dates):
            # Find matching history entry
            h_match = next((h for h in history if h["date"] == d), None)
            if h_match:
                values.append(h_match.get("stocks", {}).get(tk, 0))
                costs.append(h_match.get("stocks_cost", {}).get(tk, 0))
            else:
                values.append(0)
                costs.append(0)
        pcts = [round(((v - c) / c) * 100, 2) if c > 0 else 0
                for v, c in zip(values, costs)]
        per_stock_data[tk] = {"values": values, "cost": costs, "pct": pcts}

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

        # FX rates: current and at first purchase date
        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        fx_now = db.get_fx_rate(curr, today_str) if curr != "USD" else 1.0
        first_buy = next((t for t in txns if t["ticker"] == h["ticker"] and t["txn_type"] == "BUY"), None)
        buy_date_str = first_buy["txn_date"] if first_buy else today_str
        fx_at_buy = db.get_fx_rate(curr, buy_date_str) if curr != "USD" else 1.0

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

        # USD price return = includes FX impact but excludes dividends
        invested_usd = _to_usd(h["total_invested"], curr, db, buy_date_str)
        usd_return_pct = ((h["usd_value"] - invested_usd) / invested_usd * 100) if invested_usd > 0 else 0
        usd_cls = "gain-pos" if usd_return_pct >= 0 else "gain-neg"

        # USD total return = price return + dividends received (in USD)
        dividends_usd = _to_usd(h["dividends"], curr, db) if h["dividends"] else 0.0
        div_pct_of_basis = (dividends_usd / invested_usd * 100) if invested_usd > 0 else 0
        stock_div_pct[h["ticker"]] = round(div_pct_of_basis, 4)
        usd_total_return_pct = usd_return_pct + div_pct_of_basis
        usd_total_cls = "gain-pos" if usd_total_return_pct >= 0 else "gain-neg"

        holdings_rows.append(f"""
        <tr class="holding-row" data-ticker="{_esc(h['ticker'])}" onclick="filterStock('{_esc(h['ticker'])}')">
            <td style="cursor:pointer">
                <span class="stock-name-full"><strong>{_esc(h['name'])}</strong> <span class="muted">{_esc(h['ticker'])}</span></span>
                <span class="stock-name-hidden" style="display:none"><strong>Undisclosed</strong></span>
                <span class="pct-only hide-toggle" style="display:none" onclick="event.stopPropagation(); toggleUndisclosed(this, '{_esc(h['ticker'])}');" title="Toggle visibility">👁</span>
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
            <td class="usd-only">{_esc(h['currency'])} {_fmt_money(h['dividends'])}</td>
            <td class="{usd_total_cls}" data-return-total="{_esc(h['ticker'])}">{usd_total_return_pct:+.1f}%</td>
            <td class="pct-only" style="display:none"><select class="status-select" data-ticker="{_esc(h['ticker'])}" onchange="setHoldingLabel(this)"><option value="">—</option><option value="NEW"{" selected" if holding_labels.get(h["ticker"]) == "NEW" else ""}>NEW</option><option value="ADD"{" selected" if holding_labels.get(h["ticker"]) == "ADD" else ""}>ADD</option><option value="REDUCED"{" selected" if holding_labels.get(h["ticker"]) == "REDUCED" else ""}>REDUCED</option><option value="SOLD"{" selected" if holding_labels.get(h["ticker"]) == "SOLD" else ""}>SOLD</option></select></td>
        </tr>""")

    # Cash row in the holdings table (USD mode only).
    # Shows the cash balance as a pseudo-holding so the user sees where
    # sell proceeds and dividends went. Not clickable / not filterable.
    if cash_usd > 0:
        cash_entries = [(cur, bal) for cur, bal in sorted(cash.items()) if bal]
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
    for t in reversed(txns):  # most recent first
        type_cls = _txn_cls.get(t["txn_type"], "")
        ticker_display = _esc(t["ticker"])
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
        cash_breakdown = ", ".join(
            f"{cur} {_fmt_money(bal)}" for cur, bal in sorted(cash.items()) if bal
        ) or "no cash"
        cash_title = f"Cash by currency: {cash_breakdown}"
        cash_card = (
            f'<div class="stat-card usd-only" title="{_esc(cash_title)}">'
            f'<div class="label">Cash</div>'
            f'<div class="value" id="stat-cash">${_fmt_money(cash_usd)}</div></div>'
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
            f'<div class="stat-card"><div class="label" id="stat-holdings-label">Holdings</div>'
            f'<div class="value" id="stat-holdings-value">{len(holdings)} stocks</div></div>'
            '</div>'
        )

    performers_html = ""
    if best and worst and len(holdings) > 1:
        performers_html = (
            '<div class="performers">'
            '<div class="performer"><div class="label">Best Performer</div>'
            f'<div class="stock gain-pos" id="best-performer">{_esc(best["name"])} ({_esc(best["ticker"])}) {best["gain_pct"]:+.1f}%</div></div>'
            '<div class="performer"><div class="label">Worst Performer</div>'
            f'<div class="stock gain-neg" id="worst-performer">{_esc(worst["name"])} ({_esc(worst["ticker"])}) {worst["gain_pct"]:+.1f}%</div></div>'
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
        "CASH":     "#555555",   # neutral gray
    }
    _DONUT_FALLBACK = [
        '#6c8cff', '#4ecdc4', '#ff6b6b', '#ffd93d', '#a78bfa',
        '#f97316', '#34d399', '#f472b6', '#60a5fa', '#facc15',
    ]
    donut_html = ""
    if holdings:
        donut_labels = []
        donut_weights = []
        donut_colors = []
        donut_tickers = []
        _fb_idx = 0
        for i, h in enumerate(holdings):
            donut_labels.append(h["name"])
            donut_tickers.append(h["ticker"])
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
            donut_weights.append(round(cash_wt, 2))
            donut_colors.append('#555')

        donut_data_json = json.dumps(donut_weights)
        donut_labels_json = json.dumps(donut_labels)
        donut_colors_json = json.dumps(donut_colors)
        donut_tickers_json = json.dumps(donut_tickers)

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
            '<div class="chart-title" style="margin:0">Portfolio Value (USD)</div>'
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
        holdings_html = (
            '<div class="section-title">Holdings</div><div class="table-wrap"><table>'
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
        )

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
        '<div class="field txn-convert-field" style="display:none">'
        '<label>From</label>'
        '<div style="display:flex;gap:0.3rem">'
        '<select id="txn-from-currency" style="width:55px">'
        '<option value="USD">USD</option><option value="MYR">MYR</option>'
        '<option value="NGN">NGN</option><option value="ZAR">ZAR</option>'
        '<option value="XOF">XOF</option><option value="UZS">UZS</option>'
        '<option value="SGD">SGD</option><option value="KGS">KGS</option>'
        '<option value="KZT">KZT</option><option value="GBP">GBP</option>'
        '<option value="EUR">EUR</option><option value="SEK">SEK</option>'
        '<option value="AUD">AUD</option>'
        '</select>'
        '<input type="number" id="txn-from-amount" step="any" placeholder="amount" style="width:90px">'
        '</div></div>'
        '<div class="field txn-convert-field" style="display:none">'
        '<label>To</label>'
        '<div style="display:flex;gap:0.3rem">'
        '<select id="txn-to-currency" style="width:55px">'
        '<option value="MYR">MYR</option><option value="USD">USD</option>'
        '<option value="NGN">NGN</option><option value="ZAR">ZAR</option>'
        '<option value="XOF">XOF</option><option value="UZS">UZS</option>'
        '<option value="SGD">SGD</option><option value="KGS">KGS</option>'
        '<option value="KZT">KZT</option><option value="GBP">GBP</option>'
        '<option value="EUR">EUR</option><option value="SEK">SEK</option>'
        '<option value="AUD">AUD</option>'
        '</select>'
        '<input type="number" id="txn-to-amount" step="any" placeholder="amount" style="width:90px">'
        '</div></div>'
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
                // Label anchor — pushed further out radially
                const labelDist = oR + 45;
                const lX = cx + Math.cos(mid) * labelDist;
                const lY = cy + Math.sin(mid) * labelDist;
                const isRight = Math.cos(mid) >= 0;
                items.push({ i, mid, eX, eY, lX, lY, isRight, cx, cy, oR,
                             labelY: lY, ticker: donutTickers[i], pct: donutData[i] });
            });

            // Resolve vertical overlaps per side, clamping to canvas bounds
            const spacing = 28;
            const canvasH = chart.canvas.height;
            const minY = 20;
            const maxY = canvasH - 20;
            function resolveOverlaps(group) {
                group.sort((a, b) => a.labelY - b.labelY);
                for (let pass = 0; pass < 30; pass++) {
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
                // Clamp to canvas bounds
                group.forEach(it => {
                    if (it.labelY < minY) it.labelY = minY;
                    if (it.labelY > maxY) it.labelY = maxY;
                });
                // Re-resolve after clamping (push items inward from edges)
                for (let pass = 0; pass < 10; pass++) {
                    let moved = false;
                    for (let j = 1; j < group.length; j++) {
                        const gap = group[j].labelY - group[j-1].labelY;
                        if (gap < spacing) {
                            group[j].labelY = group[j-1].labelY + spacing;
                            if (group[j].labelY > maxY) group[j].labelY = maxY;
                            moved = true;
                        }
                    }
                    if (!moved) break;
                }
            }
            const leftItems = items.filter(it => !it.isRight);
            const rightItems = items.filter(it => it.isRight);
            resolveOverlaps(leftItems);
            resolveOverlaps(rightItems);

            items.forEach(it => {
                const { i, eX, eY, isRight, labelY, ticker, pct, cx, oR, mid } = it;
                const color = donutColors[i];
                // Final label X: keep radial direction but use resolved Y
                const labelDist = oR + 45;
                const finalX = cx + Math.cos(mid) * labelDist;

                ctx.save();
                // Straight line from donut edge to label
                ctx.beginPath();
                ctx.moveTo(eX, eY);
                ctx.lineTo(finalX, labelY);
                ctx.strokeStyle = color;
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
                    ctx.strokeStyle = color;
                    ctx.lineWidth = 1.5;
                    ctx.stroke();
                } else {
                    // Filled circle with initials (or ? for undisclosed)
                    ctx.beginPath();
                    ctx.arc(logoX, labelY, logoR, 0, Math.PI * 2);
                    ctx.fillStyle = hidden ? '#555' : color;
                    ctx.fill();
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
                const displayTicker = hidden ? 'Undisclosed' : ticker;
                ctx.fillText(displayTicker, textX, labelY - 6);
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
                borderColor: 'rgba(26,29,39,0.6)',
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
                    if (document.body.classList.contains('pct-mode'))
                        return c.parsed.y.toFixed(1) + '%';
                    return '$' + c.parsed.y.toLocaleString(undefined, {minimumFractionDigits: 2});
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
    const filteredPct = recalcPct(filtered.values, filtered.cost);
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
        // If a single stock is selected and was bought within period,
        // use simple cost basis return (same as ALL)
        if (activeStock) {
            gain = endVal - endCost;
            pct = endCost > 0 ? (gain / endCost) * 100 : 0;
        } else {
            // Portfolio level: strip out new deposits
            const endGain = endVal - endCost;
            const startGain = startVal - startCost;
            gain = endGain - startGain;
            if (startVal > 0) {
                pct = (gain / startVal) * 100;
            } else {
                // All holdings bought within this period
                gain = endVal - endCost;
                pct = endCost > 0 ? (gain / endCost) * 100 : 0;
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

        // For ALL: return vs cost basis. For time ranges: return vs start value
        // If stock was bought after range start, use cost basis
        let usdPct;
        if (currentRange === 'ALL' || (startDate && stockBuyDate >= startDate)) {
            // Use cost basis — stock was bought within or before this period
            usdPct = startCost > 0 ? ((endVal - startCost) / startCost) * 100 : 0;
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
const totalHoldings = """ + str(len(holdings)) + """;

function updatePerformers() {
    const bestEl = document.getElementById('best-performer');
    const worstEl = document.getElementById('worst-performer');
    if (!bestEl || !worstEl) return;

    const startDate = getStartDate(currentRange);
    let bestTicker = null, bestPct = -Infinity;
    let worstTicker = null, worstPct = Infinity;

    Object.keys(perStockData).forEach(ticker => {
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

        let pct;
        if (currentRange === 'ALL' || (startDate && buyDate >= startDate)) {
            pct = startCost > 0 ? ((endVal - startCost) / startCost) * 100 : 0;
        } else {
            pct = startVal > 0 ? ((endVal - startVal) / startVal) * 100 : 0;
        }

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
<link rel="icon" href="/favicon.png" type="image/png" sizes="32x32">
<link rel="manifest" href="/manifest.json">
<title>Emerging Edge Portfolio</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
:root {{
    --bg: #0f1117; --surface: #1a1d27; --surface2: #232733;
    --border: #2d3040; --text: #e2e4ea; --text-muted: #8b8fa3;
    --accent: #6c8cff; --red: #ff4d6a; --green: #4ddb8a;
    --green-dim: rgba(77,219,138,0.12); --red-dim: rgba(255,77,106,0.12);
}}
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
</style>
</head>
<body>

<div class="header">
    <h1><span>Emerging Edge</span> Portfolio</h1>
    <div style="display:flex;gap:0.5rem;align-items:center">
        <label class="nav-link" style="cursor:pointer">
            Update CSV <input type="file" id="csv-upload" accept=".csv" style="display:none" onchange="uploadCSV(this)">
        </label>
        <button id="mode-toggle" class="nav-link" onclick="toggleMode()" style="cursor:pointer">Show %</button>
        <a href="/monitor" class="nav-link">📊 Monitor</a>
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
        alert('Failed to parse selection: ' + e);
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
    // Convert-specific fields visible only for CONVERT.
    document.querySelectorAll('.txn-convert-field').forEach(
        el => {{ el.style.display = isConvert ? 'flex' : 'none'; }}
    );
}}

function addTransaction() {{
    const date = document.getElementById('txn-date').value;
    const txnType = document.getElementById('txn-type').value;

    if (!date) {{ alert('Please choose a date'); return; }}

    let payload;
    if (txnType === 'CONVERT') {{
        const fromCur = document.getElementById('txn-from-currency').value.trim().toUpperCase();
        const fromAmt = document.getElementById('txn-from-amount').value;
        const toCur = document.getElementById('txn-to-currency').value.trim().toUpperCase();
        const toAmt = document.getElementById('txn-to-amount').value;
        if (!fromCur || !toCur || fromAmt === '' || toAmt === '') {{
            alert('Please fill in from/to currency and both amounts');
            return;
        }}
        if (fromCur === toCur) {{
            alert('From and to currencies must differ');
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
            alert('Please pick a stock from the search dropdown first');
            return;
        }}
        const shares = document.getElementById('txn-shares').value;
        const price = document.getElementById('txn-price').value;
        if (shares === '' || price === '') {{
            alert('Please fill in shares and price');
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
            if (data.warning) alert('⚠ ' + data.warning);
            location.reload();
        }} else {{
            alert('Error: ' + data.message);
        }}
    }})
    .catch(err => alert('Failed: ' + err));
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
            alert('Please fill in all fields');
            return;
        }}
        if (payload.currency === payload.to_currency) {{
            alert('From and to currencies must differ');
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
            alert('Please fill in all fields');
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
            if (data.warning) alert('⚠ ' + data.warning);
            location.reload();
        }} else {{
            alert('Error: ' + data.message);
        }}
    }})
    .catch(err => alert('Failed: ' + err));
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

function deleteTxn(id) {{
    if (!confirm('Delete this transaction?')) return;
    fetch('/api/portfolio/delete', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ id: id }})
    }})
    .then(r => r.json())
    .then(data => {{
        if (data.status === 'ok') {{
            location.reload();
        }} else {{
            alert('Error: ' + data.message);
        }}
    }})
    .catch(err => alert('Failed: ' + err));
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
        alert('File too large (max 2 MB)');
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
                alert('Upload failed: ' + (data.message || 'unknown error'));
            }}
        }})
        .catch(err => alert('Upload failed: ' + err));
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
                alert('Imported ' + data.imported + ' transactions. Reloading...');
                location.reload();
            }} else {{
                alert('Error: ' + data.message);
            }}
        }})
        .catch(err => alert('Upload failed: ' + err));
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
