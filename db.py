"""
db.py — SQLite persistence layer for emerging-edge.

Tables:
  news_items      – articles from Serper news searches
  contract_items  – contract/tender search results
  earnings_dates  – next reporting dates per stock
  forum_mentions  – forum posts (i3investor, richbourse, sikafinance, etc.)
  price_snapshots – daily price captures

Every table uses a unique key (usually a URL or hash) so duplicate
items are silently skipped on INSERT (INSERT OR IGNORE).
"""

from __future__ import annotations

import sqlite3
import hashlib
import json
import os
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS news_items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT UNIQUE NOT NULL,          -- dedupe key
    ticker      TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    title       TEXT,
    snippet     TEXT,
    source      TEXT,                          -- e.g. "The Edge Markets"
    published   TEXT,                          -- ISO date from Serper
    fetched_at  TEXT NOT NULL,                 -- when we stored it
    search_type TEXT DEFAULT 'news',           -- 'news' or 'resultats'
    lang        TEXT DEFAULT 'en'
);

CREATE TABLE IF NOT EXISTS contract_items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT UNIQUE NOT NULL,
    ticker      TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    title       TEXT,
    snippet     TEXT,
    source      TEXT,
    published   TEXT,
    fetched_at  TEXT NOT NULL,
    lang        TEXT DEFAULT 'en'
);

CREATE TABLE IF NOT EXISTS earnings_dates (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker       TEXT NOT NULL,
    exchange     TEXT NOT NULL,
    report_date  TEXT,                         -- ISO date or free text
    fiscal_period TEXT,                        -- e.g. "Q1 2026"
    source_url   TEXT,
    fetched_at   TEXT NOT NULL,
    UNIQUE(ticker, exchange, report_date)
);

CREATE TABLE IF NOT EXISTS forum_mentions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    hash        TEXT UNIQUE NOT NULL,          -- SHA-256 of (ticker+text[:200])
    ticker      TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    forum       TEXT NOT NULL,                 -- e.g. "i3investor"
    author      TEXT,
    text        TEXT,
    post_url    TEXT,
    posted_at   TEXT,
    fetched_at  TEXT NOT NULL,
    lang        TEXT DEFAULT 'en'
);

CREATE TABLE IF NOT EXISTS price_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    price       REAL,
    change_pct  REAL,
    currency    TEXT,
    source_url  TEXT,
    snapshot_at TEXT NOT NULL,
    UNIQUE(ticker, exchange, snapshot_at)
);

CREATE TABLE IF NOT EXISTS insider_transactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT UNIQUE NOT NULL,          -- dedupe key
    ticker      TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    title       TEXT,
    snippet     TEXT,
    source      TEXT,
    published   TEXT,
    fetched_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_news_ticker   ON news_items(ticker);
CREATE INDEX IF NOT EXISTS idx_news_fetched  ON news_items(fetched_at);
CREATE INDEX IF NOT EXISTS idx_insider_ticker ON insider_transactions(ticker);
CREATE INDEX IF NOT EXISTS idx_insider_fetched ON insider_transactions(fetched_at);
CREATE INDEX IF NOT EXISTS idx_contract_ticker ON contract_items(ticker);
CREATE INDEX IF NOT EXISTS idx_forum_ticker  ON forum_mentions(ticker);
CREATE INDEX IF NOT EXISTS idx_earnings_ticker ON earnings_dates(ticker);
CREATE INDEX IF NOT EXISTS idx_price_ticker  ON price_snapshots(ticker);

CREATE TABLE IF NOT EXISTS portfolio_transactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    txn_type    TEXT NOT NULL,            -- 'BUY','SELL','DIVIDEND','REINVEST','CONVERT'
    shares      REAL NOT NULL,            -- for CONVERT: from_amount
    price       REAL NOT NULL,            -- for CONVERT: unused (kept 1.0)
    currency    TEXT NOT NULL,            -- for CONVERT: from_currency
    txn_date    TEXT NOT NULL,            -- ISO YYYY-MM-DD
    imported_at TEXT NOT NULL,
    to_currency TEXT,                     -- CONVERT: destination currency
    to_amount   REAL,                     -- CONVERT: destination amount
    UNIQUE(ticker, exchange, txn_type, shares, price, txn_date)
);

CREATE TABLE IF NOT EXISTS fx_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    currency    TEXT NOT NULL,
    rate_to_usd REAL NOT NULL,            -- 1 USD = X currency
    snapshot_at TEXT NOT NULL,            -- ISO YYYY-MM-DD
    UNIQUE(currency, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_txn_ticker ON portfolio_transactions(ticker);
CREATE INDEX IF NOT EXISTS idx_txn_date   ON portfolio_transactions(txn_date);
CREATE INDEX IF NOT EXISTS idx_fx_currency ON fx_snapshots(currency);
"""


class Database:
    """Thin wrapper around SQLite for the emerging-edge store."""

    def __init__(self, db_path: str = "emerging_edge.db"):
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        # Extra tables (safe to run every init — CREATE IF NOT EXISTS)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS holding_labels (
                ticker TEXT PRIMARY KEY,
                label  TEXT NOT NULL DEFAULT ''
            )""")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS serper_calls (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                called_at TEXT NOT NULL,
                endpoint  TEXT NOT NULL,
                caller    TEXT NOT NULL,
                ticker    TEXT,
                query     TEXT,
                ok        INTEGER NOT NULL DEFAULT 1
            )""")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_serper_called_at ON serper_calls(called_at)")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_serper_caller ON serper_calls(caller)")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS user_stocks (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker          TEXT NOT NULL,
                exchange        TEXT NOT NULL,
                name            TEXT NOT NULL,
                currency        TEXT NOT NULL,
                yahoo_ticker    TEXT,
                lang            TEXT DEFAULT 'en',
                forum_sources   TEXT,
                earnings_source TEXT,
                code            TEXT,
                country         TEXT,
                notes           TEXT,
                price_url       TEXT,
                added_at        TEXT NOT NULL,
                UNIQUE(ticker, exchange)
            )""")
        # Lightweight migrations: add price_url column if missing
        try:
            self.conn.execute("ALTER TABLE user_stocks ADD COLUMN price_url TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
        # Lightweight migrations for columns added after the initial schema.
        for col_sql in (
            "ALTER TABLE portfolio_transactions ADD COLUMN to_currency TEXT",
            "ALTER TABLE portfolio_transactions ADD COLUMN to_amount REAL",
        ):
            try:
                self.conn.execute(col_sql)
            except sqlite3.OperationalError:
                pass  # column already exists
        self.conn.commit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _now() -> str:
        return datetime.utcnow().isoformat() + "Z"

    @staticmethod
    def _hash(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def last_fetched(self, table: str, ticker: str) -> Optional[str]:
        """
        Return the most recent fetched_at timestamp for a ticker in
        a given table, or None if no data exists.
        Used to check staleness and skip unnecessary Serper calls.
        """
        try:
            row = self.conn.execute(
                f"SELECT MAX(fetched_at) as last FROM {table} WHERE ticker = ?",
                (ticker,)).fetchone()
            return row["last"] if row and row["last"] else None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # News
    # ------------------------------------------------------------------
    def insert_news(self, ticker: str, exchange: str, url: str,
                    title: str, snippet: str, source: str,
                    published: str, search_type: str = "news",
                    lang: str = "en") -> bool:
        """Insert a news item. Returns True if new, False if duplicate."""
        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO news_items
                   (url, ticker, exchange, title, snippet, source,
                    published, fetched_at, search_type, lang)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (url, ticker, exchange, title, snippet, source,
                 published, self._now(), search_type, lang))
            self.conn.commit()
            return self.conn.total_changes > 0
        except sqlite3.IntegrityError:
            return False

    def get_news_since(self, since_iso: str, ticker: str = None) -> list[dict]:
        """Get news items fetched after `since_iso`."""
        if ticker:
            rows = self.conn.execute(
                "SELECT * FROM news_items WHERE fetched_at >= ? AND ticker = ? ORDER BY published DESC",
                (since_iso, ticker)).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM news_items WHERE fetched_at >= ? ORDER BY published DESC",
                (since_iso,)).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Contracts / Tenders
    # ------------------------------------------------------------------
    def insert_contract(self, ticker: str, exchange: str, url: str,
                        title: str, snippet: str, source: str,
                        published: str, lang: str = "en") -> bool:
        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO contract_items
                   (url, ticker, exchange, title, snippet, source,
                    published, fetched_at, lang)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (url, ticker, exchange, title, snippet, source,
                 published, self._now(), lang))
            self.conn.commit()
            return self.conn.total_changes > 0
        except sqlite3.IntegrityError:
            return False

    def get_contracts_since(self, since_iso: str, ticker: str = None) -> list[dict]:
        if ticker:
            rows = self.conn.execute(
                "SELECT * FROM contract_items WHERE fetched_at >= ? AND ticker = ? ORDER BY published DESC",
                (since_iso, ticker)).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM contract_items WHERE fetched_at >= ? ORDER BY published DESC",
                (since_iso,)).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Insider Transactions
    # ------------------------------------------------------------------
    def insert_insider(self, ticker: str, exchange: str, url: str,
                       title: str, snippet: str, source: str,
                       published: str) -> bool:
        """Insert an insider transaction item. Returns True if new."""
        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO insider_transactions
                   (url, ticker, exchange, title, snippet, source,
                    published, fetched_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (url, ticker, exchange, title, snippet, source,
                 published, self._now()))
            self.conn.commit()
            return self.conn.total_changes > 0
        except sqlite3.IntegrityError:
            return False

    def get_insiders_since(self, since_iso: str, ticker: str = None) -> list[dict]:
        if ticker:
            rows = self.conn.execute(
                "SELECT * FROM insider_transactions WHERE fetched_at >= ? AND ticker = ? ORDER BY published DESC",
                (since_iso, ticker)).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM insider_transactions WHERE fetched_at >= ? ORDER BY published DESC",
                (since_iso,)).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Earnings Dates
    # ------------------------------------------------------------------
    def upsert_earnings(self, ticker: str, exchange: str,
                        report_date: str, fiscal_period: str,
                        source_url: str) -> bool:
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO earnings_dates
                   (ticker, exchange, report_date, fiscal_period,
                    source_url, fetched_at)
                   VALUES (?,?,?,?,?,?)""",
                (ticker, exchange, report_date, fiscal_period,
                 source_url, self._now()))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def get_upcoming_earnings(self, within_days: int = 30) -> list[dict]:
        """Return earnings dates in the next N days."""
        rows = self.conn.execute(
            """SELECT * FROM earnings_dates
               WHERE report_date >= date('now')
                 AND report_date <= date('now', ? || ' days')
               ORDER BY report_date ASC""",
            (str(within_days),)).fetchall()
        return [dict(r) for r in rows]

    def get_all_earnings(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM earnings_dates ORDER BY report_date ASC").fetchall()
        return [dict(r) for r in rows]

    def get_past_earnings(self, within_days: int = 365) -> list[dict]:
        """Return earnings dates in the past N days."""
        rows = self.conn.execute(
            """SELECT * FROM earnings_dates
               WHERE report_date < date('now')
                 AND report_date >= date('now', '-' || ? || ' days')
               ORDER BY report_date DESC""",
            (str(within_days),)).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Forum Mentions
    # ------------------------------------------------------------------
    def insert_forum(self, ticker: str, exchange: str, forum: str,
                     author: str, text: str, post_url: str,
                     posted_at: str, lang: str = "en") -> bool:
        h = self._hash(f"{ticker}{text[:200]}")
        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO forum_mentions
                   (hash, ticker, exchange, forum, author, text,
                    post_url, posted_at, fetched_at, lang)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (h, ticker, exchange, forum, author, text,
                 post_url, posted_at, self._now(), lang))
            self.conn.commit()
            return self.conn.total_changes > 0
        except sqlite3.IntegrityError:
            return False

    def get_forum_since(self, since_iso: str, ticker: str = None) -> list[dict]:
        if ticker:
            rows = self.conn.execute(
                "SELECT * FROM forum_mentions WHERE fetched_at >= ? AND ticker = ? ORDER BY posted_at DESC",
                (since_iso, ticker)).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM forum_mentions WHERE fetched_at >= ? ORDER BY posted_at DESC",
                (since_iso,)).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Price Snapshots
    # ------------------------------------------------------------------
    def insert_price(self, ticker: str, exchange: str,
                     price: float, change_pct: float,
                     currency: str, source_url: str,
                     snapshot_date: str = None) -> bool:
        """
        Store a price snapshot. Uses INSERT OR REPLACE so that
        refreshing during the day updates to the latest live price
        (the UNIQUE constraint is on ticker+exchange+snapshot_at).
        Optional snapshot_date for backfilling historical data.
        """
        snapshot_at = snapshot_date or datetime.utcnow().strftime("%Y-%m-%d")
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO price_snapshots
                   (ticker, exchange, price, change_pct, currency,
                    source_url, snapshot_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (ticker, exchange, price, change_pct, currency,
                 source_url, snapshot_at))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def get_latest_price(self, ticker: str, exchange: str) -> dict | None:
        row = self.conn.execute(
            """SELECT * FROM price_snapshots
               WHERE ticker = ? AND exchange = ?
               ORDER BY snapshot_at DESC LIMIT 1""",
            (ticker, exchange)).fetchone()
        return dict(row) if row else None

    def get_latest_prices_by_exchange(self, exchange: str) -> list[dict]:
        """Return latest price snapshot for each stock in an exchange."""
        rows = self.conn.execute(
            """SELECT p.* FROM price_snapshots p
               INNER JOIN (
                   SELECT ticker, MAX(snapshot_at) AS max_date
                   FROM price_snapshots
                   WHERE exchange = ?
                   GROUP BY ticker
               ) latest ON p.ticker = latest.ticker
                       AND p.snapshot_at = latest.max_date
                       AND p.exchange = ?
               ORDER BY p.ticker""",
            (exchange, exchange)).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Portfolio Transactions
    # ------------------------------------------------------------------
    def insert_transaction(self, ticker: str, exchange: str,
                           txn_type: str, shares: float, price: float,
                           currency: str, txn_date: str,
                           to_currency: str = None,
                           to_amount: float = None) -> int:
        """Insert a new transaction. Returns the new row id on success,
        or 0 if the row was a duplicate (or on integrity error).

        For CONVERT transactions, use:
          ticker="_CASH_", exchange="_CASH_", shares=from_amount,
          price=1.0, currency=from_currency, to_currency=..., to_amount=...
        """
        try:
            cur = self.conn.execute(
                """INSERT OR IGNORE INTO portfolio_transactions
                   (ticker, exchange, txn_type, shares, price, currency,
                    txn_date, imported_at, to_currency, to_amount)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (ticker, exchange, txn_type.upper(), shares, price,
                 currency, txn_date, self._now(), to_currency, to_amount))
            self.conn.commit()
            # cur.lastrowid is 0 when INSERT OR IGNORE was a no-op
            return cur.lastrowid or 0
        except sqlite3.IntegrityError:
            return 0

    def update_transaction(self, txn_id: int, ticker: str, exchange: str,
                           txn_type: str, shares: float, price: float,
                           currency: str, txn_date: str,
                           to_currency: str = None,
                           to_amount: float = None) -> bool:
        cur = self.conn.execute(
            """UPDATE portfolio_transactions
               SET ticker=?, exchange=?, txn_type=?, shares=?, price=?,
                   currency=?, txn_date=?, to_currency=?, to_amount=?
               WHERE id=?""",
            (ticker, exchange, txn_type.upper(), shares, price,
             currency, txn_date, to_currency, to_amount, txn_id))
        self.conn.commit()
        return cur.rowcount > 0

    def get_all_transactions(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM portfolio_transactions ORDER BY txn_date ASC, id ASC"
        ).fetchall()
        return [dict(r) for r in rows]

    def clear_transactions(self):
        self.conn.execute("DELETE FROM portfolio_transactions")
        self.conn.commit()

    # ------------------------------------------------------------------
    # Holding Labels (manual status: NEW, ADD, REDUCED, SOLD)
    # ------------------------------------------------------------------
    def get_holding_labels(self) -> dict:
        """Return {ticker: label} for all labelled holdings."""
        rows = self.conn.execute(
            "SELECT ticker, label FROM holding_labels WHERE label != ''"
        ).fetchall()
        return {r["ticker"]: r["label"] for r in rows}

    def set_holding_label(self, ticker: str, label: str):
        """Set or clear a holding's status label."""
        if label:
            self.conn.execute(
                "INSERT OR REPLACE INTO holding_labels (ticker, label) VALUES (?, ?)",
                (ticker.upper(), label.upper()))
        else:
            self.conn.execute(
                "DELETE FROM holding_labels WHERE ticker = ?", (ticker.upper(),))
        self.conn.commit()

    # ------------------------------------------------------------------
    # User Stocks (watchlist entries added at runtime)
    # ------------------------------------------------------------------
    def get_user_stocks(self) -> list[dict]:
        """Return all user-added stocks in config['stocks'] shape.

        forum_sources is decoded from its JSON string to a list.
        """
        rows = self.conn.execute(
            "SELECT * FROM user_stocks ORDER BY ticker ASC"
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            # Decode forum_sources JSON to a list (fall back to [])
            fs = d.get("forum_sources")
            try:
                d["forum_sources"] = json.loads(fs) if fs else []
            except (ValueError, TypeError):
                d["forum_sources"] = []
            # Drop internal columns not expected by callers
            d.pop("id", None)
            d.pop("added_at", None)
            out.append(d)
        return out

    def add_user_stock(self, meta: dict) -> bool:
        """Insert a user stock. Returns True if newly added, False if duplicate."""
        ticker = (meta.get("ticker") or "").strip().upper()
        exchange = (meta.get("exchange") or "").strip().upper()
        if not ticker or not exchange:
            return False
        fs = meta.get("forum_sources", [])
        if isinstance(fs, list):
            fs_str = json.dumps(fs)
        else:
            fs_str = fs or "[]"
        try:
            cur = self.conn.execute(
                """INSERT OR IGNORE INTO user_stocks
                   (ticker, exchange, name, currency, yahoo_ticker, lang,
                    forum_sources, earnings_source, code, country, notes,
                    price_url, added_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ticker, exchange,
                 meta.get("name", ticker),
                 (meta.get("currency") or "USD").upper(),
                 meta.get("yahoo_ticker", ""),
                 meta.get("lang", "en"),
                 fs_str,
                 meta.get("earnings_source", ""),
                 meta.get("code", ""),
                 meta.get("country", ""),
                 meta.get("notes", ""),
                 meta.get("price_url", ""),
                 self._now()))
            self.conn.commit()
            return cur.rowcount > 0
        except sqlite3.IntegrityError:
            return False

    def remove_user_stock(self, ticker: str, exchange: str) -> bool:
        cur = self.conn.execute(
            "DELETE FROM user_stocks WHERE ticker = ? AND exchange = ?",
            (ticker.upper(), exchange.upper()))
        self.conn.commit()
        return cur.rowcount > 0

    # ------------------------------------------------------------------
    # FX Snapshots
    # ------------------------------------------------------------------
    def insert_fx_rate(self, currency: str, rate_to_usd: float,
                       snapshot_date: str) -> bool:
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO fx_snapshots
                   (currency, rate_to_usd, snapshot_at)
                   VALUES (?,?,?)""",
                (currency, rate_to_usd, snapshot_date))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def get_fx_rate(self, currency: str, on_or_before: str) -> Optional[float]:
        """Return the FX rate closest to (but not after) the given date."""
        if currency == "USD":
            return 1.0
        row = self.conn.execute(
            """SELECT rate_to_usd FROM fx_snapshots
               WHERE currency = ? AND snapshot_at <= ?
               ORDER BY snapshot_at DESC LIMIT 1""",
            (currency, on_or_before)).fetchone()
        if row:
            return row["rate_to_usd"]
        # Fallback: earliest rate we have
        row = self.conn.execute(
            """SELECT rate_to_usd FROM fx_snapshots
               WHERE currency = ?
               ORDER BY snapshot_at ASC LIMIT 1""",
            (currency,)).fetchone()
        return row["rate_to_usd"] if row else None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self):
        self.conn.close()
