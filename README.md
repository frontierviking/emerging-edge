# Emerging Edge

A self-hosted dashboard for tracking **frontier and emerging markets** stocks — news, price action, earnings calendars, insider transactions, forum chatter, and your personal portfolio — all in one place.

Currently tracks 23 stocks across 8 exchanges: KLSE (Malaysia), NGX (Nigeria), BRVM (West Africa), UZSE (Uzbekistan), SGX (Singapore), KSE (Kyrgyzstan), NASDAQ (frontier-relevant US listings), and JSE (South Africa). Easy to add more.

## What it gives you

- **📰 News** — last 3 months by default, extendable to 10 years per stock. Free Yahoo Finance RSS for NASDAQ/KLSE/SGX/JSE; Serper search for frontier exchanges Yahoo doesn't cover.
- **💬 Forum buzz** — i3investor, richbourse, Telegram channels (all free direct scraping), plus Twitter/X and generic web discussion via Serper.
- **🔔 Insider transactions** — SEC EDGAR Form 3/4/5/144/Schedule 13 for NASDAQ (free, authoritative); KLSE Screener scrape for Bursa Malaysia (free); Serper fallback for other exchanges.
- **📅 Earnings calendar** — upcoming and past quarterly reports with direct links to the report filings.
- **📈 Price tracking** — Yahoo Finance + direct exchange-site scrapers. Multi-currency with FX rates stored historically.
- **💼 Portfolio tracker** — cash-accounting model (BUY, SELL, DIVIDEND, REINVEST, CONVERT). Donut allocation chart with company logos, historical value chart with custom date ranges, per-holding local and USD returns.
- **⚙ Engine Room** — operational status page showing server health, DB backups, Serper API credit usage with breakdown by category, and per-source freshness indicators.

## Requirements

- **macOS or Linux** with **Python 3.9+** (uses only the standard library — no pip install needed for the core)
- A free **[Serper API](https://serper.dev/)** key (optional — you get **2,500 free credits** at signup, which is enough for ~6 months of news+insider fetches on a ~20-stock watchlist). Without a key, the dashboard falls back to direct scrapers only.

## Quick start

```bash
# 1. Clone this repo
git clone https://github.com/YOUR_USERNAME/emerging-edge.git
cd emerging-edge

# 2. Set your Serper API key (optional but recommended)
cp .env.example .env
# Edit .env and paste your key from https://serper.dev/

# 3. Run the server
./start-server.sh
# Open http://localhost:8878/ in your browser
```

The dashboard auto-generates on first request and refreshes can be triggered via the floating refresh button in the bottom-right corner.

## Customizing your stock watchlist

Edit `config.json` to add, remove, or modify tracked stocks. Each stock entry looks like this:

```json
{
  "name": "Matrix Concept Holdings",
  "ticker": "MATRIX",
  "code": "5236",
  "exchange": "KLSE",
  "country": "Malaysia",
  "lang": "en",
  "forum_sources": ["i3investor", "twitter"],
  "earnings_source": "klsescreener",
  "yahoo_ticker": "5236.KL",
  "currency": "MYR",
  "notes": "Property developer"
}
```

For NASDAQ stocks, add a `"cik"` field (10-digit string, zero-padded) to enable SEC EDGAR Form 4 fetching. Look up CIKs at https://www.sec.gov/cgi-bin/browse-edgar.

## Adding portfolio transactions

1. Open `http://localhost:8878/portfolio`
2. Use the **Add Transaction** form at the bottom. Types supported:
   - **BUY** — always fresh external capital, adds to shares and cost basis.
   - **SELL** — credits cash, reduces shares.
   - **DIVIDEND** — credits cash, tracked as per-position income.
   - **REINVEST** — consumes cash in the same currency, adds shares and basis (no new external capital).
   - **CONVERT** — moves cash between currency buckets at an explicit rate.
3. Transactions can be edited or deleted inline. Changes are persisted to `emerging_edge.db`.

You can also import a CSV via `python3 monitor.py portfolio import your_transactions.csv` (see `portfolio.py` for the CSV format).

## URLs

- `http://localhost:8878/` — main dashboard (news, forums, earnings, insider, prices)
- `http://localhost:8878/portfolio` — portfolio tracker (holdings, transactions, allocation donut)
- `http://localhost:8878/engine-room` — operational status (server, backups, Serper usage, source health)

## Data model

SQLite in a single file (`emerging_edge.db`). Tables:
- `price_snapshots` — historical daily prices per ticker+exchange
- `fx_snapshots` — historical FX rates to USD
- `news_items` — deduped news articles (by URL)
- `contract_items` — contract awards / tenders
- `forum_mentions` — forum posts and discussions
- `insider_transactions` — insider/director dealings
- `earnings_dates` — upcoming and past quarterly report dates
- `portfolio_transactions` — user-entered buy/sell/dividend/reinvest/convert rows
- `holding_labels` — manual per-holding status (NEW/ADD/REDUCED/SOLD)
- `serper_calls` — Serper API call log for credit attribution

## Running as a background service (macOS)

The included `watchdog.sh` + launchd plist template can keep the server running and auto-restart it on sleep/wake or crash. See `LAUNCHD_SETUP.md` for installation instructions.

## Cost

Everything here is **free** if you self-host. The only optional paid dependency is Serper (2,500 free credits at signup; $50/month for 50,000 credits thereafter) if you want automated Twitter/X search, forum web search, and insider search for exchanges without free direct scrapers.

## Disclaimer

This is **not financial advice**. Price and news data may be delayed, incomplete, or incorrect. Always verify critical information against authoritative sources before making investment decisions. Use at your own risk.

## License

MIT. Free to use, modify, and distribute.
