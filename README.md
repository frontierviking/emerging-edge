# Emerging Edge

A self-hosted dashboard for building a personal watchlist of stocks from
**any exchange in the world**, with news, prices, earnings dates, insider
transactions, forum chatter, and a portfolio tracker — all in one place.

Unlike most finance trackers, Emerging Edge is built to cover **frontier
and emerging markets that Yahoo Finance doesn't index well**: Uzbek,
Kyrgyz, Nigerian, BRVM (West Africa), Malaysian, Singaporean, Nordic,
Indian and more. Everything runs locally on your laptop — no accounts,
no cloud — and the default data sources are free.

## What you see

- **📰 News** — last 3 months by default, expandable to 10 years per
  stock. Free **Yahoo Finance RSS** + free **Google News RSS**
  (locale-aware, so a Swedish stock gets Swedish press coverage and an
  Uzbek stock gets Russian/English), plus optional Serper news search.
- **🔔 Insider transactions** — free direct feeds from **SEC EDGAR**
  (US Form 3/4/5/144), **Finansinspektionen** (Nordic issuers via EU MAR)
  and **KLSE Screener** (Malaysia). Optional Serper fallback for the rest.
- **📅 Earnings calendar** — free per-stock dates from
  **stockanalysis.com** (covers US, LSE, ASX, KLSE, SGX, Frankfurt, TSX,
  HK, Tokyo, NSE India, OMX Stockholm, NGX, BRVM, JSE and more) plus
  12-month backfill from the **NASDAQ calendar** for US stocks.
- **💬 Forum buzz** — i3investor (Malaysia), richbourse (BRVM), any
  **public Telegram channel** you add in the Engine Room, plus Serper
  Twitter/X search on full refresh.
- **📈 Price tracking** — Yahoo chart API for most exchanges, direct
  scrapes for frontier markets (stockscope.uz, brvm.org, kse.kg,
  tradingview NSENG). Multi-currency with a live FX bar that adjusts to
  whatever currencies your watchlist holds.
- **💼 Portfolio tracker** — cash-accounting model
  (BUY / SELL / DIVIDEND / REINVEST / CONVERT). Donut allocation chart
  with company logos, historical value chart with custom date ranges,
  per-holding local-currency and USD returns, status badges (NEW / ADD /
  REDUCED / SOLD).
- **⚙ Engine Room** — operational status: server health, DB backups,
  per-source freshness, Serper credit usage, stock-catalog health, and
  settings for Serper key + Telegram channels.

## Requirements

- **macOS or Linux** with **Python 3.9+**
- **Zero Python packages** — the entire stack uses only the standard
  library. No `pip install`. No venv required.
- Optional: a free **[Serper API](https://serper.dev/)** key (2,500
  free credits at signup). Without it, Emerging Edge still covers most
  exchanges through the free direct sources listed above — Serper only
  unlocks Twitter/X cashtag search, generic discussion web search, and a
  search-based fallback for insider transactions in exchanges that lack
  a free regulatory feed.

## Quick start

```bash
# 1. Clone
git clone https://github.com/frontierviking/emerging-edge.git
cd emerging-edge

# 2. Run the server
python3 monitor.py serve

# 3. Open the app
open http://localhost:8878/
```

That's it. You'll land on the Portfolio screen with an empty watchlist.
Click **➕ Add Stock** in the header, type a company name or ticker in
any language, pick it from the autocomplete dropdown, and Emerging Edge
resolves it to the correct ticker / exchange / currency and starts
tracking it.

For US and Yahoo-indexed stocks it all just works. For NGX, BRVM, UZSE,
KSE the autocomplete draws from a bundled catalog of 300+ pre-populated
frontier-market tickers.

## How to add stocks (beyond the autocomplete)

There's only one way: the **➕ Add Stock** button in the header of the
Monitor page. The autocomplete dropdown searches:

1. **Yahoo Finance symbol search** — covers NASDAQ, NYSE, KLSE, NGX,
   JSE, LSE, OMX Stockholm, ASX, Frankfurt, Hong Kong, Tokyo, TSX, NSE
   India, Euronext, and most other indexed exchanges.
2. **Local frontier catalog** — 312 hand-curated tickers for exchanges
   Yahoo doesn't cover well: NGX (Nigeria), BRVM (West Africa), UZSE
   (Uzbekistan), KSE (Kyrgyzstan).

You can refresh the frontier catalogs from their official listing pages
via **⚙ Engine Room → 📚 Stock Catalog → ↻ Update**.

## Refreshing data

Two refresh buttons on the Monitor page:

- **🆓 Free refresh** — re-fetches prices, news, earnings, insiders,
  and forum buzz from every free source. No Serper credits used.
- **💳 Full refresh** — adds Serper-backed news, contract/tender search,
  Twitter/X forum search, and insider search for exchanges without a free
  regulatory feed. Spends roughly 4–5 Serper credits per stock.

You can save a Serper API key in **⚙ Engine Room → 🔑 Settings & Refresh**
— it's stored in your local SQLite database only.

## Portfolio

1. Open `http://localhost:8878/portfolio`
2. Use the **Add Transaction** form. Transaction types:
   - **BUY** — fresh external capital, adds shares and cost basis
   - **SELL** — credits the cash bucket, reduces shares
   - **DIVIDEND** — credits the cash bucket, tracked as per-position income
   - **REINVEST** — consumes cash in the same currency, adds shares
     (no new external capital — portfolio weights don't shift)
   - **CONVERT** — moves cash between currency buckets at an explicit rate
3. Click any row to edit or delete it inline. Everything persists to
   `emerging_edge.db`.

CSV import is available via
`python3 monitor.py portfolio import your_transactions.csv` — see
`portfolio.py` for the expected column format.

## URLs

- `http://localhost:8878/` — redirects to Portfolio (the starting screen)
- `http://localhost:8878/portfolio` — portfolio tracker
- `http://localhost:8878/monitor` — news / earnings / insider / forum feed
- `http://localhost:8878/engine-room` — operational status and settings

## Data model

Everything lives in one SQLite file (`emerging_edge.db`). Tables:

| Table | What it holds |
|---|---|
| `user_stocks` | Watchlist — resolved by the Add Stock autocomplete |
| `price_snapshots` | Daily price points per ticker × exchange |
| `fx_snapshots` | Historical FX rates to USD |
| `news_items` | Deduped news articles (by URL) |
| `contract_items` | Contract awards / tender wins |
| `forum_mentions` | Forum posts, Telegram messages, Twitter hits |
| `insider_transactions` | Director / PDMR dealings |
| `earnings_dates` | Upcoming and past quarterly report dates |
| `portfolio_transactions` | BUY/SELL/DIVIDEND/REINVEST/CONVERT rows |
| `holding_labels` | NEW/ADD/REDUCED/SOLD status flags |
| `catalog_meta` | Frontier catalog refresh status per exchange |
| `nasdaq_cal_cache` | Cached NASDAQ calendar day-sets (earnings fetcher) |
| `app_settings` | Key-value store for Serper key, Telegram channels |
| `serper_calls` | Serper API call log for credit attribution |

## Telegram forums

Public Telegram channels that discuss stocks on a given exchange can be
added in **⚙ Engine Room → 🔑 Settings & Refresh → Telegram forum channels**.
Emerging Edge scrapes `t.me/s/<handle>` (the public web preview) on
every refresh and surfaces messages mentioning your watchlist stocks in
the Forum Buzz section.

**Public channels only** — private groups require full Telegram account
authentication which is out of scope for a local tool.

## Running as a background service (macOS)

The included `watchdog.sh` + `start-server.sh` scripts can be wired to
a launchd user agent to keep the server running through
sleep/wake/crashes. The watchdog logs to `/tmp/emerging-edge-watchdog.log`.

## Cost

**Free** if you self-host. The only optional paid dependency is
[Serper](https://serper.dev/) (2,500 free credits on signup, $50/month
for 50,000 credits thereafter) if you want Twitter/X cashtag search,
discussion web search, and Serper-backed insider search.

## Disclaimer

This is **not financial advice**. Price and news data may be delayed,
incomplete, or incorrect. Always verify critical information against
authoritative sources before making investment decisions. Use at your
own risk.

## License

MIT. Free to use, modify, and distribute.
