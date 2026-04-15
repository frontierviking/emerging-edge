# Beta release notes

Thanks for trying Emerging Edge. This is alpha / beta software — it
works well for my own use case but you'll find rough edges. This file
lists the **known limitations** by area so you don't waste time
debugging things that are already on my list.

Please file everything you hit at
[Issues](https://github.com/frontierviking/emerging-edge/issues) — a
one-line bug report with the ticker, exchange, and what you expected is
plenty.

---

## 🔧 Setup / running

- **Python 3.9+** on macOS or Linux. Windows is untested.
- No `pip install`. Everything uses the Python standard library.
- The server binds to `localhost:8878` only. If you expose it to the
  network, there's no authentication — anyone on the LAN can see your
  portfolio. Put it behind an SSH tunnel or reverse proxy if you need
  remote access.
- Single-user. The same SQLite database is shared by anyone who hits
  the server. No login, no multi-tenant support.

## 📰 News coverage

- Three free sources: **Yahoo Finance RSS** (Yahoo-indexed exchanges
  only), **Google News RSS** (every stock, locale-aware per exchange),
  and Serper news search (paid, only on full refresh).
- Google News query is the company name in double quotes, so stocks
  with very short or generic names get noisy results (e.g. "Visa" picks
  up US travel-document articles).
- The age filter shows the last 3 months by default; filtering by stock
  or exchange relaxes the filter to "all dates".
- Non-Latin scripts (Cyrillic, CJK, Arabic, Korean) are stripped from
  news and forum items before rendering. If you want to see them, the
  filter is in `dashboard.py::_filter_latin`.

## 📅 Earnings calendar

- Primary source is **stockanalysis.com** — covers US, NGX, BRVM, LSE,
  ASX, KLSE, SGX, Frankfurt, TSX, HK, Tokyo, NSE India, OMX Stockholm,
  and a few more. One HTTP call per stock.
- US stocks also get 12 months of past quarterly reports from the
  **NASDAQ earnings calendar**. First US stock you add triggers ~440
  HTTP calls (cached in SQLite afterwards) — this can take ~30s.
- **UZSE (Uzbekistan)** and **KSE (Kyrgyzstan)** are NOT covered by
  stockanalysis.com or any other free source I've found. Stocks from
  those exchanges will show "no upcoming earnings". If you know of a
  structured data source, please file an issue.
- Exchange-specific page scrapes (klsescreener, NGX company profile,
  brvm.org, kse.kg, uzse.uz) exist as a secondary fallback but rarely
  find dates because the pages aren't structured.

## 🔔 Insider transactions

| Exchange | Source | Notes |
|---|---|---|
| NASDAQ / NYSE | SEC EDGAR Form 4 atom | Needs a CIK configured in the stock row (not automatic yet) |
| KLSE Malaysia | KLSE Screener scrape | Free |
| OMX / OSE / CSE / HEL (Nordics) | Finansinspektionen insider register | Matches on issuer name — if the stock name in the EU MAR register doesn't match the Yahoo name, no rows come back |
| Everything else | Serper search fallback (full refresh only) | Paid |

**Known gap**: adding a NASDAQ stock doesn't auto-look-up the CIK. You
either need to add it manually or wait for SEC to appear missing and
use full refresh which falls back to Serper.

## 💬 Forum buzz

- Free sources: **i3investor** (Malaysia), **richbourse** (BRVM),
  **public Telegram channels** (user-configured per exchange in the
  Engine Room).
- Twitter/X cashtag search runs on full refresh only (costs Serper credits).
- **Private Telegram groups** are NOT supported. Only channels with a
  public `t.me/<handle>` page. This is a structural limit of the
  `t.me/s/` web preview — private groups require full Telegram
  MTProto account auth, which is intentionally out of scope.
- Telegram scraping only sees the **last ~20 messages** of a channel
  (the public web preview limit). So a channel that posts 50 times a
  day will lose older posts between refreshes.

## 📈 Prices

- **Yahoo Finance chart API** handles most exchanges. Exotic pairs can
  time out — the UI shows the last known price from the DB in that case.
- Direct scrapers: `stockscope.uz` (UZSE), `brvm.org` (BRVM),
  `kse.kg` (KSE Kyrgyzstan), `tradingview.com/symbols/NSENG-...` (NGX).
  These can break if the sites redesign — file an issue with a sample
  ticker and the HTTP response code if prices stop appearing.
- **FX bar** is dynamic: any non-USD currency in the watchlist shows
  up. Exotic currencies (UZS, NGN, KGS) can take a few seconds to load
  on first render — fetches run in the background so the page itself
  doesn't block.

## 💼 Portfolio

- **Cash-accounting model**: BUY uses external capital, SELL / DIVIDEND
  credit cash, REINVEST consumes cash in the same currency, CONVERT
  moves cash between currency buckets at an explicit rate. This is
  different from the "average cost" model most portfolio trackers use —
  it's intentional but takes getting used to.
- If you REINVEST in a currency you don't have cash for, the transaction
  still goes through but the cash balance goes negative. That's a
  feature (you might be modelling a future reinvestment) but can be
  surprising.
- CSV import via `python3 monitor.py portfolio import your_csv.csv`.
  Column format is enforced — see `portfolio.py::_parse_csv_row` for
  the expected header.
- **Donut logo upload**: in the percentage view, click any holding in
  the donut → upload a company logo (PNG/JPG/SVG up to 2 MB). Logos
  live in `logos/` and are referenced by ticker filename.

## ⚙ Engine Room

- **Free vs Full refresh badges** in the Data Sources card reflect
  whether a source runs in free mode. Anything labelled PAID only runs
  when you click 💳 Full refresh.
- Serper credit estimate is based on "1 credit per paid source per
  stock" — rough but usually within 20% of actual usage.
- Stock Catalog covers only frontier exchanges Yahoo doesn't index
  (NGX, BRVM, UZSE, KSE). Clicking Update re-scrapes the official
  listing page and may add new tickers. Existing curated names are
  preserved.
- Telegram channel config is per-exchange, not per-stock. If you want
  different channels for different stocks on the same exchange, that's
  not supported yet — all stocks on that exchange get the same channels.

## 🐛 Known bugs / gotchas

- Browser cache can occasionally serve a stale monitor page after a
  server restart. If something looks wrong, hard-reload (Cmd+Shift+R
  on Mac, Ctrl+Shift+R on Windows/Linux).
- The first cold page load after a server restart can take a few
  seconds because the FX cache needs to warm. After that it's millisecond-fast.
- Removing a stock from the watchlist doesn't clean up its historical
  news / forum / insider / price rows. They stay in the DB harmlessly
  but take up space.
- Portfolio "Past Reports" tab only populates for NASDAQ/NYSE stocks
  (via NASDAQ calendar backfill). Other exchanges show "No past reports".

## What I'm NOT looking for feedback on (yet)

- **Styling** — dark theme is intentional, font stack is system-default,
  I'm not taking visual polish PRs until the data layer is solid.
- **Windows support** — untested, probably mostly works, but I won't
  prioritise fixes until macOS/Linux users are happy.
- **Mobile / responsive layout** — the monitor page is designed for
  desktop. A mobile-optimised layout is a separate project.

## What I AM looking for

- **Coverage gaps**: "I added $TICKER and got no earnings / no news /
  no insider". Include the exchange and the company name.
- **Data quality bugs**: "Price for $TICKER is wrong — correct value is
  X". Include the source you'd check.
- **Onboarding friction**: what you had to figure out on your own that
  should have been obvious.
- **Portfolio model weirdness**: any transaction type that produces a
  number you didn't expect.

Thanks for beta testing 🙏

— Martin
