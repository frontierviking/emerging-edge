# Emerging Edge — Session Context

Self-hosted dashboard for frontier and emerging markets stocks. Public version of the personal frontier-monitor (located at `~/AI/frontier-monitor/`). Commercial product, MIT-licensed, beta-tester-ready.

## Framework

**Stack**: Python 3.9+ stdlib only. No pip dependencies, no JS build step. SQLite for persistence. `http.server` for the web server. Self-contained HTML with inline CSS/JS.

**Files**:
- `monitor.py` — CLI entrypoint + HTTP server on port 8878
- `dashboard.py` — Monitor page HTML builder (single-page dashboard)
- `portfolio.py` — Portfolio tracking page
- `engine_room.py` — Data-source health + refresh controls page
- `fetchers.py` — All data collection (news, prices, earnings, insider, forum)
- `stock_search.py` — Yahoo + catalog search, exchange metadata
- `catalog_updaters.py` — Per-exchange catalog scrapers
- `frontier_stocks.json` — Catalog of ~2,200 tickers Yahoo doesn't index well
- `db.py` — SQLite wrapper, schema in top-of-file DDL
- `config.json` — User-visible config; `stocks: []` empty by default

**Key DB tables**: `news_items`, `forum_mentions`, `earnings_dates`, `price_snapshots`, `insider_transactions`, `portfolio_transactions`, `user_stocks` (user-added watchlist), `catalog_meta`, `app_settings` (KV for Serper key / telegram channels).

## Free-first architecture

Every data source has a **free tier** primary. Serper is the paid fallback, gated behind `_SERPER_ENABLED` so "Free refresh" skips all paid calls.

**Prices** — tiered fallback in `fetch_prices()`:
1. Yahoo chart API if `yahoo_ticker` set (covers 25+ major exchanges)
2. Per-exchange custom scrapers in `_fetch_price_scrape()` — AFX Kwayisi (Kenya/Ghana/Botswana/Zambia/Uganda), KASE, DSET/Tanzania (JSON API), DSEB, PSX, ZSE, CSEL (POST JSON API), RSE, SEM, ISX, UZSE, NGX (TradingView), BRVM, KSE
3. Serper search (last resort, paid)

**News** — in `fetch_news()`:
1. Dedicated RSS (ISX → iraq-businessnews, NGX → nairametrics, GSE → ghanabusinessnews, African → allafrica)
2. Yahoo Finance RSS (NASDAQ/KLSE/SGX/JSE)
3. Google News search RSS with per-exchange locale (`_GNEWS_LOCALE`) — covers every exchange in any language
4. Serper news (only if Yahoo empty AND Serper enabled)

**Earnings** — in `fetch_earnings()`:
1. stockanalysis.com (covers ~14 exchanges via `_SA_SLUG` map)
2. Exchange-specific templates from `config.json["earnings_urls"]` (always runs, can store dates with exchange source_url)
3. NASDAQ calendar JSON API for US (forward + backward scan for past reports)
4. Serper fallback (if Serper enabled)

**Insiders** — in `fetch_insiders()`:
1. SEC EDGAR for NASDAQ/NYSE (Form 3/4/5 via CIK)
2. KLSE Screener for Bursa Malaysia
3. Finansinspektionen for Nordic exchanges (OMX/OSE/CSE/HEL — MAR PDMR register)
4. Serper queries (only if Serper enabled; SGX gets `site:sgx.com` queries)

**Forum** — per-stock `forum_sources` list determines which scrapers run. Free: i3investor (KLSE), richbourse (BRVM), Telegram channels, and 17+ others wired into `_EXCHANGE_DEFAULTS`. Paid: twitter/web/serper_discuss markers.

## Exchange coverage (66 exchanges)

**Yahoo-covered** (25, via `_YAHOO_TO_INTERNAL` in stock_search.py):
NASDAQ, NYSE, LSE, ASX, TSX, HKSE, SGX, KLSE, JSE, FRA, BIT, OMX, OSE, CSE (Copenhagen), SWX, EURONEXT, B3, BCBA, BMV, NSE (India), BSE (India), KRX, TWSE, IDX, SET, PSE, HOSE, TASE, TADAWUL, DFM, ADX, QSE, BIST, WSE, PSE_CZ, BET, ATHEX, BVB, NZX, SSE, SZSE, BRVM, NGX, UZSE, KASE (overlap with catalog).

**Catalog-only** (in `catalog_updaters.UPDATERS`):
UZSE, NGX, BRVM, KSE, KASE, NSEK, GSE, BWSE, LUSE, DSET, DSEB, PSX, CSEM, ZSE, BELEX, BSSE, PNGX, BVMT, CSEL, UX, USE, RSE, SEM, ISX, ESX.

**True frontier** (gets `FRONTIER` badge in Add Stock dropdown — 22 exchanges):
UZSE, KSE, KASE, BRVM, NGX, NSEK, GSE, BWSE, LUSE, DSET, DSEB, CSEL, BVMT, CSEM, USE, RSE, SEM, ISX, ESX, BELEX, PNGX, UX, PSX.

ZSE (Croatia) and BSSE (Slovakia) are EU/eurozone members — in catalog but NOT flagged frontier.

## Watchlist / portfolio model

- `config.json["stocks"]` = [] in emerging-edge (empty by default)
- User-added stocks persist in `user_stocks` DB table
- `get_active_stocks(db, config)` merges config + user_stocks, deduped by (ticker, exchange)
- All UI code reads from `get_active_stocks`, never `config["stocks"]` directly
- Portfolio tracks transactions in `portfolio_transactions` (buy/sell/dividend/split)

## Refresh modes

- **Free refresh** — Sets `_SERPER_ENABLED = False`. Runs all free sources only. No credits burned.
- **Full refresh** — Needs a Serper API key (set via engine room → saved in `app_settings.serper_api_key`). Runs free sources THEN Serper fallbacks. Button is disabled if no key.

Dashboard chip states distinguish:
- Stock has price → shows colored price
- Source wired but no data yet → "⟳ Awaiting refresh" (italic muted)
- No free source for exchange → "No price source" (italic more muted)

`stock_search.has_price_source(stock)` is the single source of truth for this distinction.

## Relationship to frontier-monitor (private repo)

`~/AI/frontier-monitor/` is the author's personal fork. Kept in sync by copying files + sed-replacing branding:
- `emerging-edge` → `frontier-monitor`
- `emerging_edge` → `frontier_monitor`
- `Emerging Edge` → `FrontierViking`

FV runs on port 8877, has 23 stocks in `config.json`, uses viking-ship favicon from `digests/favicon.png`. EE runs on port 8878, empty config, globe emoji favicon (inline SVG).

## Dev conventions

- Use stdlib `urllib.request` + `ssl` tolerant context (many frontier sites have cert issues)
- Module-level `_*_TABLE_CACHE` dicts with 5-min TTL for shared-table scrapers (avoid N fetches for N stocks on same exchange)
- `_DEDICATED_RSS_DONE` set prevents refetching exchange-level RSS once per session
- `_is_fresh(db, table, ticker, hours)` checks before paid API calls
- All HTML rendered server-side; client JS is vanilla (no framework)
- Filter state persisted across page reloads via `window.location.hash = 'ex=...'` + `restoreExchange()` on load
- Stock layout toggle (grouped by exchange vs flat) persisted in `localStorage` as `ee-stock-grouped`

## Common gotchas

- **UTF-8 surrogate pairs in f-strings** — Use actual emoji chars, not `\uD83D\uDCC5` (Python can't encode surrogates)
- **UZSE press is Russian-language** — Google News locale should be `("ru", "UZ")` not `("en", "UZ")`
- **Nordic insider register** — Finansinspektionen is free, Serper not needed for OMX/OSE/CSE/HEL
- **stockanalysis.com covers most earnings** — Template scrapers run AFTER SA (line 652 in fetch_earnings) so exchange-specific source_url gets stored
- **UZSE page layout** — `/isu_infos/{ticker}?locale=en` works; `/en/listing/securities/` 404s
- **ISX table is RTL** — Cell indices flipped: close=cells[8], change%=cells[3]
- **Google News RSS snippets** — Raw HTML `<a href=...>` tags; must strip + unescape at fetch + render time
- **India NSE** — Yahoo has thin coverage; NSE site uses Cloudflare (hard to scrape)
- **No new catalog_updater needed for Yahoo-covered exchanges** — adding to `_YAHOO_TO_INTERNAL` + `_EXCHANGE_CURRENCY` is enough

## Engine room sections

Static master lists show ALL wired-up sources even without DB entries:
- **Prices**: 24 sources (Yahoo chart API × 5 major + 18 per-exchange custom scrapers)
- **News**: 7 sources (Yahoo RSS, Google News RSS, Iraq/Nairametrics/Ghana/AllAfrica RSS, Serper)
- **Earnings**: 22 sources (stockanalysis + NASDAQ calendar + 19 exchange-specific + Serper)
- **Forums**: 23 sources (i3investor, richbourse, 19 per-exchange free forums, Telegram, Serper)
- **Insiders**: grouped by source with SEC EDGAR, KLSE Screener, Finansinspektionen pre-categorized as FREE

DB counts merge in when data exists. Serper-sourced rows flagged as `💳 FULL`.

## Out of scope

- Multi-user support
- Real-time prices (all snapshots are polling-based)
- Options/derivatives
- International tax tracking in portfolio
- Any cloud/hosted service — self-hosted only
