"""
digest.py — Markdown digest generator for emerging-edge.

Reads today's data from the SQLite database and produces a structured
markdown file with sections:

  🔴 Urgent      — contract wins, earnings surprises, price moves >5%
  📰 News        — all new articles (French kept as-is)
  📅 Upcoming    — earnings dates in next 30 days
  💬 Forum buzz  — forum comments by exchange/source

French content is NEVER translated — it's included as-is per user requirement.
"""

import os
from datetime import datetime, timedelta

from db import Database


def generate_digest(db: Database, config: dict, target_date: str = None) -> str:
    """
    Build a markdown digest string for the given date (default: today).
    Returns the full markdown content.
    """
    if target_date is None:
        target_date = datetime.utcnow().strftime("%Y-%m-%d")

    # "Since" threshold: start of the target day (UTC)
    since = f"{target_date}T00:00:00Z"

    # Load data for the day
    news = db.get_news_since(since)
    contracts = db.get_contracts_since(since)
    earnings = db.get_upcoming_earnings(within_days=30)
    forum = db.get_forum_since(since)

    # Build stock lookup for nice names
    stock_map = {}
    for s in config.get("stocks", []):
        stock_map[s["ticker"]] = s

    lines = []
    lines.append(f"# 📊 Emerging Edge — Daily Digest {target_date}")
    lines.append("")
    lines.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")

    # ------------------------------------------------------------------
    # 🔴 URGENT
    # ------------------------------------------------------------------
    lines.append("---")
    lines.append("## 🔴 Urgent")
    lines.append("")

    urgent_items = []

    # Contract wins are always urgent
    for c in contracts:
        title = c.get("title", "No title")
        ticker = c.get("ticker", "")
        sname = stock_map.get(ticker, {}).get("name", ticker)
        url = c.get("url", "")
        urgent_items.append(
            f"- **{sname} ({ticker})** — Contract/Tender: [{title}]({url})"
        )

    # Price moves >5% (from today's snapshots)
    for s in config.get("stocks", []):
        price_data = db.get_latest_price(s["ticker"], s["exchange"])
        if price_data and price_data.get("change_pct") is not None:
            pct = price_data["change_pct"]
            if abs(pct) >= 5.0:
                direction = "📈" if pct > 0 else "📉"
                urgent_items.append(
                    f"- {direction} **{s['name']} ({s['ticker']})** — "
                    f"Price move {pct:+.1f}% "
                    f"({price_data.get('currency', '')} {price_data.get('price', 'N/A')})"
                )

    if urgent_items:
        lines.extend(urgent_items)
    else:
        lines.append("_No urgent items today._")
    lines.append("")

    # ------------------------------------------------------------------
    # 📰 NEWS
    # ------------------------------------------------------------------
    lines.append("---")
    lines.append("## 📰 News")
    lines.append("")

    if news:
        # Group by exchange
        by_exchange: dict[str, list] = {}
        for n in news:
            ex = n.get("exchange", "Other")
            by_exchange.setdefault(ex, []).append(n)

        for exchange, items in sorted(by_exchange.items()):
            lines.append(f"### {exchange}")
            lines.append("")
            for n in items:
                ticker = n.get("ticker", "")
                sname = stock_map.get(ticker, {}).get("name", ticker)
                title = n.get("title", "No title")
                url = n.get("url", "")
                snippet = n.get("snippet", "")
                source = n.get("source", "")
                pub = n.get("published", "")
                lang_tag = f" 🇫🇷" if n.get("lang") == "fr" else ""

                lines.append(f"- **{sname} ({ticker})**{lang_tag}")
                lines.append(f"  [{title}]({url})")
                if snippet:
                    lines.append(f"  > {snippet[:200]}")
                if source or pub:
                    lines.append(f"  _Source: {source} | {pub}_")
                lines.append("")
    else:
        lines.append("_No new articles today._")
    lines.append("")

    # ------------------------------------------------------------------
    # 📅 UPCOMING EARNINGS
    # ------------------------------------------------------------------
    lines.append("---")
    lines.append("## 📅 Upcoming (next 30 days)")
    lines.append("")

    if earnings:
        lines.append("| Stock | Exchange | Report Date | Period | Source |")
        lines.append("|-------|----------|-------------|--------|--------|")
        for e in earnings:
            ticker = e.get("ticker", "")
            sname = stock_map.get(ticker, {}).get("name", ticker)
            rdate = e.get("report_date", "TBD")
            period = e.get("fiscal_period", "")
            src = e.get("source_url", "")
            ex = e.get("exchange", "")
            # Calculate days until
            try:
                dt = datetime.strptime(rdate, "%Y-%m-%d")
                days_left = (dt - datetime.now()).days
                rdate_display = f"{rdate} ({days_left}d)"
            except ValueError:
                rdate_display = rdate
            lines.append(f"| {sname} ({ticker}) | {ex} | {rdate_display} | {period} | [link]({src}) |")
    else:
        lines.append("_No upcoming earnings dates found._")
    lines.append("")

    # ------------------------------------------------------------------
    # 💬 FORUM BUZZ
    # ------------------------------------------------------------------
    lines.append("---")
    lines.append("## 💬 Forum Buzz")
    lines.append("")

    if forum:
        # Group by forum source
        by_forum: dict[str, list] = {}
        for f in forum:
            fname = f.get("forum", "other")
            by_forum.setdefault(fname, []).append(f)

        for forum_name, items in sorted(by_forum.items()):
            lines.append(f"### {forum_name}")
            lines.append("")
            for f in items[:10]:  # cap display at 10 per forum
                ticker = f.get("ticker", "")
                sname = stock_map.get(ticker, {}).get("name", ticker)
                author = f.get("author", "Anonymous")
                text = f.get("text", "")[:300]  # truncate for readability
                post_url = f.get("post_url", "")
                lang_tag = " 🇫🇷" if f.get("lang") == "fr" else ""

                lines.append(f"- **{sname} ({ticker})**{lang_tag} — _{author}_")
                lines.append(f"  > {text}")
                if post_url:
                    lines.append(f"  [Source]({post_url})")
                lines.append("")
    else:
        lines.append("_No forum mentions today._")
    lines.append("")

    # ------------------------------------------------------------------
    # Footer
    # ------------------------------------------------------------------
    lines.append("---")
    lines.append(f"_Emerging Edge v1.0 | {len(config.get('stocks', []))} stocks tracked_")

    return "\n".join(lines)


def save_digest(db: Database, config: dict, target_date: str = None) -> str:
    """
    Generate and write the digest to a markdown file.
    Returns the file path.
    """
    if target_date is None:
        target_date = datetime.utcnow().strftime("%Y-%m-%d")

    digest_dir = config.get("digest_dir", "./digests")
    os.makedirs(digest_dir, exist_ok=True)

    content = generate_digest(db, config, target_date)
    filename = f"daily_{target_date}.md"
    filepath = os.path.join(digest_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


def print_upcoming(db: Database, config: dict):
    """Print a formatted earnings calendar to stdout."""
    earnings = db.get_all_earnings()
    stock_map = {s["ticker"]: s for s in config.get("stocks", [])}

    print("\n📅 Earnings Calendar — All Known Dates")
    print("=" * 65)

    if not earnings:
        print("  No earnings dates stored yet. Run 'python monitor.py run' first.")
        return

    for e in earnings:
        ticker = e.get("ticker", "")
        sname = stock_map.get(ticker, {}).get("name", ticker)
        rdate = e.get("report_date", "TBD")
        period = e.get("fiscal_period", "")
        ex = e.get("exchange", "")

        # Days until
        try:
            dt = datetime.strptime(rdate, "%Y-%m-%d")
            delta = (dt - datetime.now()).days
            if delta < 0:
                tag = f"({abs(delta)}d ago)"
            elif delta == 0:
                tag = "(TODAY)"
            else:
                tag = f"(in {delta}d)"
        except ValueError:
            tag = ""

        print(f"  {rdate:12s} {tag:12s}  {sname} ({ticker}) [{ex}]  {period}")

    print()
