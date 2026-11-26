"""Official Damascus directory and dated quotes (DSES, not Dhaka/Dar es Salaam).

Use the apex hostname: www.dse.sy has a mismatched TLS certificate.
Never use the undated homepage ticker or infer an SYP/USD conversion.
"""
import html
import http.cookiejar
import math
import re
import urllib.request
from datetime import datetime, timezone

BASE = 'https://dse.sy'


def text(value):
    return ' '.join(html.unescape(re.sub(r'<[^>]+>', ' ', value)).split())


def get(url, opener=None):
    request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with (opener.open(request, timeout=20) if opener else
          urllib.request.urlopen(request, timeout=20)) as response:
        return response.read().decode('utf-8')


def parse_directory(page):
    rows = {}
    for identifier, body in re.findall(
            r'<a\s+href="/issuer-company/view/(\d+)"[^>]*>(.*?)</a>', page, re.S):
        name = re.search(r'class="CompanyName"[^>]*>(.*?)</div>', body, re.S)
        symbol = re.search(r'class="company-symbol"[^>]*>(.*?)</div>', body, re.S)
        if not name or not symbol:
            continue
        ticker = text(symbol.group(1)).upper()
        if not re.fullmatch(r'[A-Z][A-Z0-9.-]*', ticker):
            continue
        rows[ticker] = {'ticker': ticker, 'exchange': 'DSES',
            'name': text(name.group(1)), 'currency': 'SYP', 'country': 'Syria',
            'lang': 'ar', 'code': ticker, 'forum_sources': [], 'earnings_source': '',
            'notes': '', 'aliases': ['Damascus Securities Exchange', 'Syria'],
            'price_url': BASE + '/issuer-company/view/' + identifier}
    return list(rows.values())


def fetch_directory():
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    get(BASE + '/index/lang/en', opener)
    entries = parse_directory(get(BASE + '/issuer-company/index', opener))
    if not entries:
        raise ValueError('No Damascus issuers found; retaining existing catalogue')
    return entries


def parse_quote(page, today=None):
    fields = {}
    for label, value in re.findall(r'<th[^>]*>([^<]*)</th>\s*<td[^>]*>(.*?)</td>', page, re.S):
        fields[text(label)] = text(value)
    try:
        stamp = fields['آخر تحديث']
        day = datetime.strptime(stamp[:10], '%Y-%m-%d').date()
        today = today or datetime.now(timezone.utc).date()
        if day > today:
            return None
        price = float(fields['سعر الإغلاق'].replace(',', ''))
        change = float(fields['نسبة تغير سعر الإغلاق'].replace('%', '').replace(',', '').strip())
        if not math.isfinite(price) or price <= 0 or not math.isfinite(change):
            return None
        return price, change, 'SYP', day.isoformat()
    except (KeyError, ValueError):
        return None


def fetch_quote(stock):
    url = stock.get('price_url', '')
    if not re.fullmatch(r'https://dse\.sy/issuer-company/view/\d+', url):
        return None
    return parse_quote(get(url))
