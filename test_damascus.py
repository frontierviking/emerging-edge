import unittest
from datetime import date
from unittest.mock import patch
from damascus import parse_directory, parse_quote
from db import Database
from fetchers import fetch_prices
from stock_search import search_catalog, has_price_source


QUOTE = '''<tr><th>unrelated</th><th>headers</th></tr>
<tr><th>آخر تحديث</th><td>2026-10-14 13:28</td></tr>
<tr><th>سعر الإغلاق</th><td>18.05</td></tr>
<tr><th>نسبة تغير سعر الإغلاق</th><td>-1.25 %</td></tr>'''


class DamascusTests(unittest.TestCase):
    def test_dated_quote_not_homepage_clock(self):
        self.assertEqual(parse_quote(QUOTE, date(2026,10,14)),
                         (18.05, -1.25, 'SYP', '2026-10-14'))
        self.assertIsNone(parse_quote(QUOTE, date(2026,10,13)))
        self.assertIsNone(parse_quote(QUOTE.replace('آخر تحديث','missing')))
        self.assertIsNone(parse_quote(QUOTE.replace('18.05','NaN')))

    def test_directory_ignores_undated_ticker_strip(self):
        page = '''<a href="/issuer-company/view/4"><table>BBSF 18.05</table></a>
        <a href="/issuer-company/view/4"><div class="CompanyName">Banque Bemo Saudi Fransi</div>
        <div class="company-symbol"> BBSF </div></a>'''
        rows = parse_directory(page)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['exchange'], 'DSES')
        self.assertEqual(rows[0]['currency'], 'SYP')

    def test_catalog_search(self):
        rows = search_catalog('Damascus', limit=50)
        self.assertEqual(len(rows), 27)
        self.assertTrue(all(has_price_source(row) for row in rows))
        self.assertTrue(any(r['ticker']=='SYTEL' for r in rows))

    def test_quote_failure_and_older_response_do_not_fall_back(self):
        db = Database(':memory:')
        stock = {'ticker':'BBSF','exchange':'DSES','price_url':'https://dse.sy/issuer-company/view/4'}
        try:
            db.insert_price('BBSF','DSES',20,1,'SYP','',snapshot_date='2026-10-14')
            for result in [None, (18.05,0,'SYP','2026-10-13')]:
                with patch('damascus.fetch_quote',return_value=result):
                    self.assertFalse(fetch_prices(stock,db,{}))
            self.assertEqual(db.get_latest_price('BBSF','DSES')['price'],20)
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
