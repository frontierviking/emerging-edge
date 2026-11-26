import unittest
from unittest.mock import patch
from db import Database
import fetchers


class MarketCapTests(unittest.TestCase):
    def test_new_listing_bypasses_other_listing_cooldown(self):
        from types import SimpleNamespace
        import threading
        db = SimpleNamespace(_db_path='/tmp/market-cap-test.db')
        old = {'ticker':'OLD', 'exchange':'BIST'}
        new = {'ticker':'RYSAS', 'exchange':'BIST'}
        with patch.object(fetchers, '_MCAP_REFRESH_LOCK', threading.Lock()), \
             patch.object(fetchers, '_MCAP_REFRESH_ATTEMPTS', {}), \
             patch.object(fetchers._threading, 'Thread') as thread:
            fetchers.schedule_market_cap_refresh(db, [old])
            fetchers._MCAP_REFRESH_LOCK.release()  # Simulate the worker finishing.
            fetchers.schedule_market_cap_refresh(db, [old])
            self.assertEqual(thread.call_count, 1)
            fetchers.schedule_market_cap_refresh(db, [old, new])
            self.assertEqual(thread.call_count, 2)
            fetchers._MCAP_REFRESH_LOCK.release()

    def test_uncovered_exchange_reaches_fallback(self):
        with patch.object(fetchers,'_sa_list_meta',return_value=None), \
             patch.object(fetchers,'_sa_slug_for',return_value=None), \
             patch.object(fetchers,'_market_cap_fallbacks',return_value=(123,'CHF')) as fallback:
            self.assertEqual(fetchers.fetch_market_cap({'ticker':'TEST','exchange':'SWX'}),(123,'CHF'))
            fallback.assert_called_once()

    def test_ft_explicit_currency_date_and_identity(self):
        page='<title>Vetropack, VETN:SWX summary</title><th>Market cap</th><td>249.31m <span>CHF</span></td><div>Data delayed at least 15 minutes, as of Oct 26 2026 16:30 BST.</div>'
        with patch.object(fetchers,'_fetch_page_text',return_value=page):
            self.assertEqual(fetchers._fetch_market_cap_ft({'ticker':'VETN','exchange':'SWX'}),(249310000,'CHF','2026-10-26'))
            self.assertIsNone(fetchers._fetch_market_cap_ft({'ticker':'STGN','exchange':'SWX'}))

    def test_mmi_preserves_session_date(self):
        page='https://mselive.site/stocks/QPAY Зах зээлийн үнэлгээ 93.3 тэрбум₮ <span>Сүүлийн арилжаа <!-- -->2026-10-27</span>'
        with patch.object(fetchers,'_fetch_page_text',return_value=page):
            self.assertEqual(fetchers._fetch_market_cap_mmi({'ticker':'QPAY'}),(93300000000,'MNT','2026-10-27'))

    def setUp(self):
        self.db = Database(':memory:')

    def tearDown(self):
        self.db.close()

    def test_khan_revalues_on_each_quote_without_network(self):
        stock = {'ticker': 'KHAN', 'exchange': 'MSE'}
        for price, day in [(1630, '2026-08-29'), (2093, '2026-10-13')]:
            self.db.insert_price('KHAN', 'MSE', price, 0, 'MNT', '', snapshot_date=day)
            with patch.object(fetchers, 'fetch_market_cap', side_effect=AssertionError('network')):
                self.assertEqual(fetchers.refresh_market_caps(self.db, [stock]), 1)
            row = self.db.conn.execute('SELECT * FROM stock_fundamentals').fetchone()
            self.assertEqual(row['market_cap'], price * 1912198000)
            self.assertEqual(row['market_cap_at'], day)
            self.assertEqual(row['market_cap_ccy'], 'MNT')

    def test_no_inferred_shares_and_failed_vendor_preserves_date(self):
        self.db.conn.execute('''INSERT INTO stock_fundamentals
            (ticker,exchange,market_cap,market_cap_ccy,market_cap_at,updated_at)
            VALUES ('TEST','TEST',100,'USD','2026-01-01','2026-01-01')''')
        with patch.object(fetchers, 'fetch_market_cap', return_value=None):
            fetchers.refresh_market_caps(self.db, [{'ticker': 'TEST', 'exchange': 'TEST'}])
        row = self.db.conn.execute('SELECT * FROM stock_fundamentals').fetchone()
        self.assertEqual(row['market_cap'], 100)
        self.assertEqual(row['market_cap_at'], '2026-01-01')

    def test_pence_scaling(self):
        self.db.conn.execute('''INSERT INTO price_snapshots
            (ticker,exchange,price,currency,snapshot_at)
            VALUES ('TEST','LSE',250,'GBX','2026-10-13')''')
        fetchers.revalue_market_caps(self.db, [{'ticker':'TEST', 'exchange':'LSE',
                                             'shares_outstanding':1000}])
        row = self.db.conn.execute('SELECT * FROM stock_fundamentals').fetchone()
        self.assertEqual(row['market_cap'], 2500)
        self.assertEqual(row['market_cap_ccy'], 'GBP')


if __name__ == '__main__':
    unittest.main()
