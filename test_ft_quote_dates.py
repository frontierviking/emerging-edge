import unittest
from contextlib import ExitStack
from datetime import date
from unittest.mock import patch
import fetchers
import market_calendar
from db import Database


class FTDateTests(unittest.TestCase):
    def test_hong_kong_holiday(self):
        self.assertTrue(market_calendar.is_holiday('HKSE', date(2026, 10, 19)))

    def test_ft_preserves_date_and_requires_two_valid_closes(self):
        rows = [('2026-10-15', 25.06, 'HKD'), ('2026-10-16', 26.14, 'HKD')]
        with patch.object(fetchers, '_backfill_ft', return_value=rows):
            self.assertEqual(fetchers._fetch_price_ft({}), (26.14, 4.31, 'HKD', '2026-10-16'))
        for invalid in [rows[:1], rows + [('2999-01-01', 30, 'HKD')],
                        [rows[0], ('2026-10-16', float('nan'), 'HKD')]]:
            with patch.object(fetchers, '_backfill_ft', return_value=invalid):
                self.assertIsNone(fetchers._fetch_price_ft({}))

    def test_both_ft_paths_store_source_date_and_move(self):
        for exchange in ['HKSE', 'ASX']:
            with self.subTest(exchange=exchange), ExitStack() as stack:
                db = Database(':memory:')
                stack.callback(db.close)
                db.insert_price('TEST',exchange,26.14,0,'HKD','old',snapshot_date='2026-10-16')
                stack.enter_context(patch.object(fetchers, 'market_closed_and_current', return_value=False))
                for name in ['_fetch_price_hk_tencent','_fetch_price_stockanalysis','_fetch_price_googlefinance',
                             '_fetch_price_yahoo','_fetch_price_scrape']:
                    stack.enter_context(patch.object(fetchers,name,return_value=None))
                stack.enter_context(patch.object(fetchers,'_fetch_price_ft',return_value=(26.14,4.31,'HKD','2026-10-16')))
                self.assertTrue(fetchers.fetch_prices({'ticker':'TEST','exchange':exchange},db,{}))
                row=db.get_latest_price('TEST',exchange)
                self.assertEqual(row['snapshot_at'],'2026-10-16')
                self.assertEqual(row['change_pct'],4.31)
                self.assertEqual(db.conn.execute('SELECT count(*) FROM price_snapshots').fetchone()[0],1)
                db.insert_price('TEST',exchange,27,1,'HKD','new',snapshot_date='2026-10-19')
                self.assertFalse(fetchers.fetch_prices({'ticker':'TEST','exchange':exchange},db,{}))
                self.assertEqual(db.get_latest_price('TEST',exchange)['price'],27)


if __name__ == '__main__':
    unittest.main()
