import unittest
from unittest.mock import patch
import fetchers
from db import Database


class BistQuotesTests(unittest.TestCase):
    def test_dated_yahoo_precedes_delayed_sources_and_preserves_move(self):
        db = Database(':memory:')
        try:
            db.insert_price('ORGE', 'BIST', 20, 0, 'TRY', 'old', snapshot_date='2026-10-08')
            with patch.object(fetchers, '_fetch_price_yahoo', return_value=(22.66, -0.18, 'TRY')), patch.dict(fetchers._YAHOO_QUOTE_ASOF, {'ORGE.IS': '2026-10-09'}), patch.object(fetchers, '_fetch_price_stockanalysis') as sa:
                self.assertTrue(fetchers.fetch_prices({'ticker':'ORGE','exchange':'BIST'}, db, {}))
                sa.assert_not_called()
            row = db.get_latest_price('ORGE', 'BIST')
            self.assertEqual(row['snapshot_at'], '2026-10-09')
            self.assertEqual(row['change_pct'], -0.18)
            self.assertIsNotNone(row['fetched_at'])
        finally:
            db.conn.close()
