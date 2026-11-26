import html
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import fetchers as f
from db import Database


class AfricanQuotesTests(unittest.TestCase):
    def page(self, market='open', stamp=None, price='33.15'):
        data = {'market': market, 'stamp': stamp or datetime.now(timezone.utc).timestamp(),
                'data': [price, '0.10 (0.30%)', '33.25']}
        return ('<link rel=canonical href="https://live.mystocks.co.ke/stock=SCOM" />'
                '<div id=rtTime>End of day - Oct 29, 2026</div>'
                '<div id=rtPrice>33.25</div><b id=rtPrev>33.70</b>'
                '<div id=rtDataJson style=display:none>' + html.escape(json.dumps(data)) + '</div>')

    def test_live_kenya_price_and_signed_change(self):
        with patch.object(f, '_fetch_page_text', return_value=self.page()):
            q = f._fetch_price_mystocks({'ticker': 'SCOM'})
        self.assertEqual(q[:3], (33.15, -0.30, 'KES'))

    def test_closed_quote_uses_close_date_not_request_stamp(self):
        with patch.object(f, '_fetch_page_text', return_value=self.page('closed')):
            self.assertEqual(f._fetch_price_mystocks({'ticker': 'SCOM'}),
                             (33.25, -1.34, 'KES', '2026-10-29'))

    def test_invalid_identity_timestamp_and_price(self):
        for page in [self.page().replace('stock=SCOM', 'stock=KCB'),
                     self.page(stamp=1), self.page(stamp=99999999999),
                     self.page(price='NaN'), self.page(price='0')]:
            with patch.object(f, '_fetch_page_text', return_value=page):
                self.assertIsNone(f._fetch_price_mystocks({'ticker': 'SCOM'}))

    def test_kenya_fallback_and_no_date_regression(self):
        db = Database(':memory:')
        self.addCleanup(db.close)
        with patch.object(f, '_fetch_price_mystocks', return_value=None), \
             patch.object(f, '_backfill_afx_nse', return_value=[
                 ('2026-10-28', 33.7, 'KES'), ('2026-10-29', 33.25, 'KES')]):
            self.assertTrue(f.fetch_prices({'ticker': 'SCOM', 'exchange': 'NSEK'}, db, {}))
            row = db.get_latest_price('SCOM', 'NSEK')
            self.assertEqual(row['snapshot_at'], '2026-10-29')
            self.assertEqual(row['change_pct'], -1.34)
            db.insert_price('SCOM', 'NSEK', 33.15, -0.3, 'KES', 'new', snapshot_date='2026-10-30')
            self.assertFalse(f.fetch_prices({'ticker': 'SCOM', 'exchange': 'NSEK'}, db, {}))

    def test_jse_open_and_prior_close_delay(self):
        self.assertFalse(f.market_is_open('JSE', datetime(2026, 10, 30, 6, 50)))
        self.assertTrue(f.market_is_open('JSE', datetime(2026, 10, 30, 7, 0)))
        self.assertTrue(f.jse_previous_close_expected('2026-10-29', datetime(2026, 10, 30, 7, 10)))
        self.assertFalse(f.jse_previous_close_expected('2026-10-28', datetime(2026, 10, 30, 6, 50)))
        self.assertFalse(f.jse_previous_close_expected('2026-10-29', datetime(2026, 10, 30, 7, 21)))

    def test_jse_ft_fallback_preserves_cents_date_move(self):
        db = Database(':memory:')
        self.addCleanup(db.close)
        with patch.object(f, '_fetch_price_yahoo', return_value=None), \
             patch.object(f, '_fetch_price_ft', return_value=(42.8, 1.42, 'ZAR', '2026-10-29')):
            self.assertTrue(f.fetch_prices({'ticker': 'ART', 'exchange': 'JSE'}, db, {}))
        row = db.get_latest_price('ART', 'JSE')
        self.assertAlmostEqual(row['price'], 4280)
        self.assertEqual(row['currency'], 'ZAc')
        self.assertEqual(row['snapshot_at'], '2026-10-29')
        self.assertEqual(row['change_pct'], 1.42)


if __name__ == '__main__':
    unittest.main()
