import unittest
from datetime import datetime
from unittest.mock import patch
import fetchers
from db import Database


class NgxTests(unittest.TestCase):
    def test_opening_delay_does_not_hide_truly_stale_quotes(self):
        self.assertTrue(fetchers.ngx_previous_close_expected('2026-10-05', datetime(2026, 10, 6, 8, 30)))
        self.assertTrue(fetchers.ngx_previous_close_expected('2026-10-05', datetime(2026, 10, 6, 10, 4)))
        self.assertFalse(fetchers.ngx_previous_close_expected('2026-10-05', datetime(2026, 10, 6, 10, 5)))
        self.assertFalse(fetchers.ngx_previous_close_expected('2026-10-02', datetime(2026, 10, 6, 8, 30)))
        self.assertFalse(fetchers.market_is_open('NGX', datetime(2026, 10, 6, 8, 29)))

    def test_absolute_move_uses_previous_close(self):
        day = datetime.utcnow().strftime('%Y-%m-%d')
        html = f'<time datetime={day}T11:00:00><table><th>Ticker<th>Name<th>Volume<th>Price<th>Change<tr><td><a href=/ngx/eti.html>ETI</a><td>Ecobank<td>123<td>110.00<td>+10.00</table>'
        self.assertEqual(fetchers._parse_ngx_quotes(html)['ETI'], (110, 10, 'NGN', day))
        self.assertEqual(fetchers._parse_ngx_quotes(html.replace('+10.00', '')), {})
        self.assertEqual(fetchers._parse_ngx_quotes(html.replace(day, '2020-01-01')), {})
        self.assertEqual(fetchers._parse_ngx_quotes(html.replace('datetime=', 'unknown=')), {})

    def test_ngn_cap_units(self):
        self.assertEqual(fetchers._parse_ngx_market_cap('market capitalization of NGN 1.27 trillion'), (1.27e12, 'NGN'))
        self.assertIsNone(fetchers._parse_ngx_market_cap('market capitalization of USD 1.27 billion'))

    def test_expired_quote_not_replayed_on_network_failure(self):
        with patch.object(fetchers, '_backfill_ft', return_value=None):
            self.assertIsNone(fetchers._fetch_ngx_quote('ETI'))

    def test_ft_uses_own_close_and_actual_date(self):
        with patch.object(fetchers, 'datetime', wraps=datetime) as clock, patch.object(fetchers, 'most_recent_completed_session', return_value='2026-10-02'), patch.object(fetchers, '_backfill_ft', return_value=[('2026-10-02', 68.2, 'NGN'), ('2026-10-05', 69.5, 'NGN')]):
            clock.utcnow.return_value = datetime(2026, 10, 5, 12)
            self.assertEqual(fetchers._fetch_ngx_quote('ETI'), (69.5, 1.91, 'NGN', '2026-10-05'))

    def test_ft_rejects_stale_future_wrong_currency_missing_prior(self):
        for rows in ([('2020-01-01', 1, 'NGN'), ('2020-01-02', 2, 'NGN')],
                     [('2099-01-01', 1, 'NGN'), ('2099-01-02', 2, 'NGN')],
                     [('2026-10-02', 1, 'USD'), ('2026-10-05', 2, 'USD')],
                     [('2026-10-05', 2, 'NGN')]):
            with patch.object(fetchers, '_backfill_ft', return_value=rows):
                self.assertIsNone(fetchers._fetch_ngx_quote('ETI'))

    def test_dated_session_move_survives_and_no_undated_fallback(self):
        db = Database(':memory:')
        stock = {'ticker': 'ETI', 'exchange': 'NGX'}
        try:
            db.insert_price('ETI', 'NGX', 110, 0, 'NGN', 'old', snapshot_date='2026-09-01')
            with patch.object(fetchers, '_fetch_ngx_quote', return_value=(110, 10, 'NGN', '2026-09-02')):
                self.assertTrue(fetchers.fetch_prices(stock, db, {}))
            row = db.get_latest_price('ETI', 'NGX')
            self.assertEqual(row['change_pct'], 10)
            self.assertEqual(row['snapshot_at'], '2026-09-02')
            self.assertIsNotNone(row['fetched_at'])
            with patch.object(fetchers, '_fetch_ngx_quote', return_value=None), patch.object(fetchers, '_fetch_price_stockanalysis') as fallback:
                self.assertFalse(fetchers.fetch_prices(stock, db, {}))
                fallback.assert_not_called()
        finally:
            db.conn.close()


if __name__ == '__main__':
    unittest.main()
