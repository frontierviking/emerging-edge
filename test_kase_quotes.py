import unittest
from unittest.mock import patch

import fetchers
from db import Database


class KaseQuotesTests(unittest.TestCase):
    def test_official_move_survives_database_write(self):
        db = Database(':memory:')
        try:
            db.insert_price('KASE', 'KASE', 110898, 2.68, 'KZT', 'old',
                            snapshot_date='2026-09-28')
            db.insert_price('KASE', 'KASE', 110500, -0.90, 'KZT',
                            'https://kase.kz/en/shares/')
            self.assertEqual(db.get_latest_price('KASE', 'KASE')['change_pct'], -0.90)
        finally:
            db.conn.close()

    def test_trading_last_not_reference_bid_or_vwap(self):
        html = '''<table><tr><th>Ticker</th><th>Price, KZT</th></tr>
        <tr><td>KASE</td><td>109 499,99</td></tr></table>
        <table><tr><th>Mode</th><th>Last</th><th>Ticker</th>
        <th>Bid</th><th>Price change per day, %</th><th>VWAP</th></tr>
        <tr><td>EQBR</td><td>110&nbsp;500,00</td><td>KASE</td>
        <td>111 481,00</td><td>-0,90</td><td>110 246,84</td></tr>
        <tr><td>OTHER</td><td>999,00</td><td>KASE</td>
        <td>1</td><td>0</td><td>1</td></tr></table>'''
        self.assertEqual(fetchers._parse_kase_shares_table(html),
                         {'KASE': (110500.0, -0.9)})

    def test_unknown_layout_fails_closed(self):
        self.assertEqual(fetchers._parse_kase_shares_table(
            '<table><tr><td>KASE</td><td>123</td></tr></table>'), {})

    def test_expired_cache_not_replayed_on_failure(self):
        with patch.dict(fetchers._KASE_TABLE_CACHE,
                        {'ts': 0, 'data': {'KASE': (1, 0)}}), \
                patch('urllib.request.urlopen', side_effect=OSError('offline')):
            self.assertEqual(fetchers._kase_shares_table(), {})


if __name__ == '__main__':
    unittest.main()
