import json
import unittest
from unittest.mock import patch
import fetchers as f


class NaverCapTests(unittest.TestCase):
    def quote(self, value='416억', code='059100', day='2026-10-30'):
        return [json.dumps({'itemCode': code, 'totalInfos': [
            {'code': 'marketValue', 'value': value}]}),
            json.dumps({'itemCode': code, 'localTradedAt': day + 'T15:30:00+09:00'})]

    def test_units_and_date(self):
        for raw, expected in [('416억', 41.6e9), ('1조 2,345억', 1.2345e12), ('2.5조', 2.5e12)]:
            with patch.object(f, '_fetch_page_text', side_effect=self.quote(raw)):
                self.assertEqual(f.fetch_market_cap({'ticker':'059100','exchange':'KRX'}),
                                 (expected, 'KRW', '2026-10-30'))

    def test_reject_bad_identity_units_date(self):
        for values in [self.quote(code='123456'), self.quote(day='2999-01-01'),
                       self.quote('416'), self.quote('0억'), self.quote('N/A')]:
            with patch.object(f, '_fetch_page_text', side_effect=values):
                self.assertIsNone(f._fetch_market_cap_naver({'ticker':'059100','exchange':'KRX'}))


if __name__ == '__main__':
    unittest.main()
