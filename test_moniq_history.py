import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import fetchers


class MoniqHistoryTests(unittest.TestCase):
    def test_public_api_and_nested_bars(self):
        now = datetime.now(timezone.utc)
        response = MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps({
            'data': {'currency': 'UZS', 'bars': [
                {'time': now.timestamp() * 1000, 'close': '400001'},
                {'time': now.timestamp() * 1000, 'close': 'NaN'},
            ]}}).encode()
        with patch('urllib.request.urlopen', return_value=response) as request:
            result = fetchers._backfill_moniq({'ticker': 'UTYK', 'exchange': 'UZSE'})
        self.assertEqual(result, [(now.date().isoformat(), 400001.0, 'UZS')])
        self.assertEqual(request.call_args.args[0].full_url,
                         'https://moniq.uz/api/v1/stocks/UTYK/bars?lang=en')

    def test_html_is_not_history(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'<html>login</html>'
        with patch('urllib.request.urlopen', return_value=response):
            self.assertIsNone(fetchers._backfill_moniq({'ticker': 'UTYK', 'exchange': 'UZSE'}))


if __name__ == '__main__':
    unittest.main()
