import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import fetchers


class YahooQuoteDateTests(unittest.TestCase):
    def test_stale_metadata_rejected_despite_current_chart_bar(self):
        payload = {'chart': {'result': [{
            'meta': {'regularMarketTime': 1721764800,
                     'regularMarketPrice': 2.46, 'currency': 'PLN',
                     'exchangeTimezoneName': 'Europe/Warsaw'},
            'timestamp': [int(datetime.now(timezone.utc).timestamp())],
            'indicators': {'quote': [{'close': [None]}]},
        }]}}
        response = MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(payload).encode()
        with patch('urllib.request.urlopen', return_value=response), \
                patch.object(fetchers, '_yahoo_circuit_open', return_value=False):
            fetchers._YAHOO_QUOTE_ASOF['TRX.WA'] = '2026-10-02'
            self.assertIsNone(fetchers._fetch_price_yahoo('TRX.WA', exchange='WSE'))
            self.assertNotIn('TRX.WA', fetchers._YAHOO_QUOTE_ASOF)

    def test_missing_timestamp_rejected(self):
        self.assertIsNone(fetchers._yahoo_quote_date({}, 'WSE'))

    def test_prior_session_allowed_when_closed(self):
        meta = {'regularMarketTime': datetime(2026, 10, 1, 14, tzinfo=timezone.utc).timestamp(),
                'exchangeTimezoneName': 'Europe/Warsaw'}
        with patch.object(fetchers, 'market_is_open', return_value=False), \
                patch.object(fetchers, 'most_recent_completed_session', return_value='2026-10-01'):
            self.assertEqual(fetchers._yahoo_quote_date(meta, 'WSE'), '2026-10-01')

    def test_polish_quote_uses_dated_fields(self):
        html = '''<span class="q_ch_act">4.80</span>
        <span class="q_ch_per cplus">(+3.00%)</span>
        <time class="q_ch_date" datetime="2026-10-02T16:22:56+0200">16:22</time>'''
        with patch.object(fetchers, '_fetch_page_text', return_value=html), \
                patch.object(fetchers, '_yahoo_quote_date', return_value='2026-10-02'):
            self.assertEqual(fetchers._fetch_price_trex_poland(), (4.8, 3.0, 'PLN', '2026-10-02'))
        with patch.object(fetchers, '_fetch_page_text', return_value=html), \
                patch.object(fetchers, '_yahoo_quote_date', return_value=None):
            self.assertIsNone(fetchers._fetch_price_trex_poland())


if __name__ == '__main__':
    unittest.main()
