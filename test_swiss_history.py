import json
import unittest
from unittest.mock import patch, mock_open
import fetchers
from stock_search import derive_yahoo_ticker, has_price_source


class SwissHistoryTests(unittest.TestCase):
    def test_swiss_symbol_without_explicit_yahoo_mapping(self):
        self.assertEqual(derive_yahoo_ticker('VETN','SWX'),'VETN.SW')
        self.assertTrue(has_price_source({'ticker':'STGN','exchange':'SWX'}))

    def test_yahoo_history_uses_minimal_headers(self):
        payload={'chart':{'result':[{'meta':{'currency':'CHF'},
            'timestamp':[1792713600,1792800000],
            'indicators':{'quote':[{'close':[18.2,18.4]}]}}]}}
        with patch.object(fetchers,'_yahoo_circuit_open',return_value=True), \
             patch.object(fetchers,'_CURL_BIN','/usr/bin/curl'), \
             patch('subprocess.check_output',return_value=b'200') as request, \
             patch('builtins.open',mock_open(read_data=json.dumps(payload))):
            rows=fetchers._backfill_yahoo('VETN.SW')
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[-1][1:],(18.4,'CHF'))
        cmd=request.call_args.args[0]
        self.assertEqual(cmd[cmd.index('-A')+1],'Mozilla/5.0')
        self.assertFalse(any('Origin:' in arg or 'Referer:' in arg for arg in cmd))


if __name__ == '__main__':
    unittest.main()
