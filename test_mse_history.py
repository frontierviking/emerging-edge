import html
import json
import unittest
from datetime import datetime
from unittest.mock import patch
from fetchers import _backfill_mse, backfill_price_history
from db import Database


class MSEHistoryTests(unittest.TestCase):
    def test_official_chart_filters_wrong_security_and_bad_prices(self):
        day=datetime.utcnow().strftime('%Y-%m-%d')
        row={'companycode':547,'Symbol':'MNDL-O-0000','MDSubOrderBookType':'Regular',
             'dates':day,'ClosingPrice':84.99}
        rows=[row,dict(row,Symbol='OTHER-O-0000',ClosingPrice=999),
              dict(row,MDSubOrderBookType='Block',ClosingPrice=888),
              dict(row,companycode=999),dict(row,ClosingPrice=0),
              dict(row,ClosingPrice='NaN'),dict(row,dates='2999-01-01')]
        page='<div data-trading-histories="'+html.escape(json.dumps(rows),quote=True)+'"></div>'
        stock={'ticker':'MNDL','exchange':'MSE','price_url':'https://open.mse.mn/securities/547'}
        with patch('fetchers._fetch_page_text',return_value=page):
            self.assertEqual(_backfill_mse(stock),[(day,84.99,'MNT')])
            db=Database(':memory:')
            try:
                self.assertEqual(backfill_price_history(stock,db)['inserted'],1)
                self.assertEqual(db.get_latest_price('MNDL','MSE')['source_url'],stock['price_url']+'/tab/tradeinfo')
            finally:
                db.close()

    def test_missing_chart_fails_cleanly(self):
        with patch('fetchers._fetch_page_text',return_value='<html>unavailable</html>'):
            self.assertIsNone(_backfill_mse({'ticker':'MNDL','price_url':'https://open.mse.mn/securities/547'}))


if __name__ == '__main__':
    unittest.main()
