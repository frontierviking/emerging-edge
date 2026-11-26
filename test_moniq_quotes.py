import unittest
from unittest.mock import patch
import fetchers
from db import Database


class MoniqQuoteTests(unittest.TestCase):
    def test_move_uses_previous_published_close(self):
        with patch.object(fetchers, '_backfill_moniq', return_value=[
                ('2026-10-16',20000.01,'UZS'),('2026-10-19',19850,'UZS')]):
            self.assertEqual(fetchers._fetch_price_moniq({}),
                             (19850,-0.75,'UZS','2026-10-19'))
        with patch.object(fetchers, '_backfill_moniq', return_value=[('2026-10-19',19850,'UZS')]):
            self.assertIsNone(fetchers._fetch_price_moniq({}))

    def test_primary_dated_quote_and_no_undated_fallback(self):
        db=Database(':memory:')
        stock={'ticker':'UZTL','exchange':'UZSE'}
        try:
            with patch.object(fetchers,'_fetch_price_moniq',return_value=(19850,-0.75,'UZS','2026-10-19')), patch.object(fetchers,'_fetch_price_stockanalysis') as other:
                self.assertTrue(fetchers.fetch_prices(stock,db,{}))
                other.assert_not_called()
            row=db.get_latest_price('UZTL','UZSE')
            self.assertEqual(row['snapshot_at'],'2026-10-19')
            self.assertEqual(row['change_pct'],-0.75)
            for quote in [None,(20000.01,5.26,'UZS','2026-10-16')]:
                with patch.object(fetchers,'_fetch_price_moniq',return_value=quote), patch.object(fetchers,'_fetch_price_stockanalysis') as other:
                    self.assertFalse(fetchers.fetch_prices(stock,db,{}))
                    other.assert_not_called()
                self.assertEqual(db.get_latest_price('UZTL','UZSE')['price'],19850)
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
