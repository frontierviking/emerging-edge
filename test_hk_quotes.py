import unittest
from unittest.mock import patch
from db import Database
import fetchers as f


class HongKongQuotes(unittest.TestCase):
    def body(self, **overrides):
        fields = ['0'] * 78
        values = {2:'01420', 3:'0.275', 4:'0.280', 30:'2026/11/03 13:12:26',
                  32:'-1.79', 75:'HKD'}
        values.update({int(k):v for k,v in overrides.items()})
        for k,v in values.items():
            fields[k] = v
        return 'v_hk01420="' + '~'.join(fields) + '";'

    def test_valid_and_invalid_quotes(self):
        with patch.object(f, 'most_recent_completed_session', return_value='2026-11-02'):
            self.assertEqual(f._parse_hk_tencent_quote(self.body(), '1420'),
                             (0.275, -1.79, 'HKD', '2026-11-03'))
            for changes in [{'2':'09998'}, {'75':'USD'}, {'3':'nan'}, {'3':'0'},
                            {'32':'50'}, {'30':'2999/11/03 13:12:26'},
                            {'30':'2026/10/30 13:12:26'}]:
                self.assertIsNone(f._parse_hk_tencent_quote(self.body(**changes), '1420'))

    def test_preserve_vendor_move_and_do_not_regress_date(self):
        db = Database(':memory:')
        self.addCleanup(db.close)
        s = {'ticker':'1420','exchange':'HKSE'}
        db.insert_price('1420','HKSE',0.2,0,'HKD','old',snapshot_date='2026-11-02')
        with patch.object(f, '_fetch_price_hk_tencent', return_value=(0.275,-1.79,'HKD','2026-11-03')):
            self.assertTrue(f.fetch_prices(s,db,{}))
        self.assertEqual(db.get_latest_price('1420','HKSE')['change_pct'], -1.79)
        with patch.object(f, '_fetch_price_hk_tencent', return_value=(0.28,0,'HKD','2026-11-02')):
            self.assertFalse(f.fetch_prices(s,db,{}))
