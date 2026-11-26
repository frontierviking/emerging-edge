import os
import tempfile
import unittest
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from db import Database


class DatabaseLockTests(unittest.TestCase):
    def test_no_implicit_lock_survives_an_uncommitted_write_or_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path=os.path.join(directory,'test.db')
            a=Database.open_connection(path)
            b=Database.open_connection(path)
            try:
                a.execute('CREATE TABLE test (id INTEGER PRIMARY KEY)')
                a.execute('INSERT INTO test VALUES (1)')
                self.assertFalse(a.in_transaction)
                with self.assertRaises(sqlite3.IntegrityError):
                    a.execute('INSERT INTO test VALUES (1)')
                b.execute('INSERT INTO test VALUES (2)')
                self.assertEqual(b.execute('SELECT count(*) FROM test').fetchone()[0],2)
                self.assertEqual(b.execute('PRAGMA busy_timeout').fetchone()[0],15000)
                self.assertEqual(b.execute('PRAGMA journal_mode').fetchone()[0],'wal')
            finally:
                a.close();b.close()

    def test_parallel_watchlist_and_price_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            db=Database(os.path.join(directory,'test.db'))
            def write(i):
                ticker=str(i)
                db.add_user_stock({'ticker':ticker,'exchange':'KRX'})
                db.insert_price(ticker,'KRX',100,0,'KRW','test')
            try:
                with ThreadPoolExecutor(max_workers=8) as pool:
                    list(pool.map(write,range(40)))
                self.assertEqual(db.conn.execute('SELECT count(*) FROM user_stocks').fetchone()[0],40)
                self.assertFalse(db.conn.in_transaction)
            finally:
                db.close()
