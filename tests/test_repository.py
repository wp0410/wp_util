"""
    Copyright 2021 Walter Pachlinger (walter.pachlinger@gmail.com)

    Licensed under the EUPL, Version 1.2 or - as soon they will be approved by the European
    Commission - subsequent versions of the EUPL (the LICENSE). You may not use this work except
    in compliance with the LICENSE. You may obtain a copy of the LICENSE at:

        https://joinup.ec.europa.eu/software/page/eupl

    Unless required by applicable law or agreed to in writing, software distributed under the
    LICENSE is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied. See the LICENSE for the specific language governing permissions
    and limitations under the LICENSE.
"""
import unittest
import sqlite3
import json
from datetime import datetime, timedelta
from decimal import Decimal
import random
import wp_repository_elem as rep_elem
import wp_repository_sl3 as repo3


class TestPerson(rep_elem.RepositoryElement):
    _attribute_map = rep_elem.AttributeMap("test_person", 
                                           [rep_elem.AttributeMapping(0, 'name', 'test_name', str, 1),
                                            rep_elem.AttributeMapping(1, 'age', 'test_age', int, 0),
                                            rep_elem.AttributeMapping(2, 'weight', 'test_wght', Decimal, 0),
                                            rep_elem.AttributeMapping(3, 'eye_color', 'test_ec', str, 0)])
    def __init__(self):
        super().__init__()
        self.name = 'John'
        self.age = 35
        self.weight = 84.6
        self.eye_color = 'brown'

class TestProduct(rep_elem.RepositoryElement):
    _attribute_map = rep_elem.AttributeMap("test_product",
                                           [rep_elem.AttributeMapping(0, 'product_id', 'test_prod_id', int, 2),
                                            rep_elem.AttributeMapping(1, 'ean', 'test_prod_ean'),
                                            rep_elem.AttributeMapping(2, 'name', 'test_name'),
                                            rep_elem.AttributeMapping(3, 'category', 'test_cat')])
    def __init__(self):
        super().__init__()
        self.product_id = None
        self.ean = '1234567890'
        self.name = '1.5 V Battery AAA'
        self.category = 'Battery'

class Test1RepositoryObject(unittest.TestCase):
    def setUp(self):
        super().setUp()

    def test_01(self):
        """ Test RepositoryElement with normal key. """
        telem = TestPerson()
        # test INSERT statement construction
        ref_stmt = 'INSERT INTO test_person( test_name, test_age, test_wght, test_ec ) VALUES( ?,?,?,? )'
        ref_stmt_params = [telem.name, telem.age, telem.weight, telem.eye_color]
        ins_stmt = telem.insert_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), ins_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, ins_stmt.stmt_params)
        # test UPDATE statement construction
        ref_stmt = 'UPDATE test_person SET test_age=?, test_wght=?, test_ec=? WHERE test_name=?'
        ref_stmt_params = [telem.age, telem.weight, telem.eye_color, telem.name]
        upd_stmt = telem.update_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), upd_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, upd_stmt.stmt_params)
        # test DELETE statement construction
        ref_stmt = 'DELETE FROM test_person WHERE test_name=?'
        ref_stmt_params = [telem.name]
        del_stmt = telem.delete_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), del_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, del_stmt.stmt_params)
        # test SELECT statement construction
        ref_stmt = 'SELECT test_name, test_age, test_wght, test_ec FROM test_person WHERE test_name=?'
        ref_stmt_params = [telem.name]
        sel_stmt = telem.select_by_key_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), sel_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, sel_stmt.stmt_params)

    def test_02(self):
        """ Test RepositoryElement with auto-increment key. """
        telem = TestProduct()
        # test INSERT statement construction
        ref_stmt = 'INSERT INTO test_product( test_prod_ean, test_name, test_cat ) VALUES ( ?, ?, ? )'
        ref_stmt_params = [telem.ean, telem.name, telem.category]
        ins_stmt = telem.insert_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), ins_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, ins_stmt.stmt_params)
        # test UPDATE statement construction
        ref_stmt = 'UPDATE test_product SET test_prod_ean=?, test_name=?, test_cat=? WHERE test_prod_id=?'
        ref_stmt_params = [telem.ean, telem.name, telem.category, telem.product_id]
        upd_stmt = telem.update_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), upd_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, upd_stmt.stmt_params)
        # test DELETE statement construction
        ref_stmt = 'DELETE FROM test_product WHERE test_prod_id=?'
        ref_stmt_params = [telem.product_id]
        del_stmt = telem.delete_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), del_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, del_stmt.stmt_params)
        # test SELECT statement construction
        ref_stmt = 'SELECT test_prod_id, test_prod_ean, test_name, test_cat FROM test_product WHERE test_prod_id=?'
        ref_stmt_params = [telem.product_id]
        sel_stmt = telem.select_by_key_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), sel_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, sel_stmt.stmt_params)


def create_test_db(db_path: str):
    db_conn = sqlite3.connect(db_path)
    db_curs = db_conn.cursor()
    table_def = """
        CREATE TABLE test_table_1( 
            key_elem_1 TEXT NOT NULL,
            key_elem_2 INTEGER NOT NULL,
            test_elem_txt TEXT,
            test_elem_int INTEGER NOT NULL,
            test_elem_dec DECIMAL,
            test_elem_dtm TEXT NOT NULL,
            PRIMARY KEY (key_elem_1, key_elem_2))
        """
    db_curs.execute(table_def)
    table_def = """
        CREATE TABLE test_table_2(
            auto_elem_1 INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            test_elem_txt TEXT,
            test_elem_int INTEGER NOT NULL,
            test_elem_dec DECIMAL )
    """
    db_curs.execute(table_def)
    db_curs.close()
    db_conn.commit()
    db_conn.close()


rnd_txt_list = ["was", "wer", "sich", "noch", "du", "anderen", "streiten", "keinen", "heute", "eine", "dann", "Sommer", "kannst", "Grube", "freut", "besorgen", "gräbt", 
                "sich", "das", "fällt", "der", "verschiebe", "selbst", "dritte", "nicht", "hinein", "eine", "auf", "wenn", "Schwalbe", "morgen", "zwei", "macht" ]

class TestTable1(rep_elem.RepositoryElement):
    _attribute_map = rep_elem.AttributeMap(
        "test_table_1",
        [rep_elem.AttributeMapping(0, 'cls_elem_1', 'key_elem_1', int, 1),
         rep_elem.AttributeMapping(1, 'cls_elem_2', 'key_elem_2', str, 1),
         rep_elem.AttributeMapping(2, 'cls_elem_txt', 'test_elem_txt', str),
         rep_elem.AttributeMapping(3, 'cls_elem_int', 'test_elem_int', int),
         rep_elem.AttributeMapping(4, 'cls_elem_dec', 'test_elem_dec', Decimal),
         rep_elem.AttributeMapping(5, 'cls_elem_dtm', 'test_elem_dtm', datetime)])

    def __init__(self):
        super().__init__()
        self._rnd = random.Random()
        self.cls_elem_1 = None
        self.cls_elem_2 = None
        self.cls_elem_txt = ""
        self.cls_elem_int = 0
        self.cls_elem_dec = 0.0
        self.cls_elem_dtm = None

    def random(self):
        self.cls_elem_1 = self._rnd.randint(1, 10000)
        self.cls_elem_2 = self._randtxt()
        self.cls_elem_txt = self._randtxt()
        self.cls_elem_int = self._rnd.randint(1, 10000)
        self.cls_elem_dec = self._rnd.randint(1, 1000000000) / self._rnd.randint(100, 10000)
        self.cls_elem_dtm = datetime.now() - timedelta(self._rnd.randint(0, 60), self._rnd.randint(1, 86400))

    def _randtxt(self):
        res = ""
        len_ = self._rnd.randint(4, 12)
        while len_ > 0:
            idx = self._rnd.randint(0, len(rnd_txt_list) - 1)
            res = res + rnd_txt_list[idx] + " "
            len_ -= 1
        res = res + "!"
        return res

    def __str__(self) -> str:
        if isinstance(self.cls_elem_dtm, datetime):
            dtm = self.cls_elem_dtm.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            dtm = self.cls_elem_dtm

        res = {
            'cls_elem_1': self.cls_elem_1,
            'cls_elem_2': self.cls_elem_2,
            'cls_elem_txt': self.cls_elem_txt,
            'cls_elem_int': self.cls_elem_int,
            'cls_elem_dec': self.cls_elem_dec,
            'cls_elem_dtm': dtm
        }
        return json.dumps(res)

import os

class Test2Repository(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._db_path = 'test_db_{}.sl3'.format(datetime.now().strftime("%Y%m%d%H%M%S.%f"))
        create_test_db(self._db_path)
        self._repository = repo3.SQLiteRepository(TestTable1)
        self._repository.open(self._db_path)

    def tearDown(self):
        super().tearDown()
        self._repository.close()
        os.remove(self._db_path)

    def test_01_table1(self):
        t1 = TestTable1()
        t1.random()
        
        # Test INSERT and SELECT_BY_KEY
        num_rec = self._repository.insert(t1)
        self.assertTrue(num_rec == 1)
        t2 = self._repository.select_by_key(t1)
        self.assertTrue(t2 is not None)
        self.assertTrue(isinstance(t2, TestTable1))
        self.assertEqual(t1.cls_elem_1, t2.cls_elem_1)
        self.assertEqual(t1.cls_elem_2, t2.cls_elem_2)
        self.assertEqual(t1.cls_elem_int, t2.cls_elem_int)
        self.assertEqual(t1.cls_elem_dec, t2.cls_elem_dec)
        self.assertEqual(t1.cls_elem_dtm, t2.cls_elem_dtm)

        # Test UPDATE
        t1.cls_elem_int = 100
        t1.cls_elem_txt = 'Changed!'
        t1.cls_elem_dtm = datetime.now()
        num_rec = self._repository.update(t1)
        self.assertTrue(num_rec == 1)
        t3 = self._repository.select_by_key(t1)
        self.assertTrue(t3 is not None)
        self.assertIsInstance(t3, TestTable1)
        self.assertEqual(t1.cls_elem_1, t3.cls_elem_1)
        self.assertEqual(t1.cls_elem_2, t3.cls_elem_2)
        self.assertEqual(t1.cls_elem_int, t3.cls_elem_int)
        self.assertEqual(t1.cls_elem_dec, t3.cls_elem_dec)
        self.assertEqual(t1.cls_elem_dtm, t3.cls_elem_dtm)

        # Test DELETE
        num_rec = self._repository.delete(t1)
        self.assertTrue(num_rec == 1)
        t4 = self._repository.select_by_key(t1)
        self.assertTrue(t4 is None)

        # test SELECT_ALL
        num_rec = 0
        for cnt in range(100):
            t5 = TestTable1()
            t5.random()
            num_rec += self._repository.insert(t5)
        self.assertEqual(num_rec, 100)
        rec_list = self._repository.select_all()
        self.assertEqual(len(rec_list), num_rec)
        for rec in rec_list:
            self.assertIsInstance(rec, TestTable1)
            t6 = self._repository.select_by_key(rec)
            self.assertIsInstance(t6, TestTable1)
            self.assertEqual(t6.cls_elem_1, rec.cls_elem_1)
            self.assertEqual(t6.cls_elem_2, rec.cls_elem_2)
            self.assertEqual(t6.cls_elem_int, rec.cls_elem_int)
            self.assertEqual(t6.cls_elem_dec, rec.cls_elem_dec)
            self.assertEqual(t6.cls_elem_dtm, rec.cls_elem_dtm)
            self._repository.delete(t6)
        rec_list = self._repository.select_all()
        self.assertEqual(len(rec_list), 0)

    def test_02_With(self):
        with repo3.SQLiteRepository(TestTable1, self._db_path) as repo:
            num_rec = 0
            for cnt in range(25):
                t0 = TestTable1()
                t0.random()
                num_rec += repo.insert(t0)
            self.assertEqual(num_rec, 25)
            rec_list = repo.select_all()
            self.assertEqual(num_rec, len(rec_list))
            for rec in rec_list:
                repo.delete(rec)
            rec_list = repo.select_all()
            self.assertEqual(0, len(rec_list))


if __name__ == '__main__':
    unittest.main(verbosity=5)
