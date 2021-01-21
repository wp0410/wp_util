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
import wp_repository_elem as rep_elem


class TestElement(rep_elem.RepositoryElement):
    _attribute_map = rep_elem.AttributeMap("test_table", 
                                           [rep_elem.AttributeMapping(0, 'name', 'test_name', 1),
                                            rep_elem.AttributeMapping(1, 'age', 'test_age', 0),
                                            rep_elem.AttributeMapping(2, 'weight', 'test_wght', 0),
                                            rep_elem.AttributeMapping(3, 'eye_color', 'test_ec', 0)])
    def __init__(self):
        super().__init__()
        self.name = 'John'
        self.age = 35
        self.weight = 84.6
        self.eye_color = 'brown'


class Test1RepositoryObject(unittest.TestCase):
    def setUp(self):
        super().setUp()

    def test_01(self):
        telem = TestElement()
        ref_stmt = 'INSERT INTO test_table( test_name, test_age, test_wght, test_ec ) VALUES( ?,?,?,? )'
        ref_stmt_params = [telem.name, telem.age, telem.weight, telem.eye_color]
        ins_stmt = telem.insert_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), ins_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, ins_stmt.stmt_params)
        ref_stmt = 'UPDATE test_table SET test_name=?, test_age=?, test_wght=?, test_ec=? WHERE test_name=?'
        ref_stmt_params = [telem.name, telem.age, telem.weight, telem.eye_color, telem.name]
        upd_stmt = telem.update_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), upd_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, upd_stmt.stmt_params)
        ref_stmt = 'DELETE FROM test_table WHERE test_name=?'
        ref_stmt_params = [telem.name]
        del_stmt = telem.delete_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), del_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, del_stmt.stmt_params)
        ref_stmt = 'SELECT test_name, test_age, test_wght, test_ec FROM test_table WHERE test_name=?'
        ref_stmt_params = [telem.name]
        sel_stmt = telem.select_by_key_statement()
        self.assertEqual(ref_stmt.lower().strip().replace(' ',''), sel_stmt.stmt_text.lower().strip().replace(' ',''))
        self.assertEqual(ref_stmt_params, sel_stmt.stmt_params)

if __name__ == '__main__':
    unittest.main(verbosity=5)
