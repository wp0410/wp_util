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
import sqlite3
from datetime import datetime
from typing import Any
from wp_sql_statement import SQLStatement

class AttributeMapping:
    """ Definition of the mapping between a column in a database table and an attribute of
        a python class (sub-class of RepositoryElement).

    Attributes:
        _select_rank : int
            Position of the column in the SELECT clause of a SQL SELECT statement created from the
            attribute mapping. Only entries having _select_rank >= 0 will be included in the default
            statement.
        _cl_attr_name : str
            Name of the class attribute.
        _cl_attr_type : type
            Type of the class attribute.
        _db_attr_name : str
            Name of the attribute of the database table.
        _db_key : int
            Specifies whether or not the database attribute is part of the table's primary key as well
            as the key type (auto-increment key or normal key).
        _inc_insert : bool
            Specifies whether or not the database attribute shall be included in INSERT statements.
        _inc_update : bool
            Specifies whether or not the database attribute shall be included in UPDATE statements.

    Properties:
        select_rank : int
            Getter for the "_select_rank" instance attribute.
        class_attr_name : str
            Getter for the "_cl_attr_name" instance attribute.
        db_attr_name : str
            Getter for the "_db_attr_name" instance attribute.
        is_db_key : bool
            Indicates whether or not the table attribute is part of the table's primary key.
        is_autoincrement_key : bool
            Indicates whether or not the table attribute is an auto-increment key element of the underlying
            database table.
        include_in_insert : bool
            Tests whether or not the attribute shall be included in an auto-generated INSERT statement.
        include_in_update : bool
            Tests whether or not the attribute shall be included in an auto-generated UPDATE statement.
        include_in_select : bool
            Indicates whether or not the database attribute shall appear on the select list of a
            SELECT statement.

    Methods:
        AttributeMapping():
            Constructor.
        by_rank : int, static
            Returns the "select_rank" attribute of an "AttributeMapping" instance. Needed for sorting
            a list of "AttributeMapping" objects for correctly composing a SELECT statement.
    """
    def __init__(self, select_rank: int, cls_attr_name: str, db_attr_name: str, cls_attr_type: type = str,
                 db_key: int = 0, include_in_insert: bool = True, include_in_update: bool = True):
        """ Constructor.

        Parameters:
            select_rank : int
                Position of the column in the SELECT clause of a SQL SELECT statement created from the
                attribute mapping. Only entries having _select_rank >= 0 will be included in the default
                statement.
            cls_attr_name : str
                Name of the class attribute.
            cls_attr_type : type
                Type of the class attribute.
            db_attr_name : str
                Name of the attribute of the database table.
            db_key : int, optional
                Specifies whether or not the database attribute is part of the table's primary key. Legal values are:
                    0 ... attribute is not part of the primary key;
                    1 ... attribute is part of the primary key;
                    2 ... attribute is an AUTO_INCREMENT primary key.
            include_in_insert : bool, optional
                Specifies whether or not the database attribute shall be included in INSERT statements.
            include_in_update : bool, optional
                Specifies whether or not the database attribute shall be included in UPDATE statements.
        """
        # pylint: disable=too-many-arguments
        self._select_rank = select_rank
        self._cl_attr_name = cls_attr_name
        self._cl_attr_type = cls_attr_type
        self._db_attr_name = db_attr_name
        self._db_key = db_key
        self._inc_insert = include_in_insert
        self._inc_update = include_in_update

    @property
    def select_rank(self) -> int:
        """ Getter for the "_select_rank" instance attribute.

        Returns:
            int : value of the "_select_rank" instance attribute.
        """
        return self._select_rank

    @property
    def class_attr_name(self) -> str:
        """ Getter for the "_cl_attr_name" instance attribute.

        Returns:
            str : value of the "_cl_attr_name" instance attribute.
        """
        return self._cl_attr_name

    @property
    def class_attr_type(self) -> type:
        """ Getter for the "_cl_attr_type" instance attribute.

        Returns:
            type : value of the "_cl_attr_type" instance attribute.
        """
        return self._cl_attr_type

    @property
    def db_attr_name(self) -> str:
        """ Getter for the "_db_attr_name" instance attribute.

        Returns:
            str : value of the "db_attr_name" instance attribute.
        """
        return self._db_attr_name

    @property
    def is_db_key(self) -> bool:
        """ Indicates whether or not the table attribute is part of the table's primary key.

        Returns:
            bool : true if the attribute is part of the primary key of the underlying table, false otherwise.
        """
        return self._db_key != 0

    @property
    def is_autoincrement_key(self) -> bool:
        """ Indicates whether or not the table attribute is an auto-increment key element of the underlying
            database table.

        Returns:
            bool : true if the attribute is an auto-increment key element, false otherwise.
        """
        return self._db_key == 2

    @property
    def include_in_insert(self) -> bool:
        """ Tests whether or not the attribute shall be included in an auto-generated INSERT statement.

        Returns:
            bool : True if attribute shall be included in an insert statement; False otherwise.
        """
        return self._inc_insert and not self.is_autoincrement_key

    @property
    def include_in_update(self) -> bool:
        """ Tests whether or not the attribute shall be included in an auto-generated UPDATE statement.

        Returns:
            bool : True if attribute shall be included in an update statement; False otherwise.
        """
        return self._inc_update and not self.is_autoincrement_key and not self.is_db_key

    @property
    def include_in_select(self) -> bool:
        """ Indicates whether or not the database attribute shall appear on the select list of a
            SELECT statement.

        Returns:
            bool : true if the database attribute shall be included in the SELECT statement, false otherwise.
        """
        return self._select_rank >= 0

    @staticmethod
    def by_rank(mapping: object) -> int:
        """ Returns the "select_rank" attribute of an "AttributeMapping" instance. Needed for sorting
            a list of "AttributeMapping" objects for correctly composing a SELECT statement.

        Parameters:
            mapping : AttributeMapping
                Instance to be sorted.

        Returns:
            int : rank of the database attribute in the "SELECT clause" of the default SELECT statement.
        """
        return mapping.select_rank


class AttributeMap:
    """ Defines properties to easily access the elements of a list of "AttributeMapping" entries.

    Attributes:
        _table_name : str
            Name of the underlying database table.
        _mappings : str
            List of "AttributeMapping" entries.
        _auto_increment_attr : AttributeMapping
            Element of the "_mappings" list that defines an auto-increment key for the underlying table.

    Properties:
        table_name : str
            Getter for the "_table_name" instance variable.
        mappings : list
            Getter for the "_mappings" instance variable.
        has_autoincrement_key : bool
            Checks whether or not the underlying table has an auto-increment key.
        autoincrement_attribute : AttributeMapping:
            Getter for the "AttributeMapping" instance representing the auto-increment key attribute fo the
            underlying table.
        attributes_for_select : list
            Getter for the list of attributes that are relevant for the SELECT clause of a SQL select statement.
        attributes_for_insert : list
            Getter for the list of attributes that are relevant for a SQL INSERT statement.
        attributes_for_update : list
            Getter for the list of attributes that are relevant for a SQL UPDATE statement.
        db_key_attributes : list
            Getter for the list of attributes that are part of the primary key of the underlying table.

    Methods:
        __getitem__ : AttributeMapping
            Accessor for the Attribute Mappings by class attribute name.
        _select_mappings : list
            Retrieves all attributes for which a specified bool property returns True.
    """
    def __init__(self, table_name: str, attribute_mappings: list):
        self._table_name = table_name
        self._mappings = attribute_mappings
        self._mappings.sort(key = AttributeMapping.by_rank)
        self._auto_increment_attr = None
        for mapping in self._mappings:
            if mapping.is_autoincrement_key:
                self._auto_increment_attr = mapping

    @property
    def table_name(self) -> str:
        """ Getter for the "_table_name" instance variable.

        Returns:
            str : Name of the underlying database table.
        """
        return self._table_name

    @property
    def mappings(self) -> list:
        """ Getter for the "_mappings" instance variable.

        Returns:
            list : Complete list of "AttributeMapping" entries.
        """
        return self._mappings

    @property
    def has_auto_increment_key(self) -> bool:
        """ Checks whether or not the underlying table has an auto-increment key.

        Returns:
            true if the underlying table has an auto-increment key; false otherwise.
        """
        return self._auto_increment_attr is not None

    @property
    def autoincrement_attribute(self) -> AttributeMapping:
        """ Getter for the "AttributeMapping" instance representing the auto-increment key attribute fo the
            underlying table.

        Returns:
            AttributeMapping :
                instance representing the auto-increment attribute, or None if no such element is defined.
        """
        return self._auto_increment_attr

    @property
    def attributes_for_select(self) -> list:
        """ Getter for the list of attributes that are relevant for the SELECT clause of a SQL select statement.

        Returns : list
            List of attributes that shall be included in the SELECT clause of an SQL SELECT statement.
        """
        return self._select_mappings('include_in_select')

    @property
    def attributes_for_insert(self) -> list:
        """ Getter for the list of attributes that are relevant for a SQL INSERT statement.

        Returns : list
            List of attributes that shall be included in the SQL INSERT statement.
        """
        return self._select_mappings('include_in_insert')

    @property
    def attributes_for_update(self) -> list:
        """ Getter for the list of attributes that are relevant for a SQL UPDATE statement.

        Returns : list
            List of attributes that shall be included in the SQL UPDATE statement.
        """
        return self._select_mappings('include_in_update')

    @property
    def db_key_attributes(self) -> list:
        """ Getter for the list of attributes that are part of the primary key of the underlying table.

        Returns : list
            List of attributes that are part of the table's primary key.
        """
        return self._select_mappings('is_db_key')

    def __getitem__(self, key_value) -> AttributeMapping:
        """ Accessor for the Attribute Mappings by class attribute name.

        Parameters:
            key_value : str
                Name of a class attribute.

        Returns:
            AttributeMapping
                Attribute Mapping element with the matching class attribute name. None if not found.
        """
        for mapping in self._mappings:
            if mapping.class_attr_name == key_value:
                return mapping
        return None

    def _select_mappings(self, bool_attr_name: str) -> list:
        """ Retrieves all attributes for which a specified bool property returns True.

        Parameters:
            bool_attr_name : str
                Name of the bool property of an "AttributeMapping" instance.

        Returns:
            list : list of entries for which the bool property is True.
        """
        mappings = []
        for mapping in self._mappings:
            if getattr(mapping, bool_attr_name):
                mappings.append(mapping)
        return mappings

class RepositoryElement:
    """ Blueprint for items to be stored in a SQLite Repository. Following the "Repository" design
        pattern, a "Repository" is a collection of items of the same type. In this implementation,
        the "Repository" will be mapped to a SQLite table. To make a Python class "storable" in a
        Repository, it must be derived from this class. In simple situations (e.g. class instance
        is being mapped 1:1 on a single row in a single table) it is sufficient to instantiate the
        "_attribute_map" class instance with a meaningful "AttributeMap". I more sophisticated
        cases the methods to create the SQL statements ("insert_statement", "update_statement", ...)
        must be overloaded.

    Methods:
        SQLiteRepositoryElement()
            Constructor
        load_row : RepositoryElement
            Converts an array of column values read from a SQLite cursor into a RepositoryElement
            instance.
        insert_statement : SQLStatement
            Creates the SQL DML statement to insert a RepositoryElement into the SQLite table,
            mapping its attributes to table columns.
        update_statement : SQLStatement
            Creates the SQL DML statement to update a row in the SQLite table with data from the
            RepositoryElement.
        delete_statement : SQLStatement
            Creates the SQL DML statement to delete the row corresponding to the RepositoryElement
            from the SQLite table.
        select_by_key_statement : SQLStatement
            Creates the SQL DML statement to select the row corresponding to the RepositoryElement
            (identfied by its primary key) from the SQLite table.
        select_all_statement : SQLStatement
            Creates the SQL SELECT statement to retrieve all entries from the repository, sorted by
            their key attributes.
        select_where_statement : SQLStatement
            Creates the SQL SELECT statement to retrieve all entries from the repository that match the given
            criteria, sorted by their key attributes.
        insert : int
            Inserts a RepositoryElement into the SQLite table by executing its SQL INSERT
            statement.
        update : int
            Updates a RepositoryElement in the SQLite table by executing its SQL UPDATE
            statement.
        delete : int
            Deletes a RepositoryElement from the SQLite table by executing its SQL DELETE
            statement.
        _select_clause: SQLStatement
            Creates the SELECT clause of the SQL SELECT statements from the Attribute Map.
        _key_where_clause: SQLStatement
            Creates the WHERE clause for the table attributes marked as part of the primary key
            in the Attribute Map.
        _where_clause_term : SQLStatement
            Creates part of a where clause and its parameters from the tuple containing the comparison criterion
            and appends it to the given SQL SELECT statement.
        _key_order_clause: SQLStatement
            Creates an ORDER BY clause for the table attributes marked as part of the primary key
            in the Attribute Map.
        _type_conversion : Any, static
            Converts the type of the element read from the database to the target type of the corresponding
            class attribute.
    """
    _attribute_map = AttributeMap("", [])

    def __init__(self):
        """ Constructor. """

    def load_row(self, cursor_row) -> object:
        """ Converts an array of column values read from a SQLite cursor into a RepositoryElement
            instance.

        Parameters:
            cursorRow : list
                array of column values read from a SQLite cursor.

        Returns : RepositoryElement
            The result of mapping the list of values into a RepositoryElement
        """
        for mapping in self._attribute_map.mappings:
            class_attr_val = self._type_conversion(mapping.class_attr_type, cursor_row[mapping.select_rank])
            setattr(self, mapping.class_attr_name, class_attr_val)

    @staticmethod
    def _type_conversion(cls_attr_type: type, db_attr_value: Any) -> Any:
        """ Converts the type of the element read from the database to the target type of the corresponding
            class attribute.

        Parameters:
            cls_attr_type : type
                Type of the class attribute.
            db_attr_value : Any
                Value read from the database table attribute.

        Returns:
            Any : correctly converted value.
        """
        if isinstance(db_attr_value, cls_attr_type):
            res = db_attr_value
        elif isinstance(db_attr_value, str):
            if cls_attr_type is datetime:
                if db_attr_value.find(".") >= 0:
                    res = datetime.strptime(db_attr_value, "%Y-%m-%d %H:%M:%S.%f")
                else:
                    res = datetime.strptime(db_attr_value, "%Y-%m-%d %H:%M:%S")
            else:
                res = cls_attr_type(db_attr_value)
        else:
            res = cls_attr_type(db_attr_value)
        return res

    def insert_statement(self) -> SQLStatement:
        """ Creates the SQL DML statement to insert a RepositoryElement into the SQLite table,
            mapping its attributes to table columns.

        Returns:
            SQLStatement: SQL INSERT statement created from the Attribute Map of the class.
        """
        ins_stmt = SQLStatement()
        ins_stmt.stmt_text = 'INSERT INTO {} ('.format(self._attribute_map.table_name)
        value_list = ' VALUES( '
        att_no = 0
        for mapping in self._attribute_map.attributes_for_insert:
            if att_no == 0:
                ins_stmt.append_text(mapping.db_attr_name)
                value_list = value_list + '?'
            else:
                ins_stmt.append_text(', ' + mapping.db_attr_name)
                value_list = value_list + ', ?'
            ins_stmt.append_param(getattr(self, mapping.class_attr_name))
            att_no += 1
        ins_stmt.append_text(' ) ' + value_list + ' )')
        return ins_stmt

    def update_statement(self) -> SQLStatement:
        """ Creates the SQL DML statement to update a row in the SQLite table with data from the
            RepositoryElement.

        Returns:
            SQLStatement: SQL UPDATE statement created from the Attribute Map of the class.
        """
        upd_stmt = SQLStatement()
        upd_stmt.stmt_text = 'UPDATE {} SET '.format(self._attribute_map.table_name)
        att_no = 0
        for mapping in self._attribute_map.attributes_for_update:
            if att_no == 0:
                upd_stmt.append_text(' {} = ?'.format(mapping.db_attr_name))
            else:
                upd_stmt.append_text(', {} = ?'.format(mapping.db_attr_name))
            upd_stmt.append_param(getattr(self, mapping.class_attr_name))
            att_no += 1
        return self._key_where_clause(upd_stmt)

    def delete_statement(self) -> SQLStatement:
        """ Creates the SQL DML statement to delete the row corresponding to the RepositoryElement
            from the SQLite table.

        Returns:
            SQLStatement: SQL DELETE statement created from the Attribute Map of the class.
        """
        del_stmt = SQLStatement()
        del_stmt.stmt_text = 'DELETE FROM {} '.format(self._attribute_map.table_name)
        return self._key_where_clause(del_stmt)

    def select_by_key_statement(self) -> SQLStatement:
        """ Creates the SQL DML statement to select the row corresponding to the RepositoryElement
            (identfied by its primary key) from the SQLite table.

        Returns:
            SQLStatement: SQL SELECT statement for selecting the single entry identfied by the given
                          primary key values.
        """
        sel_stmt = SQLStatement()
        self._select_clause(sel_stmt)
        return self._key_where_clause(sel_stmt)

    def select_all_statement(self) -> SQLStatement:
        """ Creates the SQL SELECT statement to retrieve all entries from the repository, sorted by
            their key attributes.

        Returns:
            SQLStatement:
                SQL SELECT statement to retrieve all entries sorted by their key attributes.
        """
        sel_stmt = SQLStatement()
        self._select_clause(sel_stmt)
        return self._key_order_clause(sel_stmt)

    def select_where_statement(self, where_criteria: list) -> SQLStatement:
        """ Creates the SQL SELECT statement to retrieve all entries from the repository that match the given
            criteria, sorted by their key attributes.

        Parameters:
            where_criteria : list
                List containing the criteria for selecting the repository entries. Every criterion is a tuple
                ('attribute_name', 'operator', 'value').

        Returns:
            SQLStatement:
                SQL SELECT statement to retrieve all matching repository elements.
        """
        sel_stmt = SQLStatement()
        sel_stmt.stmt_params = []
        self._select_clause(sel_stmt)
        att_no = 0
        for where_term in where_criteria:
            if att_no == 0:
                sel_stmt.append_text(' WHERE ')
            else:
                sel_stmt.append_text( ' AND ')
            att_no += 1
            self._where_clause_term(sel_stmt, where_term)
        return self._key_order_clause(sel_stmt)

    def _select_clause(self, sql_stmt: SQLStatement) -> SQLStatement:
        """ Creates the SELECT clause of the SQL SELECT statements from the Attribute Map.

        Parameters:
            sql_stmt: SQLStatement
                Statement to create the SELECT clause in.

        Returns:
            SQLStatement
                Statement containing the SELECT clause.
        """
        sql_stmt.stmt_text = 'SELECT '
        att_no = 0
        for mapping in self._attribute_map.attributes_for_select:
            if att_no == 0:
                sql_stmt.append_text(' {}'.format(mapping.db_attr_name))
            else:
                sql_stmt.append_text(', {}'.format(mapping.db_attr_name))
            att_no += 1
        sql_stmt.append_text(' FROM {} '.format(self._attribute_map.table_name))
        return sql_stmt

    def _where_clause_term(self, sql_stmt: SQLStatement, where_term: tuple) -> SQLStatement:
        """ Creates part of a where clause and its parameters from the tuple containing the comparison criterion
            and appends it to the given SQL SELECT statement.

        Parameters:
            sql_stmt : SQLStatement
                Statement to create the SELECT clause in.
            where_term : tuple
                Tuple containing the comparison criterion. It has to be a tuple
                (class_attribute_name, operator, value), where:
                    class_attribute_name : str
                        Name of the attribute of the contents class. Must be contained in the Attribute Map of
                        the contents class.
                    operator : str
                        Comparison operator to be applied to the class attribute. Legal values are:
                            "=", "!=", ">", "<", ">=", "<=", "LIKE", "BETWEEN".
                    value : str
                        Value to compare the class attribute to. In case of operator is "BETWEEN", value must
                        be a list containing exactly 2 elements.
        Returns:
            SQLStatement
                Statement containing the created WHERE clause part.
        """
        cond_att, cond_op, cond_val = where_term
        mapping = self._attribute_map[cond_att]
        if mapping is None:
            raise ValueError('Invalid class attribute name: "{}"'.format(cond_att))
        if cond_op.upper() not in ["=", "!=", ">", "<", "=>", "<=", "LIKE", "BETWEEN", "IN"]:
            raise ValueError('Invalid where clause operator: "{}"'.format(cond_op))
        sql_stmt.append_text(' {} {} '.format(mapping.db_attr_name, cond_op))
        if cond_op.upper() == "BETWEEN":
            sql_stmt.append_text('? AND ? ')
        elif cond_op.upper() == "IN":
            sql_stmt.append_text(f"( {','.join(['?']*len(cond_val))} )")
        else:
            sql_stmt.append_text(' ? ')
        sql_stmt.append_param(cond_val)
        return sql_stmt

    def _key_where_clause(self, sql_stmt: SQLStatement) -> SQLStatement:
        """ Creates a WHERE clause for the table attributes marked as part of the primary key in the
            Attribute Map.

        Parameters:
            sql_stmt : SQLStatement
                SQL statement to append the WHERE clause to.

        Returns:
            SQLStatement : Given SQL statement with WHERE clause.
        """
        sql_stmt.append_text(' WHERE ')
        att_no = 0
        for mapping in self._attribute_map.db_key_attributes:
            if att_no == 0:
                sql_stmt.append_text(' {} = ? '.format(mapping.db_attr_name))
            else:
                sql_stmt.append_text(' AND {} = ? '.format(mapping.db_attr_name))
            sql_stmt.append_param(getattr(self, mapping.class_attr_name))
            att_no += 1
        return sql_stmt

    def _key_order_clause(self, sql_stmt: SQLStatement) -> SQLStatement:
        """ Creates an ORDER BY clause for the table attributes marked as part of the primary key
            in the Attribute Map.

        Parameters:
            sql_stmt : SQLStatement
                SQL statement to append the ORDER BY clause to.

        Returns:
            SQLStatement : Given SQL statement with ORDER BY clause.
        """
        sql_stmt.append_text(' ORDER BY ')
        att_no = 0
        for mapping in self._attribute_map.db_key_attributes:
            if att_no == 0:
                sql_stmt.append_text(mapping.db_attr_name)
            else:
                sql_stmt.append_text(', {}'.format(mapping.db_attr_name))
            att_no += 1
        return sql_stmt

    def insert(self, cursor: sqlite3.Cursor) -> int:
        """ Inserts a RepositoryElement into the SQLite table by executing its SQL INSERT
            statement.

        Parameters:
            cursor : sqlite3.Cursor
                An open cursor within an active SQLite database connection.

        Returns:
            int : If the underlying table has an auto-increment key: new key value;
                  Otherwise: number of inserted rows (1 ... OK, 0 ... row not inserted)
        """
        sql_insert_stmt = self.insert_statement()
        cursor.execute(sql_insert_stmt.stmt_text, sql_insert_stmt.stmt_params)
        num_rows = cursor.rowcount
        if self._attribute_map.has_auto_increment_key:
            auto_key = cursor.lastrowid
            setattr(self, self._attribute_map.autoincrement_attribute.class_attr_name, auto_key)
            return auto_key
        return num_rows

    def update(self, cursor: sqlite3.Cursor) -> int:
        """ Updates a RepositoryElement in the SQLite table by executing its SQL UPDATE statement.

        Parameters:
            cursor : sqlite3.Cursor

        Returns : int
            Number of updated rows.
        """
        sql_update_stmt = self.update_statement()
        cursor.execute(sql_update_stmt.stmt_text, sql_update_stmt.stmt_params)
        return cursor.rowcount

    def delete(self, cursor: sqlite3.Cursor) -> int:
        """ Deletes a RepositoryElement from the SQLite table by executing its SQL DELETE
            statement.

        Parameters:
            cursor : sqlite3.Cursor

        Returns : int
            Number of deleted rows.
        """
        sql_delete_stmt = self.delete_statement()
        cursor.execute(sql_delete_stmt.stmt_text, sql_delete_stmt.stmt_params)
        return cursor.rowcount
