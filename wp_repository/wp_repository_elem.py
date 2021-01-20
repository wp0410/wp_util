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
            Getter for the "_inc_insert" instance attribute.
        include_in_update : bool
            Getter for the "_inc_update" instance attribute.
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
    def __init__(self, select_rank: int, cls_attr_name: str, db_attr_name: str,
                 db_key: int = 0, include_in_insert: bool = True, include_in_update: bool = True):
        """ Constructor.

        Parameters:
            select_rank : int
                Position of the column in the SELECT clause of a SQL SELECT statement created from the
                attribute mapping. Only entries having _select_rank >= 0 will be included in the default
                statement.
            cls_attr_name : str
                Name of the class attribute.
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
        """ Getter for the "_inc_insert" instance attribute.

        Returns:
            Value of the "_inc_insert" instance attribute.
        """
        return self._inc_insert

    @property
    def include_in_update(self) -> bool:
        """ Getter for the "_inc_update" instance attribute.

        Returns:
            Value of the "_inc_update" instance attribute.
        """
        return self._inc_update

    @property
    def include_in_select(self) -> bool:
        """ Indicates whether or not the database attribute shall appear on the select list of a
            SELECT statement.

        Returns:
            bool : true if the database attribute shall be included in the SELECT statement, false otherwise.
        """
        return self._select_rank >= 0

    @staticmethod
    def by_rank(mapping: AttributeMapping) -> int:
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
            if getattr(self, bool_attr_name):
                mappings.append(mapping)
        return mappings

class RepositoryElement:
    """ Blueprint for items to be stored in a SQLite Repository. Following the "Repository" design
        pattern, a "Repository" is a collection of items of the same type. In this implementation,
        the "Repository" will be mapped to a SQLite table. To make a Python class "storable" in a
        Repository, it must be derived from this class. The methods that create SQL DML statements
        must be overloaded to provide the specific statement to manipulate the underlying SQLite
        table of the Repository.

    Methods:
        RepositoryElement() : RepositoryElement
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
        insert : int
            Inserts a RepositoryElement into the SQLite table by executing its SQL INSERT
            statement.
        update : int
            Updates a RepositoryElement in the SQLite table by executing its SQL UPDATE
            statement.
        delete : int
            Deletes a RepositoryElement from the SQLite table by executing its SQL DELETE
            statement.
    """
    _attribute_map = AttributeMap("", [])

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
            setattr(self, mapping.class_attr_name, cursor_row[mapping.select_rank])

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
            SQLStatement: SQL INSERT statement for selecting the single entry identfied by the given
                          primary key values.
        """
        sel_stmt = SQLStatement()
        sel_stmt.stmt_text = 'SELECT '
        att_no = 0
        for mapping in self._attribute_map.attributes_for_select:
            if att_no == 0:
                sel_stmt.append_text(' {}'.format(mapping.db_attr_name))
            else:
                sel_stmt.append_text(', {}'.format(mapping.db_attr_name))
            att_no += 1
        return self._key_where_clause(sel_stmt)

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

    def insert(self, cursor: sqlite3.Cursor) -> int:
        """ Inserts a RepositoryElement into the SQLite table by executing its SQL INSERT
            statement.

        Parameters:
            cursor : sqlite3.Cursor
                An open cursor within an active SQLite database connection.

        Returns:
            int : Number of inserted rows (1 ... OK, 0 ... row not inserted)
        """
        sql_insert_stmt = self.insert_statement()
        cursor.execute(sql_insert_stmt.stmt_text, sql_insert_stmt.stmt_params)
