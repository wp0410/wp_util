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
from wp_repository_elem import RepositoryElement

class SQLiteRepository:
    """ The repository class following the "Repository" design pattern. Maps Python objects onto a
        relational table and allows for DML operations (insert, update, delete, select) on the
        resulting object collection.

    Attributes:
        _sql_connection : sqlite3.Connection
            Handle to a session in a SQLite database.
        _contents_class_name : str
            Name of the contents class
        _can_close : bool
            Indicates whether or not the DB session can be closed by the object instance itself.

    Methods:
        SQLiteRepository()
            Constructor
        open : None
            Opens the session to a SQLite database.
        close : None
            Closes the session to the SQLite database.
        insert : int
            Maps an object of the contents class to a database record and inserts it into the
            underlying table.
        update : int
            Updates the underlying database record with data from the given contents class object.
        delete : int
            Deletes the row identified by the given element from the underlying table.
        select_by_key : RepositoryObject
            Selects the single element identified by the primary key values of the given parameter element
            from the underlying table.
    """
    def __init__(self, contents_type: str, sql_connection: sqlite3.Connection = None):
        """ Constructor.

        Parameters:
            contents_type: str
               Class name of the contents type, to be used for converting the SQLite cursor row into an
               object.
            sql_connection: sqlite3.Connection, optional
                Handle to an open connection to a SQLite database. If specified, this connection will be
                used for the SQL operations of the SQLiteRepository instance.
        """
        self._sql_connection = sql_connection
        self._contents_class_name = contents_type
        self._can_close = sql_connection is None

    def __del__(self):
        """ Destructor. """
        self.close()

    def open(self, sqlite_file_path: str) -> None:
        """ Opens the session to a SQLite database.

        Parameters:
            sqlite_file_path : str
                Full path name of the SQLite database file to open.
        """
        self._sql_connection = sqlite3.connect(sqlite_file_path)
        cursor = self._sql_connection.cursor()
        cursor.execute('PRAGMA foreign_keys=ON')
        cursor.close()
        self._can_close = True

    def close(self) -> None:
        """ Closes the session to the SQLite database. """
        if self._can_close and self._sql_connection is not None:
            self._sql_connection.close()
            self._sql_connection = None

    def insert(self, element: RepositoryElement, do_commit: bool = True) -> int:
        """ Maps an object of the contents class to a database record and inserts it into the
            underlying table.

        Parameters:
            element : RepositoryElement
                Python object to be mapped and inserted to the underlying table.
            do_commit : bool, optional
                Indicates whether or not the insert transaction shall be committed.
                Default value is "True".

        Returns:
            int : If the underlying table as an auto-increment key, the key value for the new
                  row will be returned. Otherwise, the return value will be the number of inserted
                  records (0 or 1).
        """
        cursor = self._sql_connection.cursor()
        res = element.insert(cursor)
        cursor.close()
        if do_commit:
            self._sql_connection.commit()
        return res

    def update(self, element: RepositoryElement, do_commit: bool = True) -> int:
        """ Updates the underlying database record with data from the given contents class object.

        Parameters:
            element : RepositoryElement
                Python object to be used to update a row in the underlying table.
            do_commit : bool, optional
                Indicates whether or not the update transaction shall be committed.
                Default value is "True".

        Returns:
            int : the number of successfully updated records (0 or 1).
        """
        cursor = self._sql_connection.cursor()
        res = element.update(cursor)
        cursor.close()
        if do_commit:
            self._sql_connection.commit()
        return res

    def delete(self, element: RepositoryElement, do_commit: bool = True) -> int:
        """ Deletes the row identified by the given element from the underlying table.

        Parameters:
            element : RepositoryElement
                Python object identifying the row to be deleted from the underlying table.
            do_commit : bool, optional
                Indicates whether or not the delete transaction shall be committed.
                Default value is "True".

        Returns:
            int : the number of successfully deleted records (0 or 1).
        """
        cursor = self._sql_connection.cursor()
        res = element.delete(cursor)
        cursor.close()
        if do_commit:
            self._sql_connection.commit()
        return res

    def select_by_key(self, source_element: RepositoryElement, do_commit: bool = True) -> RepositoryElement:
        """ Selects the single element identified by the primary key values of the given parameter element
            from the underlying table.

        Parameters:
            source_element : RepositoryElement
                Python object identifying the row to be retrieved from the underlying table.
            do_commit : bool, optional
                Indicates whether or not the select transaction shall be committed.
                Default value is "True".

        Returns:
            RepositoryElement: retrieved row converted to the contents class, or None if the given source_element
                               does not identify a row in the database.
        """
        res = None
        select_stmt = source_element.select_by_key_statement()
        cursor = self._sql_connection.cursor()
        cursor.execute(select_stmt.stmt_text, select_stmt.stmt_params)
        qry_result = cursor.fetchone()
        cursor.close()
        if do_commit:
            self._sql_connection.commit()
        if qry_result is None:
            return None
        res = (globals()[self._contents_class_name])()
        res.load_row(qry_result)
        return res