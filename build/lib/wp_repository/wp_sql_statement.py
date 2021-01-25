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
from typing import Any

class SQLStatement:
    """ Encapsulation of SQL DML statements and their parameters.

    Attributes:
        stmt_text : str
            The text of the SQL DML statement.
        stmt_params : list
            List of parameter values corresponding to the place holders in the SQL statement text.

    Methods:
        SQLStatement() : SQLStatement
            Constructor
        append_text : str
            Appends a piece of text to the SQL statement text and returns the result.
        append_param : list
            Appends a value (or list of values) to the parameter list of the SQL statement
            and returns the result.
    """
    def __init__(self):
        """ Constructor

        Returns : SQLStatement
            A new SQL DML statements with empty statement text and empty parameter list.
        """
        self.stmt_text = None
        self.stmt_params = []

    def append_text(self, stmt_text: str) -> str:
        """ Appends a piece of text to the SQL statement text and returns the result.

        Parameters:
            stmt_text : str
                A piece of a SQL statement.

        Returns:
            str : The SQL statement text after having appended the given text snippet.
        """
        self.stmt_text = self.stmt_text + stmt_text
        return self.stmt_text

    def append_param(self, stmt_param: Any) -> list:
        """ Appends a value (or list of values) to the parameter list of the SQL statement
            and returns the result.

        Parameters:
            stmt_param : Any
                List of values or single value to be appended to the list or parameters.

        Returns:
            list : Parameter list after having appended the value or the list of values.
        """
        if isinstance(stmt_param, list):
            self.stmt_params.extend(stmt_param)
        else:
            self.stmt_params.append(stmt_param)
        return self.stmt_params
