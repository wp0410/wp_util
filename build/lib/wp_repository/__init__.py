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
# pylint: disable=wrong-import-position
import sys
if __file__.rfind('\\') < 0:
    DELIMITER = '/'
else:
    DELIMITER = '\\'

current_path = __file__[:__file__.rfind(DELIMITER) - len(__file__)]
if current_path not in sys.path:
    sys.path.append(current_path)

from wp_repository_sl3 import SQLiteRepository
from wp_sql_statement import SQLStatement
from wp_repository_elem import AttributeMapping
from wp_repository_elem import AttributeMap
from wp_repository_elem import RepositoryElement
