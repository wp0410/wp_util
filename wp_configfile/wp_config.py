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
import os
import json

class ConfigFile:
    """ Wrapper for the local configuration file. Looks for a file with the same name as the script,
        with extension .config.json in the current application directory, if no file name is given.

    Attributes:
        _config : dict
            The dictionary created by loading the JSON configuration file.

    Methods:
        ConfigFile:
            Constructor.
        contains : bool
            Checks whether or not a configuration item is present in the settings file.
        __getitem__ : Any
            Accessor for an element of the configuration dictionary.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, config_file_path: str = None, app_name: str = None):
        """ Constructor.

        Parameters:
            config_file_path : str, optional
                Full path name of the configuration file.
        """
        if config_file_path is not None:
            conf_file_name = config_file_path
        elif app_name is not None:
            conf_file_name = f"{app_name}.config.json"
        else:
            conf_file_name = f"{os.path.splitext(os.path.basename(__file__))[0]}.config.json"
        with open(conf_file_name) as json_file:
            self._config = json.load(json_file)

    def __getitem__(self, item_name: str) -> Any:
        """ Accessor for an element of the configuration dictionary. """
        return self._config[item_name]

    def contains(self, item_name: str) -> bool:
        """ Checks whether or not a configuration item is present in the settings file. """
        return item_name in self._config
