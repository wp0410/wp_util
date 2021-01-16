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

class DictConfigWrapper:
    """ Wrapper class for dictionaries containing configuration parameters. Mandatory parameters and their
        types can be checked, and optional parameters can be retrieved.

    Attributes:
        _config : dict
            Dictionary containing the configuration settings.

    Methods:
        DictConfigWrapper():
            Constructor
        value_error : Any
            Checks the result returned by a method for mandatory parameter checking. If the parameter is
            contained in the dictionary, its value is returned. Otherwise, an exception will be raised.
        mandatory_int : tuple
            Checks whether or not a mandatory 'int' parameter is contained in the configuration dictionary.
        optional_int : int
            Checks whether or not an optional 'int' parameter is contained in the configuration dictionary.
            If not, it will be created.
        mandatory_str : tuple
            Checks whether or not a mandatory 'str' parameter is contained in the configuration dictionary.
        optional_str : str
            Checks whether or not an optional 'str' parameter is contained in the configuration dictionary.
            If not, it will be created.
        mandatory_dict : tuple
            Checks whether or not a mandatory 'dict' parameter is contained in the configuration dictionary.
        mandatory_list : tuple
            Checks whether or not a mandatory 'list' parameter is contained in the configuration dictionary.
        __getitem__ : Any
            Accessor for a dictionary element.
    """
    def __init__(self, config_dict: dict):
        """ Constructor.

        Parameters:
            config_dict : dict
                Dictionary containing the configuration settings.
        """
        if not isinstance(config_dict, dict):
            raise TypeError('DictConfigWrapper(): Settings container must be dict, is: "{}"'.format(type(config_dict)))
        self._config = config_dict

    @staticmethod
    def value_error(check_result: tuple) -> Any:
        """ Checks the result returned by a method for mandatory parameter checking. If the parameter is
            contained in the dictionary, its value is returned. Otherwise, an exception will be raised.

        Parameters:
            check_result : tuple
                Result tuple of a mandatory parameter checking method.

        Returns:
            Any : value of the mandatory parameter, if contained in the parameter dictionary.
        """
        check_res, error_exception, setting_value = check_result
        if check_res:
            return setting_value
        raise error_exception

    def mandatory_int(self, setting_name: str, constraints: list = None) -> tuple:
        """ Checks whether or not a mandatory 'int' parameter is contained in the configuration dictionary.

        Parameters:
            setting_name : str
                Name of the mandatory parameter.
            constraints : list
                Constraints for the parameter value, format is [min_value: int, max_value: int, optional].

        Returns:
            tuple : Result of the check. Format is (result, exception, value) where:
                        result : bool
                            Parameter is available, type and contraints are correct.
                        exception: ValueError | TypeError | None
                            Contains error details in case the parameter is missing or its value is invalid.
                        value : int
                            Value of the parameter, if present and valid.
        """
        # pylint: disable=too-many-return-statements
        if setting_name not in self._config:
            return (False, ValueError('Missing mandatory parameter: "{}"'.format(setting_name)), None)
        if isinstance(self._config[setting_name], int):
            setting_value = self._config[setting_name]
        else:
            if isinstance(self._config[setting_name], str) and self._config[setting_name].isnumeric():
                setting_value = int(self._config[setting_name])
                self._config[setting_name] = setting_value
            else:
                try:
                    setting_value = int(self._config[setting_name], 16)
                    self._config[setting_name] = setting_value
                except TypeError:
                    return (False, TypeError('Parameter "{}": type must be "int", is "{}"'.format(
                        setting_name, type(self._config[setting_name]))), None)
                except ValueError:
                    return (False, TypeError('Parameter "{}": type must be "int", is "{}"'.format(
                        setting_name, type(self._config[setting_name]))), None)
        if constraints is None or not isinstance(constraints, list) or len(constraints) == 0:
            return (True, None, setting_value)
        if len(constraints) > 0 and setting_value < constraints[0]:
            return (False, ValueError('Parameter "{}": value {} less than minimum {}'.format(
                setting_name, setting_value, constraints[0])), None)
        if len(constraints) > 1 and setting_value > constraints[1]:
            return (False, ValueError('Parameter "{}": value {} greater than maximum {}'.format(
                setting_name, setting_value, constraints[1])), None)
        return (True, None, setting_value)

    def optional_int(self, setting_name: str, default_value: int) -> int:
        """ Checks whether or not an optional 'int' parameter is contained in the configuration dictionary.
            If not, it will be created.

        Parameters:
            setting_name : str
                Name of the mandatory parameter.
            default_value : int
                Default value for the parameter to be used in case it is contained in the dictionary.
        """
        setting_value = None
        if setting_name in self._config:
            if isinstance(self._config[setting_name], int):
                setting_value = self._config[setting_name]
            elif isinstance(self._config[setting_name], str) and self._config[setting_name].isnumeric():
                setting_value = int(self._config[setting_name])
            else:
                try:
                    setting_value = int(self._config[setting_name], 16)
                except TypeError:
                    setting_value = None
                except ValueError:
                    setting_value = None
        if setting_value is None:
            self._config[setting_name] = default_value
            setting_value = default_value
        return default_value

    def mandatory_str(self, setting_name: str, constraints: list = None) -> tuple:
        """ Checks whether or not a mandatory 'str' parameter is contained in the configuration dictionary.

        Parameters:
            setting_name : str
                Name of the mandatory parameter.
            constraints : list
                Constraints for the parameter value, format is [min_length: int, max_length: int, optional].

        Returns:
            tuple : Result of the check. Format is (result, exception, value) where:
                        result : bool
                            Parameter is available, type and contraints are correct.
                        exception: ValueError | TypeError | None
                            Contains error details in case the parameter is missing or its value is invalid.
                        value : str
                            Value of the parameter, if present and valid.
        """
        # pylint: disable=too-many-return-statements
        if setting_name not in self._config:
            return (False, ValueError('Missing mandatory parameter: "{}"'.format(setting_name)), None)
        if isinstance(self._config[setting_name], str):
            setting_value = self._config[setting_name]
        else:
            try:
                setting_value = str(self._config[setting_name])
                self._config[setting_name] = setting_value
            except TypeError:
                return (False, TypeError('Parameter "{}": type must be "str", is "{}"'.format(
                    setting_name, type(self._config[setting_name]))), None)
            except ValueError:
                return (False, TypeError('Parameter "{}": type must be "str", is "{}"'.format(
                    setting_name, type(self._config[setting_name]))), None)
        if constraints is None or not isinstance(constraints, list) or len(constraints) == 0:
            return (True, None, setting_value)
        if len(constraints) > 0 and len(setting_value) < constraints[0]:
            return (False, ValueError('Parameter "{}": length {} less than minimum {}'.format(
                setting_name, setting_value, constraints[0])), None)
        if len(constraints) > 1 and len(setting_value) > constraints[1]:
            return (False, ValueError('Parameter "{}": length {} greater than maximum {}'.format(
                setting_name, setting_value, constraints[1])), None)
        return (True, None, setting_value)

    def optional_str(self, setting_name: str, default_value: str) -> str:
        """ Checks whether or not an optional 'str' parameter is contained in the configuration dictionary.
            If not, it will be created.

        Parameters:
            setting_name : str
                Name of the mandatory parameter.
            default_value : str
                Default value for the parameter to be used in case it is contained in the dictionary.
        """
        setting_value = None
        if setting_name in self._config:
            if isinstance(self._config[setting_name], str):
                setting_value = self._config[setting_name]
            else:
                try:
                    setting_value = str(self._config[setting_name], 16)
                except TypeError:
                    setting_value = None
                except ValueError:
                    setting_value = None
        if setting_value is None:
            self._config[setting_name] = default_value
            setting_value = default_value
        return default_value

    def mandatory_dict(self, setting_name: str, constraints: list = None) -> tuple:
        """ Checks whether or not a mandatory 'dict' parameter is contained in the configuration dictionary.

        Parameters:
            setting_name : str
                Name of the mandatory parameter.
            constraints : list
                Constraints for the parameter value, format is [required_elem_1, required_elem_2, ...].

        Returns:
            tuple : Result of the check. Format is (result, exception, value) where:
                        result : bool
                            Parameter is available, type and contraints are correct.
                        exception: ValueError | TypeError | None
                            Contains error details in case the parameter is missing or its value is invalid.
                        value : dict
                            Value of the parameter, if present and valid.
        """
        if setting_name not in self._config:
            return (False, ValueError('Missing mandatory parameter: "{}"'.format(setting_name)), None)
        if not isinstance(self._config[setting_name], dict):
            return (False, TypeError('Parameter "{}": type must be "dict", is "{}'.format(
                setting_name, type(self._config[setting_name]))), None)
        setting_value = self._config[setting_name]
        for mand_setting in constraints:
            if mand_setting not in setting_value:
                return (False, ValueError('Parameter "{}": missing element "{}"'.format(
                    setting_name, mand_setting)), None)
        return (True, None, setting_value)

    def mandatory_list(self, setting_name: str, constraints: list = None) -> tuple:
        """ Checks whether or not a mandatory 'list' parameter is contained in the configuration dictionary.

        Parameters:
            setting_name : str
                Name of the mandatory parameter.
            constraints : list
                Constraints for the parameter value, format is [min_length: int, max_length: int, optional].

        Returns:
            tuple : Result of the check. Format is (result, exception, value) where:
                        result : bool
                            Parameter is available, type and contraints are correct.
                        exception: ValueError | TypeError | None
                            Contains error details in case the parameter is missing or its value is invalid.
                        value : list
                            Value of the parameter, if present and valid.
        """
        if setting_name not in self._config:
            return (False, ValueError('Missing mandatory parameter: "{}"'.format(setting_name)), None)
        if not isinstance(self._config[setting_name], list):
            return (False, TypeError('Parameter "{}": type must be "list", is "{}'.format(
                setting_name, type(self._config[setting_name]))), None)
        setting_value = self._config[setting_name]
        if constraints is None or not isinstance(constraints, list) or len(constraints) == 0:
            return (True, None, setting_value)
        if len(constraints) > 0 and len(setting_value) < constraints[0]:
            return (False, ValueError('Parameter "{}": length {} less than minimum {}'.format(
                setting_name, setting_value, constraints[0])), None)
        if len(constraints) > 1 and len(setting_value) > constraints[1]:
            return (False, ValueError('Parameter "{}": length {} greater than maximum {}'.format(
                setting_name, setting_value, constraints[1])), None)
        return (True, None, setting_value)

    def __getitem__(self, setting_name) -> Any:
        """ Accessor for a dictionary element.

        Parameters:
            setting_name : str
                Name of a configuration parameter.

        Returns:
            Any : value of the configuration parameter.
        """
        if setting_name in self._config:
            return self._config[setting_name]
        raise KeyError('Configuration: no such element "{}"'.format(setting_name))
