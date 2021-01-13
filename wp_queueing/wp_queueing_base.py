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

class QueueingException(Exception):
    """ Exception thrown by the queueing utilities.

    Attributes:
        reason : str
            Reason for throwing the exception.
        message : str
            Error message

    Methods:
        __str__ : str
            Converts the exception object to a string.
    """
    def __init__(self, reason, message):
        """ Contructor """
        super().__init__()
        self.reason = reason
        self.message = message

    def __str__(self) -> str:
        """ Converts the exception object to a string.

        Returns:
            str : String representation of the exception object.
        """
        return 'QUEUEING EXCEPTION({}): {}'.format(self.reason, self.message)
