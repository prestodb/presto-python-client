"""
This module defines exceptions for Presto operations. It follows the structure
defined in pep-0249.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


class HttpError(Exception):
    pass


class Http503Error(HttpError):
    pass


class PrestoError(Exception):
    pass


class PrestoQueryError(Exception):
    def __init__(self, error):
        self._error = error

    @property
    def error_code(self):
        return self._error['errorCode']

    @property
    def error_name(self):
        return self._error['errorName']

    @property
    def error_type(self):
        return self._error['errorType']

    @property
    def error_exception(self):
        return self._error['failureInfo']['type']

    @property
    def message(self):
        return self._error.get(
            'message',
            'Presto did no return an error message',
        )

    @property
    def error_location(self):
        location = self._error['errorLocation']
        return (location['lineNumber'], location['columnNumber'])

    def __repr__(self):
        return '{}(type={}, name={}, message="{}")'.format(
            self.__class__.__name__,
            self.error_type,
            self.error_name,
            self.message,
        )

    def __str__(self):
        return repr(self)


class PrestoExternalError(PrestoQueryError):
    pass


class PrestoInternalError(PrestoQueryError):
    pass


class PrestoUserError(PrestoQueryError):
    pass
