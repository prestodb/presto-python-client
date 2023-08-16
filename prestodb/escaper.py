# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This code is forked from https://github.com/dropbox/PyHive (the Apache License, Version 2.0)
from __future__ import absolute_import

import datetime

try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable

import prestodb.exceptions

class ParamsEscaper(object):
    _DATE_FORMAT = "%Y-%m-%d"
    _TIME_FORMAT = "%H:%M:%S.%f"
    _DATETIME_FORMAT = "{} {}".format(_DATE_FORMAT, _TIME_FORMAT)

    def escape_args(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}

        if isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)

        raise prestodb.exceptions.ProgrammingError("Unsupported param format: {}".format(parameters))

    def escape_number(self, item):
        return item

    def escape_bytes(self, item):
        return self.escape_string(item.decode("utf-8"))

    def escape_string(self, item):
        # This is good enough when backslashes are literal, newlines are just followed, and the way
        # to escape a single quote is to put two single quotes.
        # (i.e. only special character is single quote)
        return "'{}'".format(item.replace("'", "''"))

    def escape_sequence(self, item):
        l = map(str, map(self.escape_item, item))
        return '(' + ','.join(l) + ')'

    def escape_datetime(self, item, format, cutoff=0):
        dt_str = item.strftime(format)
        formatted = dt_str[:-cutoff] if cutoff and format.endswith(".%f") else dt_str

        _type = "timestamp" if isinstance(item, datetime.datetime) else "date"
        return "{} {}".format(_type, formatted)

    def escape_item(self, item):
        if item is None:
            return 'NULL'

        if isinstance(item, (int, float)):
            return self.escape_number(item)

        if isinstance(item, bytes):
            return self.escape_bytes(item)

        if isinstance(item, str):
            return self.escape_string(item)

        if isinstance(item, Iterable):
            return self.escape_sequence(item)

        if isinstance(item, datetime.datetime):
            return self.escape_datetime(item, self._DATETIME_FORMAT)

        if isinstance(item, datetime.date):
            return self.escape_datetime(item, self._DATE_FORMAT)

        raise prestodb.exceptions.ProgrammingError("Unsupported object {}".format(item))

escaper = ParamsEscaper()

def escape(params):
    return escaper.escape_args(params)
