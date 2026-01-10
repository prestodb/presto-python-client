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

# Attribution:
# This code is adapted from the trino-python-client project (Apache 2.0 License).
# https://github.com/trinodb/trino-python-client/blob/master/trino/sqlalchemy/datatype.py

from sqlalchemy import types


class DOUBLE(types.Float):
    __visit_name__ = "DOUBLE"


class REAL(types.Float):
    __visit_name__ = "REAL"


class BOOLEAN(types.Boolean):
    __visit_name__ = "BOOLEAN"


class TINYINT(types.Integer):
    __visit_name__ = "TINYINT"


class SMALLINT(types.Integer):
    __visit_name__ = "SMALLINT"


class INTEGER(types.Integer):
    __visit_name__ = "INTEGER"


class BIGINT(types.BigInteger):
    __visit_name__ = "BIGINT"


class DECIMAL(types.DECIMAL):
    __visit_name__ = "DECIMAL"


class VARCHAR(types.String):
    __visit_name__ = "VARCHAR"


class CHAR(types.String):
    __visit_name__ = "CHAR"


class VARBINARY(types.LargeBinary):
    __visit_name__ = "VARBINARY"


class JSON(types.JSON):
    __visit_name__ = "JSON"


class DATE(types.Date):
    __visit_name__ = "DATE"


class TIME(types.Time):
    __visit_name__ = "TIME"


class TIMESTAMP(types.TIMESTAMP):
    __visit_name__ = "TIMESTAMP"


class INTERVAL(types.TypeEngine):
    __visit_name__ = "INTERVAL"

    def __init__(self, start, end=None, precision=None):
        self.start = start
        self.end = end
        self.precision = precision


class ARRAY(types.TypeEngine):
    __visit_name__ = "ARRAY"

    def __init__(self, item_type):
        self.item_type = item_type


class MAP(types.TypeEngine):
    __visit_name__ = "MAP"

    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type


class ROW(types.TypeEngine):
    __visit_name__ = "ROW"

    def __init__(self, attr_types):
        self.attr_types = attr_types


class HYPERLOGLOG(types.TypeEngine):
    __visit_name__ = "HYPERLOGLOG"


class QDIGEST(types.TypeEngine):
    __visit_name__ = "QDIGEST"


class P4HYPERLOGLOG(types.TypeEngine):
    __visit_name__ = "P4HYPERLOGLOG"
