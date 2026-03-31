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
# https://github.com/trinodb/trino-python-client/blob/master/trino/sqlalchemy/dialect.py

import re
from sqlalchemy import types, util, text
from sqlalchemy.engine import default
from sqlalchemy.sql import sqltypes

from prestodb import auth, dbapi
from prestodb.sqlalchemy import compiler, datatype

_type_map = {
    # Standard types
    "boolean": datatype.BOOLEAN,
    "tinyint": datatype.TINYINT,
    "smallint": datatype.SMALLINT,
    "integer": datatype.INTEGER,
    "bigint": datatype.BIGINT,
    "real": datatype.REAL,
    "double": datatype.DOUBLE,
    "decimal": datatype.DECIMAL,
    "varchar": datatype.VARCHAR,
    "char": datatype.CHAR,
    "varbinary": datatype.VARBINARY,
    "json": datatype.JSON,
    "date": datatype.DATE,
    "time": datatype.TIME,
    "time with time zone": datatype.TIME,  # TODO: time with time zone
    "timestamp": datatype.TIMESTAMP,
    "timestamp with time zone": datatype.TIMESTAMP,  # TODO: timestamp with time zone
    "interval year to month": datatype.INTERVAL,
    "interval day to second": datatype.INTERVAL,
    # Specific types
    "array": datatype.ARRAY,
    "map": datatype.MAP,
    "row": datatype.ROW,
    "hyperloglog": datatype.HYPERLOGLOG,
    "p4hyperloglog": datatype.P4HYPERLOGLOG,
    "qdigest": datatype.QDIGEST,
}


class PrestoDialect(default.DefaultDialect):
    name = "presto"
    driver = "presto"
    author = "Presto Team"
    supports_alter = False
    supports_pk_on_update = False
    supports_full_outer_join = True
    supports_simple_order_by_label = False
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_boolean = True

    statement_compiler = compiler.PrestoSQLCompiler
    type_compiler = compiler.PrestoTypeCompiler
    preparer = compiler.PrestoIdentifierPreparer
    
    def create_connect_args(self, url):
        args = {"host": url.host}
        if url.port:
            args["port"] = url.port
        if url.username:
            args["user"] = url.username
        if url.password:
            args["http_scheme"] = "https"
            args["auth"] = auth.BasicAuthentication(url.username, url.password)

        db_parts = (url.database or "system").split("/")
        if len(db_parts) == 1:
            args["catalog"] = db_parts[0]
        elif len(db_parts) == 2:
            args["catalog"] = db_parts[0]
            args["schema"] = db_parts[1]
        else:
            raise ValueError("Unexpected database format: {}".format(url.database))

        return ([args], {})

    @classmethod
    def import_dbapi(cls):
        return dbapi

    def has_table(self, connection, table_name, schema=None):
        return self._has_object(connection, "TABLE", table_name, schema)

    def has_sequence(self, connection, sequence_name, schema=None):
        return False

    def _has_object(self, connection, object_type, object_name, schema=None):
        if schema is None:
            schema = connection.engine.dialect.default_schema_name

        query = text(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name = :table"
        )
        return (
            connection.execute(
                query, {"schema": schema, "table": object_name}
            ).scalar()
            > 0
        )

    def get_schema_names(self, connection, **kw):
        result = connection.execute(
            text("SELECT schema_name FROM information_schema.schemata")
        )
        return [row[0] for row in result]

    def get_table_names(self, connection, schema=None, **kw):
        schema = schema or self.default_schema_name
        if schema is None:
            raise ValueError("schema argument is required")
        
        query = text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = :schema"
        )
        result = connection.execute(query, {"schema": schema})
        return [row[0] for row in result]

    def get_columns(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        if schema is None:
            raise ValueError("schema argument is required")
            
        query = text(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table "
            "ORDER BY ordinal_position"
        )
        result = connection.execute(query, {"schema": schema, "table": table_name})
        
        columns = []
        for row in result:
            col_name, col_type, is_nullable, default_val = row
            columns.append(
                {
                    "name": col_name,
                    "type": self._parse_type(col_type),
                    "nullable": is_nullable.lower() == "yes",
                    "default": default_val,
                }
            )
        return columns

    def _parse_type(self, type_str):
        type_str = type_str.lower()
        match = re.match(r"^([a-zA-Z0-9_ ]+)(\((.+)\))?$", type_str)
        if not match:
            return sqltypes.NullType()

        type_name = match.group(1).strip()
        type_args = match.group(3)

        if type_name in _type_map:
            type_class = _type_map[type_name]
            if type_args:
                return type_class(*self._parse_type_args(type_args))
            return type_class()
        return sqltypes.NullType()

    def _parse_type_args(self, type_args):
        # TODO: improve parsing for nested types
        return [int(a.strip()) if a.strip().isdigit() else a.strip() for a in type_args.split(",")]

    def do_rollback(self, dbapi_connection):
        # Presto transactions usually auto-commit or are read-only
        pass

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Presto doesn't enforce foreign keys
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Presto doesn't enforce primary keys
        return {"constrained_columns": [], "name": None}

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # TODO: Implement index reflection
        return []

    def do_ping(self, dbapi_connection):
        cursor = None
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        except Exception:
            if cursor:
                cursor.close()
            return False
        else:
            cursor.close()
            return True
