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
# https://github.com/trinodb/trino-python-client/blob/master/trino/sqlalchemy/compiler.py

from sqlalchemy.sql import compiler


class PrestoSQLCompiler(compiler.SQLCompiler):
    def visit_char_length_func(self, fn, **kw):
        return "length{}".format(self.function_argspec(fn, **kw))

    def limit_clause(self, select, **kw):
        text = ""
        if select._limit_clause is not None:
            text += " LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text

    def visit_lambda_element(self, element, **kw):
        # Lambda expression not fully supported in standard SQLCompiler yet
        return super(PrestoSQLCompiler, self).visit_lambda_element(element, **kw)


class PrestoTypeCompiler(compiler.GenericTypeCompiler):
    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_REAL(self, type_, **kw):
        return "REAL"

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"

    def visit_SMALLINT(self, type_, **kw):
        return "SMALLINT"

    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_VARCHAR(self, type_, **kw):
        if type_.length is None:
            return "VARCHAR"
        return "VARCHAR(%d)" % type_.length

    def visit_CHAR(self, type_, **kw):
        if type_.length is None:
            return "CHAR"
        return "CHAR(%d)" % type_.length

    def visit_VARBINARY(self, type_, **kw):
        return "VARBINARY"

    def visit_JSON(self, type_, **kw):
        return "JSON"

    def visit_FLOAT(self, type_, **kw):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kw):
        if type_.precision is None:
            return "DECIMAL"
        if type_.scale is None:
            return "DECIMAL(%d)" % (type_.precision)
        return "DECIMAL(%d, %d)" % (type_.precision, type_.scale)

    def visit_DECIMAL(self, type_, **kw):
        return self.visit_NUMERIC(type_, **kw)

    def visit_DATE(self, type_, **kw):
        return "DATE"

    def visit_TIME(self, type_, **kw):
        return "TIME"

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"

    def visit_DATETIME(self, type_, **kw):
        return "TIMESTAMP"

    def visit_CLOB(self, type_, **kw):
        return "VARCHAR"

    def visit_NCLOB(self, type_, **kw):
        return "VARCHAR"

    def visit_TEXT(self, type_, **kw):
        return "VARCHAR"

    def visit_BLOB(self, type_, **kw):
        return "VARBINARY"

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_ARRAY(self, type_, **kw):
        return "ARRAY(%s)" % self.process(type_.item_type, **kw)

    def visit_MAP(self, type_, **kw):
        return "MAP(%s, %s)" % (
            self.process(type_.key_type, **kw),
            self.process(type_.value_type, **kw),
        )

    def visit_ROW(self, type_, **kw):
        items = [
            "%s %s" % (name, self.process(attr_type, **kw))
            for name, attr_type in type_.attr_types
        ]
        return "ROW(%s)" % ", ".join(items)

    def visit_HYPERLOGLOG(self, type_, **kw):
        return "HyperLogLog"

    def visit_QDIGEST(self, type_, **kw):
        return "QDigest"

    def visit_P4HYPERLOGLOG(self, type_, **kw):
        return "P4HyperLogLog"


class PrestoIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = {
        "alter",
        "and",
        "as",
        "between",
        "by",
        "case",
        "cast",
        "constraint",
        "create",
        "cross",
        "cube",
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        "deallocate",
        "delete",
        "describe",
        "distinct",
        "drop",
        "else",
        "end",
        "escape",
        "except",
        "execute",
        "exists",
        "extract",
        "false",
        "for",
        "from",
        "full",
        "group",
        "grouping",
        "having",
        "in",
        "inner",
        "insert",
        "intersect",
        "into",
        "is",
        "join",
        "left",
        "like",
        "localtime",
        "localtimestamp",
        "natural",
        "normalize",
        "not",
        "null",
        "on",
        "or",
        "order",
        "outer",
        "prepare",
        "recursive",
        "right",
        "rollup",
        "select",
        "table",
        "then",
        "true",
        "uescape",
        "union",
        "unnest",
        "using",
        "values",
        "when",
        "where",
        "with",
    }
