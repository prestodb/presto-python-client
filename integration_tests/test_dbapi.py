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
from __future__ import absolute_import, division, print_function

from datetime import date, datetime
import numpy as np
import prestodb
import pytest

import integration_tests.fixtures as fixtures
from integration_tests.fixtures import run_presto
from prestodb.transaction import IsolationLevel

TEST_SESSION_PROPERTIES = {
    # Need a property that allows supplying an arbitrary string
    # that requires URL encoding
    "spatial_partitioning_table_name": '{"DQ":{"dq_pending":true,"dq_insert_task_id":"di.universal_dq.testdqblocking.insert_task"}}',
    "query_max_run_time": "10m",
    "query_priority": "1",
}

@pytest.fixture
def presto_connection(run_presto):
    _, host, port = run_presto

    yield prestodb.dbapi.Connection(
        host=host, port=port, user="test", source="test", max_attempts=1
    )


@pytest.fixture
def presto_connection_with_transaction(run_presto):
    _, host, port = run_presto

    yield prestodb.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        max_attempts=1,
        isolation_level=IsolationLevel.READ_UNCOMMITTED,
    )


def test_select_query(presto_connection):
    cur = presto_connection.cursor()
    cur.execute("select * from system.runtime.nodes")
    rows = cur.fetchall()
    assert len(rows) > 0
    row = rows[0]
    assert row[0] == "test"
    assert row[2].split("-")[0] == fixtures.PRESTO_VERSION
    columns = dict([desc[:2] for desc in cur.description])
    assert columns["node_id"] == "varchar"
    assert columns["http_uri"] == "varchar"
    assert columns["node_version"] == "varchar"
    assert columns["coordinator"] == "boolean"
    assert columns["state"] == "varchar"


def test_select_query_result_iteration(presto_connection):
    cur0 = presto_connection.cursor()
    cur0.execute("select custkey from tpch.sf1.customer LIMIT 10")
    rows0 = cur0.genall()

    cur1 = presto_connection.cursor()
    cur1.execute("select custkey from tpch.sf1.customer LIMIT 10")
    rows1 = cur1.fetchall()

    assert len(list(rows0)) == len(rows1)


def test_select_query_no_result(presto_connection):
    cur = presto_connection.cursor()
    cur.execute("select * from system.runtime.nodes where false")
    rows = cur.fetchall()
    assert len(rows) == 0


def test_select_query_stats(presto_connection):
    cur = presto_connection.cursor()
    cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")

    query_id = cur.stats["queryId"]
    completed_splits = cur.stats["completedSplits"]
    cpu_time_millis = cur.stats["cpuTimeMillis"]
    processed_bytes = cur.stats["processedBytes"]
    processed_rows = cur.stats["processedRows"]
    wall_time_millis = cur.stats["wallTimeMillis"]

    while cur.fetchone() is not None:
        assert query_id == cur.stats["queryId"]
        assert completed_splits <= cur.stats["completedSplits"]
        assert cpu_time_millis <= cur.stats["cpuTimeMillis"]
        assert processed_bytes <= cur.stats["processedBytes"]
        assert processed_rows <= cur.stats["processedRows"]
        assert wall_time_millis <= cur.stats["wallTimeMillis"]

        query_id = cur.stats["queryId"]
        completed_splits = cur.stats["completedSplits"]
        cpu_time_millis = cur.stats["cpuTimeMillis"]
        processed_bytes = cur.stats["processedBytes"]
        processed_rows = cur.stats["processedRows"]
        wall_time_millis = cur.stats["wallTimeMillis"]


def test_select_failed_query(presto_connection):
    cur = presto_connection.cursor()
    with pytest.raises(prestodb.exceptions.PrestoUserError):
        cur.execute("select * from catalog.schema.do_not_exist")
        cur.fetchall()

def test_select_query_result_iteration_statement_params(presto_connection):
    cur = presto_connection.cursor()
    cur.execute(
        """
        select * from (
            values
            (1, 'one', 'a'),
            (2, 'two', 'b'),
            (3, 'three', 'c'),
            (4, 'four', 'd'),
            (5, 'five', 'e')
        ) x (id, name, letter)
        where id >= ?
        """,
        params=(3,)  # expecting all the rows with id >= 3
    )

    rows = cur.fetchall()
    assert len(rows) == 3

    for row in rows:
        # Validate that all the ids of the returned rows are greather or equals than 3
        assert row[0] >= 3


def test_select_query_param_types(presto_connection):
    cur = presto_connection.cursor()

    date_param = date.today()
    timestamp_param = datetime.now().replace(microsecond=0)
    float_param = 1.5
    list_param = (1,2,3)
    cur.execute(
        """
        select ?,?,?,?
        """,
        params=(date_param, timestamp_param, float_param, list_param,)
    )

    rows = cur.fetchall()
    assert len(rows) == 1
    for row in rows:
        assert date.fromisoformat(row[0]) == date_param
        assert datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f") == timestamp_param
        assert row[2] == float_param
        assert (row[3] == np.array(list_param)).all()

@pytest.mark.parametrize('params', [
    'NOT A LIST OR TUPPLE',
    {'invalid', 'params'},
    object,
])
def test_select_query_invalid_params(presto_connection, params):
    cur = presto_connection.cursor()
    with pytest.raises(AssertionError):
        cur.execute('select ?', params=params)

        
def test_select_tpch_1000(presto_connection):
    cur = presto_connection.cursor()
    cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
    rows = cur.fetchall()
    assert len(rows) == 1000


def test_cancel_query(presto_connection):
    cur = presto_connection.cursor()
    cur.execute("select * from tpch.sf1.customer")
    cur.cancel()  # would raise an exception if cancel fails

    cur = presto_connection.cursor()
    with pytest.raises(Exception) as cancel_error:
        cur.cancel()
    assert "Cancel query failed; no running query" in str(cancel_error.value)


def test_session_properties(run_presto):
    _, host, port = run_presto

    connection = prestodb.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        session_properties=TEST_SESSION_PROPERTIES,
        max_attempts=1,
    )
    cur = connection.cursor()
    cur.execute("SHOW SESSION")
    rows = cur.fetchall()
    assert len(rows) > 2
    for prop, value, _, _, _ in rows:
        if prop == "query_max_run_time":
            assert value == "10m"
        elif prop == "query_priority":
            assert value == "1"


def test_transaction_single(presto_connection_with_transaction):
    connection = presto_connection_with_transaction
    for _ in range(3):
        cur = connection.cursor()
        cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows = cur.fetchall()
        connection.commit()
        assert len(rows) == 1000


def test_transaction_rollback(presto_connection_with_transaction):
    connection = presto_connection_with_transaction
    for _ in range(3):
        cur = connection.cursor()
        cur.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows = cur.fetchall()
        connection.rollback()
        assert len(rows) == 1000


def test_transaction_multiple(presto_connection_with_transaction):
    with presto_connection_with_transaction as connection:
        cur1 = connection.cursor()
        cur1.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows1 = cur1.fetchall()

        cur2 = connection.cursor()
        cur2.execute("SELECT * FROM tpch.sf1.customer LIMIT 1000")
        rows2 = cur2.fetchall()

    assert len(rows1) == 1000
    assert len(rows2) == 1000
