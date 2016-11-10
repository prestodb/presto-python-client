from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import fixtures
from fixtures import run_presto
import prestodb


def test_select_query(run_presto):
    conn = prestodb.dbapi.Connection(
        host=fixtures.PRESTO_HOST,
        port=fixtures.PRESTO_PORT,
        user='test',
        source='test',
    )
    cur = conn.cursor()
    cur.execute('select * from system.runtime.nodes')
    rows = cur.fetchall()
    assert len(rows) > 0
    row = rows[0]
    assert row[0] == 'ffffffff-ffff-ffff-ffff-ffffffffffff'
    assert row[2] == fixtures.PRESTO_VERSION
    columns = dict(cur.description)
    assert columns['node_id'] == 'varchar'
    assert columns['http_uri'] == 'varchar'
    assert columns['node_version'] == 'varchar'
    assert columns['coordinator'] == 'boolean'
    assert columns['state'] == 'varchar'
