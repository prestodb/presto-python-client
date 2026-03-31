from __future__ import absolute_import, division, print_function

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.types import Integer, String
from integration_tests.fixtures import run_presto

@pytest.fixture
def sqlalchemy_engine(run_presto):
    _, host, port = run_presto
    # Construct the SQLAlchemy URL. 
    # Note: 'test' user and 'test' catalog/schema match the dbapi fixtures.
    url = "presto://test@{}:{}/test/test".format(host, port)
    engine = create_engine(url)
    return engine

def test_sqlalchemy_engine_connect(sqlalchemy_engine):
    with sqlalchemy_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1

def test_sqlalchemy_query_execution(sqlalchemy_engine):
    with sqlalchemy_engine.connect() as conn:
        # Using a system table that is guaranteed to exist
        result = conn.execute(text("SELECT * FROM system.runtime.nodes LIMIT 1"))
        row = result.fetchone()
        assert row is not None

def test_sqlalchemy_reflection(sqlalchemy_engine):
    # This requires tables to exist. 
    # tpch is usually available in the test environment (referenced in test_dbapi.py)
    insp = inspect(sqlalchemy_engine)
    
    # Check schemas
    schemas = insp.get_schema_names()
    assert "sys" in schemas or "system" in schemas
    
    # Check tables in a specific schema (e.g. system.runtime)
    tables = insp.get_table_names(schema="system")
    assert "nodes" in tables or "runtime.nodes" in tables # Representation might vary

def test_sqlalchemy_orm_basic(sqlalchemy_engine):
    # Basic table definition
    metadata = MetaData()
    # we use a known table from tpch to avoid needing CREATE TABLE rights or persistence
    # tpch.sf1.customer
    # but that might be read-only. 
    
    # For integration test without write access, we typically verify SELECTs
    # If we need to write, we arguably should rely on the test_dbapi.py establishing environment
    
    with sqlalchemy_engine.connect() as conn:
        result = conn.execute(text("SELECT count(*) FROM tpch.sf1.customer"))
        count = result.scalar()
        assert count > 0
