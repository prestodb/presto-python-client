import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from prestodb.sqlalchemy.base import PrestoDialect
from prestodb import auth
import prestodb.dbapi

# Mocking the interaction with the DBAPI for unit tests
class MockCursor:
    def __init__(self, display_list=None):
        self.description = []
        self._display_list = display_list or []

    def execute(self, operation, parameters=None):
        pass

    def fetchall(self):
        return self._display_list
    
    def close(self):
        pass

class MockConnection:
    def __init__(self, host, **kwargs):
        self.host = host
        self.kwargs = kwargs
        self._cursor = MockCursor()

    def cursor(self):
        return self._cursor
    
    def close(self):
        pass
    
    def commit(self):
        pass

@pytest.fixture
def mock_dbapi(monkeypatch):
    def connect(*args, **kwargs):
        return MockConnection(*args, **kwargs)
    
    monkeypatch.setattr(prestodb.dbapi, "connect", connect)
    return prestodb.dbapi

def test_engine_creation():
    url = "presto://user:password@localhost:8080/catalog/schema"
    # Registry might not be loaded in test environment without setup.py install,
    # so we might need to manually register if it fails, but ideally via entrypoints.
    # For unit test, we can pass the dialect class directly to create_engine is not robust
    # but for "presto://", it relies on entry points.
    # Alternatively we rely on the implementation in base which imports dbapi.
    
    # We will test the dialect logic directly first
    dialect = PrestoDialect()
    u = make_url(url)
    connect_args = dialect.create_connect_args(u)
    
    args = connect_args[0][0]
    assert args["host"] == "localhost"
    assert args["port"] == 8080
    assert args["user"] == "user"
    assert args["catalog"] == "catalog"
    assert args["schema"] == "schema"
    assert args["http_scheme"] == "https"
    assert isinstance(args["auth"], auth.BasicAuthentication)

def test_type_parsing():
    dialect = PrestoDialect()
    
    # Simple types
    assert str(dialect._parse_type("integer")) == "INTEGER"
    assert str(dialect._parse_type("varchar")) == "VARCHAR"
    
    # Types with args
    assert str(dialect._parse_type("varchar(255)")) == "VARCHAR(255)"
    assert str(dialect._parse_type("decimal(10, 2)")) == "DECIMAL(10, 2)"
    
    # Multi-word types (Fixed by recent patch)
    assert str(dialect._parse_type("timestamp with time zone")) == "TIMESTAMP"
    assert str(dialect._parse_type("time with time zone")) == "TIME"

def test_type_parsing_case_insensitive():
    dialect = PrestoDialect()
    assert str(dialect._parse_type("INTEGER")) == "INTEGER"
    assert str(dialect._parse_type("Varchar(10)")) == "VARCHAR(10)"

def test_reflection_queries_generated():
    # Verify that reflection methods generate the expected SQL (using info schema)
    # We can mock the connection.execute and check the query
    dialect = PrestoDialect()
    
    class InspectableConnection:
        def __init__(self):
            self.queries = []
            self.engine = type('Engine', (), {'dialect': dialect})()
            
        def execute(self, sql, params=None):
            self.queries.append((sql, params))
            return type('Result', (), {'scalar': lambda: 0, '__iter__': lambda x: iter([])})()
            
    conn = InspectableConnection()
    
    dialect.get_table_names(conn, schema="test_schema")
    last_query, last_params = conn.queries[-1]
    assert "information_schema.tables" in str(last_query)
    assert last_params["schema"] == "test_schema"
    
    dialect.get_columns(conn, "test_table", schema="test_schema")
    last_query, last_params = conn.queries[-1]
    assert "information_schema.columns" in str(last_query)
    assert last_params["table"] == "test_table"
