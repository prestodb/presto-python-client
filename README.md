![Build Status](https://travis-ci.org/prestodb/presto-python-client.svg?branch=master)

# Introduction

This package provides a client interface to query [Presto](https://prestodb.io/)
a distributed SQL engine. It supports Python 2.7, 3.5, 3.6, 3.7, and pypy.

# Installation

```
$ pip install presto-python-client
```

# Quick Start

Use the DBAPI interface to query Presto:

```python
import prestodb
conn=prestodb.dbapi.connect(
    host='localhost',
    port=8080,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
)
cur = conn.cursor()
cur.execute('SELECT * FROM system.runtime.nodes')
rows = cur.fetchall()
```

This will query the `system.runtime.nodes` system tables that shows the nodes
in the Presto cluster.

The DBAPI implementation in `prestodb.dbapi` provides methods to retrieve fewer
rows for example `Cursorfetchone()` or `Cursor.fetchmany()`. By default
`Cursor.fetchmany()` fetches one row. Please set
`prestodb.dbapi.Cursor.arraysize` accordingly.

# Basic Authentication
The `BasicAuthentication` class can be used to connect to a LDAP-configured Presto
cluster:
```python
import prestodb
conn=prestodb.dbapi.connect(
    host='coordinator url',
    port=8443,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
    http_scheme='https',
    auth=prestodb.auth.BasicAuthentication("principal id", "password"),
)
cur = conn.cursor()
cur.execute('SELECT * FROM system.runtime.nodes')
rows = cur.fetchall()
```

# Oauth Authentication
To enable GCS access, Oauth authentication support is added by passing in a `shadow.json` file of a service account.
Following example shows a use case where both Kerberos and Oauth authentication are enabled.

```python
import getpass
import prestodb
from prestodb.client import ClientSession, PrestoRequest, PrestoQuery
from requests_kerberos import DISABLED

kerberos_auth = prestodb.auth.KerberosAuthentication(
   mutual_authentication=DISABLED,
   service_name='kerberos service name',
   force_preemptive=True,
   hostname_override='example.com'
)

req = PrestoRequest(
    host='GCP coordinator url',
    port=443,
    client_session=ClientSession(user=getpass.getuser()),
    service_account_file='Service account json file path',
    http_scheme='https',
    auth=kerberos_auth
)

query = PrestoQuery(req, "SELECT * FROM system.runtime.nodes")
rows = list(query.execute())
```

# Transactions
The client runs by default in *autocommit* mode. To enable transactions, set
*isolation_level* to a value different than `IsolationLevel.AUTOCOMMIT`:

```python
import prestodb
from prestodb import transaction
with prestodb.dbapi.connect(
    host='localhost',
    port=8080,
    user='the-user',
    catalog='the-catalog',
    schema='the-schema',
    isolation_level=transaction.IsolationLevel.REPEATABLE_READ,
) as conn:
  cur = conn.cursor()
  cur.execute('INSERT INTO sometable VALUES (1, 2, 3)')
  cur.execute('INSERT INTO sometable VALUES (4, 5, 6)')
```

The transaction is created when the first SQL statement is executed.
`prestodb.dbapi.Connection.commit()` will be automatically called when the code
exits the *with* context and the queries succeed, otherwise
`prestodb.dbapi.Connection.rollback()' will be called.

# Improved Python types

If you enable the flag `experimental_python_types`, the client will convert the results of the query to the
corresponding Python types. For example, if the query returns a `DECIMAL` column, the result will be a `Decimal` object.

Limitations of the Python types are described in the
[Python types documentation](https://docs.python.org/3/library/datatypes.html). These limitations will generate an
exception `prestodb.exceptions.DataError` if the query returns a value that cannot be converted to the corresponding Python
type.

```python
import prestodb
import pytz
from datetime import datetime

conn = prestodb.dbapi.connect(
    experimental_python_types=True
    ...
)

cur = conn.cursor()

params = datetime(2020, 1, 1, 16, 43, 22, 320000, tzinfo=pytz.timezone('America/Los_Angeles'))

cur.execute("SELECT ?", params=(params,))
rows = cur.fetchall()

assert rows[0][0] == params
assert cur.description[0][1] == "timestamp with time zone"

# Running Tests

There is a helper scripts, `run`, that provides commands to run tests.
Type `./run tests` to run both unit and integration tests.

`presto-python-client` uses [pytest](https://pytest.org/) for its tests. To run
only unit tests, type:

```
$ pytest tests
```

Then you can pass options like `--pdb` or anything supported by `pytest --help`.

To run the tests with different versions of Python in managed *virtualenvs*,
use `tox` (see the configuration in `tox.ini`):

```
$ tox
```

To run integration tests, make sure the Docker daemon is running and then run:

```
$ pytest integration_tests
```

They build a `Docker` image and then run a container with a Presto server:
- the image is named `presto-server:${PRESTO_VERSION}`
- the container is named `presto-python-client-tests-{uuid4()[:7]}`

The container is expected to be removed after the tests are finished.

Please refer to the `Dockerfile` for details. You will find the configuration
in `etc/`.

You can use `./run` to manipulate the containers:

- `./run presto_server`: build and run Presto in a container
- `./run presto_cli CONTAINER_ID`: connect the Java Presto CLI to a container
- `./run list`: list the running containers
- `./run clean`: kill the containers

# Development

Start by forking the repository and then modify the code in your fork.
Please refer to `CONTRIBUTING.md` before submitting your contributions.

Clone the repository and go inside the code directory. Then you can get the
version with `python setup.py --version`.

We recommend that you use `virtualenv` to develop on `presto-python-client`:

```
$ virtualenv /path/to/env
$ /path/to/env/bin/activate
$ pip install -r requirements.txt
```

For development purpose, pip can reference the code you are modifying in a
*virtualenv*:

```
$ pip install -e .[tests]
```

That way, you do not need to run `pip install` again to make your changes
applied to the *virtualenv*.

When the code is ready, submit a Pull Request.

# Need Help?

Feel free to create an issue as it make your request visible to other users and contributors.

If an interactive discussion would be better or if you just want to hangout and chat about the Presto Python client, you can join us on the *#presto-python-client* channel on [Slack](https://prestodb.slack.com).
