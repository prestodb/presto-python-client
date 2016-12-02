# Introduction

This package provides a client interface to query [Presto](https://prestodb.io/)
a distributed SQL engine. It supports Python 2.7 and Python 3.5.

# Installation

```
$ pip install presto-python-client
```

# Quick Start

Use the DBAPI interface to query Presto:

```python
conn = prestodb.dbapi.Connection(
        host=coordinator_hostname,
        port=coordinator_port,
    )
    cur = conn.cursor()
    cur.execute('select * from system.runtime.nodes')
    rows = cur.fetchall()
```

This will query the `system.runtime.nodes` system tables that shows the nodes
in the Presto cluster.

The DBAPI implementation in `prestodb.dbapi` provides methods to retrieve fewer
rows for example `Cursorfetchone()` or `Cursor.fetchmany()`. By default
`Cursor.fetchmany()` fetches one row. Please set
`prestodb.dbapi.Cursor.arraysize` accordingly.

# Running Tests

`presto-python-client` uses [pytest](https://pytest.org/) for its unit tests:

```
$ python setup.py test
```

or

```
$ pytest
```

Then you can pass options like `--pdb` or anything supported by `pytest --help`.

To run the tests with different versions of Python in managed *virtualenvs*
use `tox`:

```
$ tox
```

To run integration tests:

```
$ pytest integration_tests
```

They download a tarball of Presto and then executes it locally on a single node
i.e. the coordinator and a worker run on the same process. The tarball can be
cache in a custom directory. To enable this feature set the environment
variable `PRESTO_PYTHON_CLIENT_TEST_CACHE_DIR` to a directory that will
permanently stores the tarball (for example `presto-server-0.157.tar.gz`). You
are then responsible for cleaning this directory. Please make sure no malicious
user can leave a modified and untrusted version of a Presto tarball there as
integration tests would then execute malicious code. This is especially true
if you use a shared directory like `/tmp`.

# Development

Start by forking the repository and then modify the code in your fork.
Please refer to CONTRIBUTING.md before submitting your contributions.

Clone the repository and go inside the code directory. Then you can get the
version with `python setup.py --version`.

We recommend that you use `virtualenv` to develop on `presto-python-client`:

```
$ virtualenv /path/to/env
$ /path/to/env/bin/active
$ pip install -r requirements.txt
```

Now to link the version installed in the *virtualenv* to the file that you
currently modifies:

```
$ pipe install -e .
```

When the code is ready, submit a Pull Request.
