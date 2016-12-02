"""
This module implements the Python DBAPI 2.0 as described in
https://www.python.org/dev/peps/pep-0249/ .

Fetch methods returns rows as a list of lists on purpose to let the caller
decide to convert then to a list of tuples.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

from future.builtins import range

from prestodb import constants
import prestodb.exceptions
import prestodb.client


__all__ = ['connect', 'Connection', 'Cursor']


apilevel = '2.0'
threadsafety = 2

logger = logging.getLogger(__name__)


class Error(Exception):
    pass


class DatabaseError(Error):
    pass


class OperationalError(Error):
    """

    Exception raised for errors that are related to the database's operation
    and not necessarily under the control of the programmer, e.g. an unexpected
    disconnect occurs, the data source name is not found, a transaction could
    not be processed, a memory allocation error occurred during processing, ...

    """
    pass


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database.

    See class :py:class:`Connection` for arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    """Presto does not have a notion of a persistent connection.

    Thus, these objects are small stateless factories for cursors, which do all
    the real work.

    """
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def close(self):
        """Presto does not have anything to close"""
        # TODO cancel outstanding queries?
        pass

    def commit(self):
        """FIXME: Not supported yet"""
        raise NotImplementedError

    def cursor(self):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(*self._args, **self._kwargs)

    def rollback(self):
        """FIXME: Not supported yet"""
        raise NotImplementedError


class Cursor(object):
    """Database cursor.

    Cursors are not isolated, i.e., any changes done to the database by a
    cursor are immediately visible by other cursors or connections.
    """

    def __init__(
        self,
        host,
        port=constants.DEFAULT_PORT,
        user=None,
        source=constants.DEFAULT_SOURCE,
        catalog=constants.DEFAULT_CATALOG,
        schema=constants.DEFAULT_SCHEMA,
        session_properties=None,
        http_scheme=constants.HTTP,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._source = source
        self._catalog = catalog
        self._schema = schema
        self._session_properties = session_properties
        self._http_scheme = http_scheme

        self.arraysize = 1
        self._rows = None

    @property
    def description(self):
        if self._query.columns is None:
            return None

        return [
            (col['name'], col['type']) for col in self._query.columns
        ]

    @property
    def rowcount(self):
        """Not supported.

        Presto cannot reliablity determine the number of rows returned by an
        operation. For example, the result of a SELECT query is streamed and
        the number of rows is only knowns when all rows have been retrieved.
        """
        return -1

    def setinputsizes(self, sizes):
        """Not supported"""
        pass

    def setoutputsize(self, size, column):
        """Not supported"""
        pass

    def execute(self, operation, params=None):
        request = prestodb.client.PrestoRequest(
            self._host,
            self._port,
            self._user,
            self._source,
            self._catalog,
            self._schema,
            self._session_properties,
            self._http_scheme,
        )

        self._query = prestodb.client.PrestoQuery(request, sql=operation)
        return self._query.execute()

    def fetchone(self):
        # () -> Optional[List[Any]]
        """

        PEP-0249: Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.
        """

        try:
            return next(self._query.result)
        except StopIteration:
            return None
        except prestodb.exceptions.HttpError as err:
            raise OperationalError(str(err))

    def fetchmany(self, size=None):
        # type: Optional[int] -> List[List[Any]]
        """
        PEP-0249: Fetch the next set of rows of a query result, returning a
        sequence of sequences (e.g. a list of tuples). An empty sequence is
        returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If
        it is not given, the cursor's arraysize determines the number of rows
        to be fetched. The method should try to fetch as many rows as indicated
        by the size parameter. If this is not possible due to the specified
        number of rows not being available, fewer rows may be returned.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        Note there are performance considerations involved with the size
        parameter. For optimal performance, it is usually best to use the
        .arraysize attribute. If the size parameter is used, then it is best
        for it to retain the same value from one .fetchmany() call to the next.
        """

        if size is None:
            size = self.arraysize

        result = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        if not self._query.result.rownumber == 0:
            raise DatabaseError('no rows returned')
        return result

    def fetchall(self):
        # type: () -> List[List[Any]]
        return list(self._query.result)
