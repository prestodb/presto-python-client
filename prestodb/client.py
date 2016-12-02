"""
This module implements the Presto protocol to submit SQL statements, track
their state and retrieve their result as described in
https://github.com/prestodb/presto/wiki/HTTP-Protocol
and Presto source code.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from future.moves.urllib.parse import urlunparse
import logging

import backoff
import requests

from prestodb import constants
from prestodb import exceptions


__all__ = ['PrestoResult', 'PrestoRequest', 'PrestoQuery']


MAX_TRIES = 5
logger = logging.getLogger(__name__)


class PrestoStatus(object):
    def __init__(self, id, stats, info_uri, next_uri, rows, columns=None):
        self.id = id
        self.stats = stats
        self.info_uri = info_uri
        self.next_uri = next_uri
        self.rows = rows
        self.columns = columns

    def __repr__(self):
        return (
            'PrestoStatus('
            'id={}, stats={{...}}, info_uri={}, next_uri={}, rows=<count={}>'
            ')'.format(
                self.id,
                self.info_uri,
                self.next_uri,
                len(self.rows),
            )
        )


class PrestoRequest(object):
    http = requests

    def __init__(
        self,
        host,
        port,
        user,
        source,
        catalog,
        schema,
        session_properties,
        http_scheme,
        auth=None,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._source = source
        self._catalog = catalog
        self._schema = schema
        self._session_properties = session_properties
        self._http_scheme = http_scheme

        self._next_uri = None
        self._session = self.http.Session()
        self._session.headers.update(self.headers)
        self._auth = auth
        if self._auth:
            if http_scheme == constants.HTTP:
                raise ValueError('cannot use authentication with HTTP')
            self._auth.set_session(self._session)

    def get_url(self, path):
        return urlunparse(
            (
                self._http_scheme,
                '{}:{}'.format(self._host, self._port),
                path,
                '',
                '',
                '',
            )
        )

    @property
    def statement_url(self):
        return self.get_url(constants.URL_STATEMENT_PATH)

    @property
    def next_uri(self):
        return self._next_uri

    @property
    def headers(self):
        headers = {
            'X-Presto-Catalog': self._catalog,
            'X-Presto-Schema': self._schema,
            'X-Presto-Source': self._source,
            'X-Presto-User': self._user,
        }

        if self._session_properties:
            headers['X-Presto-Session'] = ','.join(
                '{}={}'.format(name, value)
                for name, value in self._session_properties.items()
            )

        return headers

    def post(self, sql):
        return self._session.post(
            self.statement_url,
            data=sql.encode('utf-8'),
        )

    def get(self, url):
        return self._session.get(url)

    def _process_error(self, error):
        error_type = error['errorType']
        if error_type == 'EXTERNAL':
            raise exceptions.PrestoExternalError(error)
        elif error_type == 'USER_ERROR':
            return exceptions.PrestoUserError(error)

        return exceptions.PrestoQueryError(error)

    def process(self, http_response):
        status_code = http_response.status_code
        if status_code != self.http.codes.ok:
            if status_code == 503:
                raise exceptions.Http503Error('service unavailable')

            raise exceptions.HttpError(
                'error {}: {}'.format(
                    http_response.status_code,
                    http_response.content,
                )
            )

        http_response.encoding = 'utf-8'
        response = http_response.json()
        logger.debug('HTTP {}: {}'.format(http_response.status_code, response))
        if 'error' in response:
            error = response['error']

            raise self._process_error(error)

        if 'X-Presto-Clear-Session' in http_response.headers:
            propname = response.headers['X-Presto-Clear-Session']
            self._session_properties.pop(propname, None)

        if 'X-Presto-Set-Session' in http_response.headers:
            set_session_header = response.headers['X-Presto-Set-Session']
            name, value = set_session_header.split('=', 1)
            self._session_properties[name] = value

        self._next_uri = response.get('nextUri')

        return PrestoStatus(
            id=response['id'],
            stats=response['stats'],
            info_uri=response['infoUri'],
            next_uri=self._next_uri,
            rows=response.get('data', []),
            columns=response.get('columns'),
        )


class PrestoQuery(object):
    def __init__(self, request, sql):
        self.query_id = None
        self.state = None

        self._columns = None

        self._finished = False
        self._request = request
        self._sql = sql

    @property
    def columns(self):
        return self._columns

    @backoff.on_exception(
        backoff.expo,
        exception=(
            exceptions.Http503Error,
            PrestoRequest.http.ConnectionError,
        ),
        max_tries=MAX_TRIES,
    )
    def execute(self):
        # type: () -> PrestoResult
        """Initiate a Presto query by sending the SQL statement

        This is the first HTTP request sent to the coordinator.
        It sets the query_id and returns a Result object used to
        track the rows returned by the query. To fetch all rows,
        call fetch() until is_finished is true.

        """

        response = self._request.post(self._sql)
        status = self._request.process(response)
        self.query_id = status.id
        self.state = status.stats['state']
        if status.next_uri is None:
            self._finished = True
        self.result = PrestoResult(self, status.rows)
        return self.result

    @backoff.on_exception(
        backoff.expo,
        exception=(
            exceptions.Http503Error,
            PrestoRequest.http.ConnectionError
        ),
        max_tries=MAX_TRIES,
    )
    def fetch(self):
        # type: () -> List[List[Any]]
        """Continue fetching data for the current query_id

        """

        response = self._request.get(self._request.next_uri)
        status = self._request.process(response)
        if status.columns:
            self._columns = status.columns
        self.state = status.stats['state']
        logger.debug(status)
        if status.next_uri is None:
            self._finished = True
        return status.rows

    def is_finished(self):
        # type: () -> bool
        return self._finished


class PrestoResult(object):
    def __init__(self, query, rows=None):
        self._query = query
        self._rows = rows or []
        self._rownumber = 0

        self._it = None

    @property
    def rownumber(self):
        # () -> int
        return self._rownumber

    def __iter__(self):
        # Initial fetch from the first POST request
        for row in self._rows:
            self._rownumber += 1
            yield row

        # Subsequent fetches from GET requests until next_uri is empty.
        while not self._query.is_finished():
            rows = self.fetch()
            for row in rows:
                self._rownumber += 1
                logger.debug('row {}'.format(row))
                yield row

    def __next__(self):
        if self._it is None:
            self._it = self.__iter__()
        return next(self._it)

    def fetch(self):
        # () -> List[List[Any]]
        rows = self._query.fetch()
        return rows
