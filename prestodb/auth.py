from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import abc
import os

from future.utils import with_metaclass
from typing import Any  # NOQA

import requests_kerberos


class Authentication(with_metaclass(abc.ABCMeta)):  # type: ignore
    @abc.abstractmethod
    def set_http_session(self, http_session):
        pass

    @abc.abstractmethod
    def set_client_session(self, client_session):
        pass

    @abc.abstractmethod
    def setup(self):
        pass

    def handle_err(self, error):
        pass


class KerberosAuthentication(Authentication):
    def __init__(
        self,
        config=None,  # type: Optional[Text]
        service_name=None,  # type: Text
        mutual_authentication=False,  # type: boolean
        force_preemptive=False,  # type: boolean
        hostname_override=None,  # type: Optional[Text]
        sanitize_mutual_error_response=True,  # type: boolean
        principal=None,  # type: Optional[Text]
        delegate=False,  # type: boolean
        ca_bundle=None,  # type: Optional[Text]
    ):
        self._config = config
        self._service_name = service_name
        self._mutual_authentication = mutual_authentication
        self._force_preemptive = force_preemptive
        self._hostname_override = hostname_override
        self._sanitize_mutual_error_response = sanitize_mutual_error_response
        self._principal = principal
        self._delegate = delegate
        self._ca_bundle = ca_bundle

    def set_client_session(self, client_session):
        pass

    def set_http_session(self, http_session):
        if self._config:
            os.environ['KRB5_CONFIG'] = self._config
        http_session.trust_env = False
        http_session.auth = requests_kerberos.HTTPKerberosAuth(
            mutual_authentication=self._mutual_authentication,
            force_preemptive=self._force_preemptive,
            hostname_override=self._hostname_override,
            sanitize_mutual_error_response=self._sanitize_mutual_error_response,
            principal=self._principal,
            delegate=self._delegate,
            service=self._service_name,
        )
        if self._ca_bundle:
            http_session.verify = self._ca_bundle
        return http_session

    def setup(self, presto_client):
        self.set_client_session(presto_client.client_session)
        self.set_http_session(presto_client.http_session)

    def handle_error(self, handle_error):
        pass
