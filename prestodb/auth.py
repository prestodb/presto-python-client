from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import abc
import os

from futures.utils import with_metaclass

import requests_kerberos


class Authentication(with_metaclass(abc.ABCMeta)):
    def set_session(self, session):
        pass


class KerberosAuthentication(Authentication):
    def __init__(self, config, service_name, mutual_auth, ca_bundle):
        self._config = config
        self._service_name = service_name
        self._mutual_auth = mutual_auth
        self._ca_bundle = ca_bundle

    def set_session(self, session):
        os.environ['KRB5_CONFIG'] = self._config
        session.trust_env = False
        session.auth = requests_kerberos.HTTPKerberosAuth(
            mutual_authentication=self._mutual_auth,
            service=self._kerberos_service_name,
        )
        session.verify = self._ca_bundle = self._ca_bundle
        return session
