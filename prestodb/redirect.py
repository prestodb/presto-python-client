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

import abc
import ipaddress
import socket
from typing import Any, Text  # NOQA

from six import with_metaclass
from six.moves.urllib_parse import urlparse


class RedirectHandler(with_metaclass(abc.ABCMeta)):  # type: ignore
    @abc.abstractmethod
    def handle(self, url):
        pass


class GatewayRedirectHandler(RedirectHandler):
    def handle(self, url):
        # type: (Text) -> Text
        if url is None:
            return None
        return url
