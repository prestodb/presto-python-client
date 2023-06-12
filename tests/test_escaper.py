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
from __future__ import absolute_import
import datetime
import prestodb.escaper

def test_escape_args():
    escaper = prestodb.escaper.ParamsEscaper()

    assert escaper.escape_args({'foo': 'bar'}) ==  {'foo': "'bar'"}
    assert escaper.escape_args({'foo': 123}) == {'foo': 123}
    assert escaper.escape_args({'foo': 123.456}) == {'foo': 123.456}
    assert escaper.escape_args({'foo': ['a', 'b', 'c']}) == {'foo': "('a','b','c')"}
    assert escaper.escape_args({'foo': ('a', 'b', 'c')}) == {'foo': "('a','b','c')"}
    assert escaper.escape_args({'foo': {'a', 'b'}}) in ({'foo': "('a','b')"}, {'foo': "('b','a')"})
    assert escaper.escape_args(('bar',)) == ("'bar'",)
    assert escaper.escape_args([123]) == (123,)
    assert escaper.escape_args((123.456,)) == (123.456,)
    assert escaper.escape_args((['a', 'b', 'c'],)) == ("('a','b','c')",)

    assert escaper.escape_args((datetime.date(2020, 4, 17),)) == ('date 2020-04-17',)
    assert escaper.escape_args((datetime.datetime(2020, 4, 17, 12, 0, 0, 123456),)) == ('timestamp 2020-04-17 12:00:00.123456',)
