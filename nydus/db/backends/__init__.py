# Copyright 2011 DISQUS
#
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

class BaseConnection(object):
    """
    Base connection class.

    Child classes should implement at least
    connect() and disconnect() methods.
    """
    def __init__(self, num, **options):
        self._connection = None
        self.num = num

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.connect()
        return self._connection
    
    def close(self):
        if self._connection:
            self.disconnect()
        self._connection = None

    def connect(self):
        raise NotImplementedError

    def disconnect(self):
        raise NotImplementedError