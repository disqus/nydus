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

from __future__ import absolute_import

from redis import Redis as RedisClient

from nydus.db.backends import BaseConnection

class Redis(BaseConnection):
    def __init__(self, host='localhost', port=6379, db=0, **options):
        self.host = host
        self.port = port
        self.db = db
        super(Redis, self).__init__(**options)

    def connect(self):
        return RedisClient(host=self.host, port=self.port, db=self.db)

    def disconnect(self):
        self.connection.disconnect()

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            # Send to router
            return getattr(self.connection, attr)
