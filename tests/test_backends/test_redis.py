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

from .. import BaseTest

from nydus.db.backends.redis import Redis
from nydus.db import Cluster

class RedisTest(BaseTest):
    def setUp(self):
        self.redis = Redis(num=0, db_num=1)
        self.redis.flushdb()

    def test_proxy(self):
        self.assertEquals(self.redis.incr('RedisTest_proxy'), 1)
    
    def test_with_pool(self):
        p = Cluster(
            hosts={0: self.redis},
        )
        self.assertEquals(p.incr('RedisTest_with_pool'), 1)