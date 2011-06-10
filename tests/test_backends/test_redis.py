from .. import BaseTest

from nydus.db.backends.redis import Redis
from nydus.db import ConnectionPool

class RedisTest(BaseTest):
    def setUp(self):
        self.redis = Redis(num=0, db_num=1)
        self.redis.flushdb()

    def test_proxy(self):
        self.assertEquals(self.redis.incr('RedisTest_proxy'), 1)
    
    def test_with_pool(self):
        p = ConnectionPool(
            hosts={0: self.redis},
        )
        self.assertEquals(p.incr('RedisTest_with_pool'), 1)