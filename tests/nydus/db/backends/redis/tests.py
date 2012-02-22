from __future__ import absolute_import

from tests import BaseTest

from nydus.db.backends.redis import Redis
from nydus.db.base import Cluster
import redis


class RedisTest(BaseTest):
    def setUp(self):
        self.redis = Redis(num=0, db_num=1)
        self.redis.flushdb()

    def test_proxy(self):
        self.assertEquals(self.redis.incr('RedisTest_proxy'), 1)

    def test_with_cluster(self):
        p = Cluster(
            hosts={0: self.redis},
        )
        self.assertEquals(p.incr('RedisTest_with_cluster'), 1)

    def test_provides_retryable_exceptions(self):
        self.assertEquals(Redis.retryable_exceptions, redis.exceptions)

    def test_provides_identifier(self):
        self.assertEquals(self.redis.identifier, str(self.redis.identifier))
