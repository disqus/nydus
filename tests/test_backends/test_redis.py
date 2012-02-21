"""
tests.test_backends.test_redis
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from .. import BaseTest

from nydus.db.backends.redis import Redis
from nydus.db import Cluster, create_cluster
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

    def test_pipelined_map(self):
        redis = create_cluster({
            'engine': 'nydus.db.backends.redis.Redis',
            'router': 'nydus.db.routers.redis.PartitionRouter',
            'hosts': {
                0: {'db': 5},
                1: {'db': 6},
                2: {'db': 7},
                3: {'db': 8},
                4: {'db': 9},
            }
        })
        chars = ('a', 'b', 'c', 'd', 'e', 'f')
        with redis.pipelined_map() as conn:
            [conn.set(c, i) for i, c in enumerate(chars)]
            res = [conn.get(c) for c in chars]
        self.assertEqual(range(len(chars)), [int(r._wrapped) for r in res])

