from __future__ import absolute_import

from tests import BaseTest

from nydus.db.backends.redis import Redis
from nydus.db.base import Cluster, create_cluster
import mock
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
        with redis.map() as conn:
            [conn.set(c, i) for i, c in enumerate(chars)]
            res = [conn.get(c) for c in chars]
        self.assertEqual(range(len(chars)), [int(r._wrapped) for r in res])

    @mock.patch('nydus.db.backends.redis.RedisClient')
    def test_client_instantiates_with_kwargs(self, RedisClient):
        client = Redis(num=0)
        client.connect()

        self.assertEquals(RedisClient.call_count, 1)
        RedisClient.assert_any_call(host='localhost', port=6379, db=0, socket_timeout=None)

    @mock.patch('nydus.db.backends.redis.RedisClient')
    def test_map_does_pipeline(self, RedisClient):
        redis = create_cluster({
            'engine': 'nydus.db.backends.redis.Redis',
            'router': 'nydus.db.routers.redis.PartitionRouter',
            'hosts': {
                0: {'db': 0},
                1: {'db': 1},
            }
        })
        with redis.map() as conn:
            conn.set('a', 0)
            conn.set('d', 1)

        # ensure this was actually called through the pipeline
        self.assertFalse(RedisClient().set.called)

        self.assertEquals(RedisClient().pipeline.call_count, 2)
        RedisClient().pipeline.assert_called_with()

        self.assertEquals(RedisClient().pipeline().set.call_count, 2)
        RedisClient().pipeline().set.assert_any_call('a', 0)
        RedisClient().pipeline().set.assert_any_call('d', 1)

        self.assertEquals(RedisClient().pipeline().execute.call_count, 2)
        RedisClient().pipeline().execute.assert_called_with()

    @mock.patch('nydus.db.backends.redis.RedisClient')
    def test_map_only_runs_on_required_nodes(self, RedisClient):
        redis = create_cluster({
            'engine': 'nydus.db.backends.redis.Redis',
            'router': 'nydus.db.routers.redis.PartitionRouter',
            'hosts': {
                0: {'db': 0},
                1: {'db': 1},
            }
        })
        with redis.map() as conn:
            conn.set('a', 0)
            conn.set('b', 1)

        # ensure this was actually called through the pipeline
        self.assertFalse(RedisClient().set.called)

        self.assertEquals(RedisClient().pipeline.call_count, 1)
        RedisClient().pipeline.assert_called_with()

        self.assertEquals(RedisClient().pipeline().set.call_count, 2)
        RedisClient().pipeline().set.assert_any_call('a', 0)
        RedisClient().pipeline().set.assert_any_call('b', 1)

        self.assertEquals(RedisClient().pipeline().execute.call_count, 1)
        RedisClient().pipeline().execute.assert_called_with()
