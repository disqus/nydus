from __future__ import absolute_import

from nydus.db import create_cluster
from nydus.db.base import BaseCluster
from nydus.db.backends.redis import Redis
from nydus.db.promise import EventualCommand
from nydus.testutils import BaseTest, fixture
import mock
import redis as redis_


class RedisPipelineTest(BaseTest):
    @fixture
    def cluster(self):
        return create_cluster({
            'backend': 'nydus.db.backends.redis.Redis',
            'router': 'nydus.db.routers.keyvalue.PartitionRouter',
            'hosts': {
                0: {'db': 5},
                1: {'db': 6},
                2: {'db': 7},
                3: {'db': 8},
                4: {'db': 9},
            }
        })

    # XXX: technically we're testing the Nydus map code, and not ours
    def test_pipelined_map(self):
        chars = ('a', 'b', 'c', 'd', 'e', 'f')
        with self.cluster.map() as conn:
            [conn.set(c, i) for i, c in enumerate(chars)]
            res = [conn.get(c) for c in chars]
        self.assertEqual(range(len(chars)), [int(r) for r in res])

    def test_map_single_connection(self):
        with self.cluster.map() as conn:
            conn.set('a', '1')
        self.assertEquals(self.cluster.get('a'), '1')

    def test_no_proxy_without_call_on_map(self):
        with self.cluster.map() as conn:
            result = conn.incr

        assert type(result) is EventualCommand
        assert not result.was_called()


class RedisTest(BaseTest):

    def setUp(self):
        self.redis = Redis(num=0, db=1)
        self.redis.flushdb()

    def test_proxy(self):
        self.assertEquals(self.redis.incr('RedisTest_proxy'), 1)

    def test_with_cluster(self):
        p = BaseCluster(
            backend=Redis,
            hosts={0: {'db': 1}},
        )
        self.assertEquals(p.incr('RedisTest_with_cluster'), 1)

    def test_provides_retryable_exceptions(self):
        self.assertEquals(Redis.retryable_exceptions, frozenset([redis_.ConnectionError, redis_.InvalidResponse]))

    def test_provides_identifier(self):
        self.assertEquals(self.redis.identifier, str(self.redis.identifier))

    @mock.patch('nydus.db.backends.redis.StrictRedis')
    def test_client_instantiates_with_kwargs(self, RedisClient):
        client = Redis(num=0)
        client.connect()

        self.assertEquals(RedisClient.call_count, 1)
        RedisClient.assert_any_call(host='localhost', port=6379, db=0, socket_timeout=None,
            password=None, unix_socket_path=None)

    @mock.patch('nydus.db.backends.redis.StrictRedis')
    def test_map_does_pipeline(self, RedisClient):
        redis = create_cluster({
            'backend': 'nydus.db.backends.redis.Redis',
            'router': 'nydus.db.routers.keyvalue.PartitionRouter',
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

    @mock.patch('nydus.db.backends.redis.StrictRedis')
    def test_map_only_runs_on_required_nodes(self, RedisClient):
        redis = create_cluster({
            'engine': 'nydus.db.backends.redis.Redis',
            'router': 'nydus.db.routers.keyvalue.PartitionRouter',
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

    def test_normal_exceptions_dont_break_the_cluster(self):
        redis = create_cluster({
            'engine': 'nydus.db.backends.redis.Redis',
            'router': 'nydus.db.routers.keyvalue.ConsistentHashingRouter',
            'hosts': {
                0: {'db': 0},
                1: {'db': 1},
            }
        })

        # Create a normal key
        redis.set('a', 0)

        with self.assertRaises(redis_.ResponseError):
            # We are going to preform an operation on a key that is not a set
            # This call *should* raise the actual Redis exception, and
            # not continue on to think the host is down.
            redis.scard('a')

        # This shouldn't raise a HostListExhausted exception
        redis.get('a')

    def test_custom_identifier_specified(self):
        cluster_config = {
            'backend': 'nydus.db.backends.redis.Redis',
            'hosts': {
                0: {'db': 0, 'identifier': 'redis://127.0.0.1:6379/0'},
                1: {'db': 1, 'identifier': 'redis://127.0.0.1:6380/1'},
            },
        }

        redis = create_cluster(cluster_config)
        for idx in cluster_config['hosts'].keys():
            self.assertEquals(redis.hosts[idx].identifier,
                              cluster_config['hosts'][idx]['identifier'])
