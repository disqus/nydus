from __future__ import absolute_import

from nydus.db import create_cluster
from nydus.db.base import BaseCluster
from nydus.db.backends.memcache import Memcache
from nydus.testutils import BaseTest

import mock
import pylibmc


class MemcacheTest(BaseTest):

    def setUp(self):
        self.memcache = Memcache(num=0)

    def test_provides_retryable_exceptions(self):
        self.assertEquals(Memcache.retryable_exceptions, frozenset([pylibmc.Error]))

    def test_provides_identifier(self):
        self.assertEquals(self.memcache.identifier, str(self.memcache.identifier))

    @mock.patch('pylibmc.Client')
    def test_client_instantiates_with_kwargs(self, Client):
        client = Memcache(num=0)
        client.connect()

        self.assertEquals(Client.call_count, 1)
        Client.assert_any_call(['localhost:11211'], binary=True, behaviors=None)

    @mock.patch('pylibmc.Client.get')
    def test_with_cluster(self, get):
        p = BaseCluster(
            backend=Memcache,
            hosts={0: {}},
        )
        result = p.get('MemcacheTest_with_cluster')
        get.assert_called_once_with('MemcacheTest_with_cluster')
        self.assertEquals(result, get.return_value)

    @mock.patch('pylibmc.Client')
    def test_map_does_pipeline(self, Client):
        cluster = create_cluster({
            'engine': 'nydus.db.backends.memcache.Memcache',
            'router': 'nydus.db.routers.RoundRobinRouter',
            'hosts': {
                0: {'binary': True},
                1: {'binary': True},
                2: {'binary': True},
                3: {'binary': True},
            }
        })

        with cluster.map() as conn:
            conn.set('a', 1)
            conn.set('b', 2)
            conn.set('c', 3)
            conn.set('d', 4)
            conn.set('e', 5)
            conn.set('f', 6)
            conn.set('g', 7)

        self.assertEqual(Client().set.call_count, 7)
        self.assertEqual(Client.call_count, 5)
        self.assertEqual(len(conn.get_results()), 7)

    @mock.patch('pylibmc.Client')
    def test_pipeline_get_multi(self, Client):
        cluster = create_cluster({
            'engine': 'nydus.db.backends.memcache.Memcache',
            'router': 'nydus.db.routers.RoundRobinRouter',
            'hosts': {
                0: {'binary': True},
                1: {'binary': True},
            }
        })

        keys = ['a', 'b', 'c', 'd', 'e', 'f']
        with cluster.map() as conn:
            for key in keys:
                conn.get(key)

        self.assertEqual(len(conn.get_results()), len(keys))
        self.assertEqual(Client().get.call_count, 0)
        # Note: This is two because it should execute the command once for each
        # of the two servers.
        self.assertEqual(Client().get_multi.call_count, 2)
