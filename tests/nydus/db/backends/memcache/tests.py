from __future__ import absolute_import

from tests import BaseTest

from nydus.db import create_cluster
from nydus.db.base import BaseCluster

from nydus.db.backends.memcache import Memcache

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

    def test_with_cluster(self):
        p = BaseCluster(hosts={0: self.memcache})
        self.assertEquals(p.get('MemcacheTest_with_cluster'), None)

    @mock.patch('pylibmc.Client')
    def test_map(self, Client):
        cluster = create_cluster({
            'engine': 'nydus.db.backends.memcache.Memcache',
            'router': 'nydus.db.routers.keyvalue.PartitionRouter',
            'hosts': {
                0: {'db': 0},
                1: {'db': 1},
            }
        })

        with cluster.map() as conn:
            conn.set('a', 1)
            conn.set('b', 2)

        self.assertTrue(Client().set.called)
