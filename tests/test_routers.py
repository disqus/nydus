"""
tests.test_routers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from . import BaseTest
from nydus.db import Cluster
from nydus.db.backends import BaseConnection
from nydus.db.routers.redis import ConsistentHashingRouter
from nydus.db.routers.redis import RoundRobinRouter


class DummyConnection(BaseConnection):
    def __init__(self, i):
        self.host = 'dummyhost'
        self.i = i
        super(DummyConnection, self).__init__(i)

    @property
    def identifier(self):
        return "%s:%s" % (self.host, self.i)


class RoundRobinRouterTest(BaseTest):

    def setUp(self):
        self.router = RoundRobinRouter()
        self.hosts = dict((i, DummyConnection(i)) for i in range(5))
        self.cluster = Cluster(router=self.router, hosts=self.hosts)

    def get_db(self, *args, **kwargs):
        kwargs.setdefault('cluster', self.cluster)
        return self.router.get_db(*args, **kwargs)


class ConsistentHashingRouterTest(BaseTest):

    def setUp(self):
        self.router = ConsistentHashingRouter()
        self.hosts = dict((i, DummyConnection(i)) for i in range(5))
        self.cluster = Cluster(router=self.router, hosts=self.hosts)

    def get_db(self, **kwargs):
        kwargs.setdefault('cluster', self.cluster)
        return self.router.get_db(func='info', **kwargs)


class RoundRobinTest(BaseTest):

    def setUp(self):
        self.router = RoundRobinRouter()
        self.hosts = dict((i, DummyConnection(i)) for i in range(5))
        self.cluster = Cluster(router=self.router, hosts=self.hosts)

    def get_db(self, *args, **kwargs):
        kwargs.setdefault('cluster', self.cluster)
        return self.router.get_db(*args, **kwargs)

    def test_cluster_of_zero_returns_zero(self):
        self.cluster.hosts = dict()
        self.assertEquals([], self.get_db())

    def test_cluster_of_one_returns_one(self):
        self.cluster.hosts = {0: DummyConnection('foo')}
        self.assertEquals([0], [self.get_db(), ])

    def test_multi_node_cluster_returns_correct_host(self):
        self.cluster.hosts = {0: DummyConnection('foo'), 1: DummyConnection('bar')}
        self.assertEquals([0, 1, 0, 1], [self.get_db(), self.get_db(), self.get_db(), self.get_db(), ])


class InterfaceTest(ConsistentHashingRouterTest):

    def test_offers_router_interface(self):
        self.assertTrue(callable(self.router.get_db))

    def test_get_db_returns_itereable(self):
        iter(self.get_db())

    def test_returns_whole_cluster_without_key(self):
        self.assertEquals(range(5), self.get_db())

    def test_returns_sequence_with_one_item_when_given_key(self):
        self.assert_(len(self.get_db(key='foo')) is 1)


class HashingTest(ConsistentHashingRouterTest):

    def get_db(self, **kwargs):
        kwargs['key'] = 'foo'
        return super(HashingTest, self).get_db(**kwargs)

    def test_cluster_of_zero_returns_zero(self):
        self.cluster.hosts = dict()
        self.assertEquals([], self.get_db())

    def test_cluster_of_one_returns_one(self):
        self.cluster.hosts = dict(only_key=DummyConnection('foo'))
        self.assertEquals(['only_key'], self.get_db())

    def test_multi_node_cluster_returns_correct_host(self):
        self.assertEquals([2], self.get_db())


class RetryableTest(HashingTest):

    def test_attempt_reconnect_threshold_is_set(self):
        self.assertEqual(self.router.attempt_reconnect_threshold, 100000)

    def test_retry_gives_next_host_if_primary_is_offline(self):
        self.assertEquals([2], self.get_db())
        self.assertEquals([4], self.get_db(retry_for=2))

    def test_retry_host_change_is_sticky(self):
        self.assertEquals([2], self.get_db())
        self.assertEquals([4], self.get_db(retry_for=2))

        self.assertEquals([4], self.get_db())

    def test_adds_back_down_host_once_attempt_reconnect_threshold_is_passed(self):
        ConsistentHashingRouter.attempt_reconnect_threshold = 3

        self.assertEquals([2], self.get_db())
        self.assertEquals([4], self.get_db(retry_for=2))
        self.assertEquals([4], self.get_db())

        # Router should add host 1 back to the pool now
        self.assertEquals([2], self.get_db())

        ConsistentHashingRouter.attempt_reconnect_threshold = 100000

    def test_raises_host_list_exhaused_if_no_host_can_be_found(self):
        # Kill the first 4
        [self.get_db(retry_for=i) for i in range(4)]

        # And the 5th should raise an error
        self.assertRaises(
            ConsistentHashingRouter.HostListExhaused,
            self.get_db, **dict(retry_for=4))
