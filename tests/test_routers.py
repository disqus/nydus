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
from nose import tools


class DummyConnection(BaseConnection):
    def __init__(self, i):
        self.host = 'dummyhost'
        self.i = i
        super(DummyConnection, self).__init__(i)

    @property
    def identifier(self):
        return "%s:%s" % (self.host, self.i)


class ConsistentHashingRouterTest(BaseTest):

    def setUp(self):
        self.router = ConsistentHashingRouter()
        self.hosts = dict((i, DummyConnection(i)) for i in range(5))
        self.cluster = Cluster(router=self.router, hosts=self.hosts)

    def get_db(self, **kwargs):
        kwargs.setdefault('cluster', self.cluster)
        return self.router.get_db(func='info', **kwargs)


class InterfaceTest(ConsistentHashingRouterTest):

    def test_offers_router_interface(self):
        tools.assert_true(callable(self.router.get_db))

    def test_get_db_returns_itereable(self):
        iter(self.get_db())

    def test_returns_whole_cluster_without_key(self):
        tools.assert_items_equal(range(5), self.get_db())

    def test_returns_sequence_with_one_item_when_given_key(self):
        tools.ok_(len(self.get_db(key='foo')) is 1)

class HashingTest(ConsistentHashingRouterTest):

    def get_db(self, **kwargs):
        kwargs['key'] = 'foo'
        return super(HashingTest, self).get_db(**kwargs)

    def test_cluster_of_zero_returns_zero(self):
        self.cluster.hosts = dict()
        tools.assert_items_equal([], self.get_db())

    def test_cluster_of_one_returns_one(self):
        self.cluster.hosts = dict(only_key=DummyConnection('foo'))
        tools.assert_items_equal(['only_key'], self.get_db())

    def test_multi_node_cluster_returns_correct_host(self):
        tools.assert_items_equal([2], self.get_db())

class RetryableTest(HashingTest):

    def test_attempt_reconnect_threshold_is_set(self):
        tools.assert_equal(self.router.attempt_reconnect_threshold, 100000)

    def test_retry_gives_next_host_if_primary_is_offline(self):
        tools.assert_items_equal([2], self.get_db())
        tools.assert_items_equal([4], self.get_db(retry_for=2))

    def test_retry_host_change_is_sticky(self):
        tools.assert_items_equal([2], self.get_db())
        tools.assert_items_equal([4], self.get_db(retry_for=2))

        tools.assert_items_equal([4], self.get_db())

    def test_adds_back_down_host_once_attempt_reconnect_threshold_is_passed(self):
        ConsistentHashingRouter.attempt_reconnect_threshold = 3

        tools.assert_items_equal([2], self.get_db())
        tools.assert_items_equal([4], self.get_db(retry_for=2))
        tools.assert_items_equal([4], self.get_db())

        # Router should add host 1 back to the pool now
        tools.assert_items_equal([2], self.get_db())

        ConsistentHashingRouter.attempt_reconnect_threshold = 100000

    def test_raises_host_list_exhaused_if_no_host_can_be_found(self):
        # Kill the first 4
        [self.get_db(retry_for=i) for i in range(4)]

        # And the 5th should raise an error
        tools.assert_raises(
            ConsistentHashingRouter.HostListExhaused,
            self.get_db, **dict(retry_for=4))
