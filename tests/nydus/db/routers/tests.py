from __future__ import absolute_import

import mock
import time

from collections import Iterable
from inspect import getargspec

from nydus.db.base import BaseCluster
from nydus.db.backends import BaseConnection
from nydus.db.routers import BaseRouter, RoundRobinRouter
from nydus.db.routers.keyvalue import ConsistentHashingRouter
from nydus.testutils import BaseTest


def _get_func(func):
    return getattr(func, '__wraps__', func)


class DummyConnection(BaseConnection):
    def __init__(self, i):
        self.host = 'dummyhost'
        self.i = i
        super(DummyConnection, self).__init__(i)

    @property
    def identifier(self):
        return "%s:%s" % (self.host, self.i)


class BaseRouterTest(BaseTest):
    Router = BaseRouter

    class TestException(Exception):
        pass

    def setUp(self):
        self.hosts = dict((i, {}) for i in xrange(5))
        self.cluster = BaseCluster(router=self.Router, hosts=self.hosts, backend=DummyConnection)
        self.router = self.cluster.router

    def get_dbs(self, *args, **kwargs):
        return self.router.get_dbs(*args, **kwargs)

    def test_not_ready(self):
        self.assertTrue(not self.router._ready)

    def test_get_dbs_iterable(self):
        db_nums = self.get_dbs(attr='test', args=('foo',))
        self.assertIsInstance(db_nums, Iterable)

    def test_get_dbs_unabletosetuproute(self):
        with mock.patch.object(self.router, '_setup_router', return_value=False):
            with self.assertRaises(BaseRouter.UnableToSetupRouter):
                self.get_dbs(attr='test', args=('foo',))

    def test_setup_router_returns_true(self):
        self.assertTrue(self.router.setup_router())

    def test_offers_router_interface(self):
        func = _get_func(self.router.get_dbs)
        self.assertTrue(callable(func))
        dbargs, _, _, dbdefaults = getargspec(func)
        self.assertTrue(set(dbargs) >= set(['self', 'attr', 'args', 'kwargs']))
        self.assertIsNone(dbdefaults)

        func = _get_func(self.router.setup_router)
        self.assertTrue(callable(func))
        setupargs, _, _, setupdefaults = getargspec(func)
        self.assertTrue(set(setupargs) >= set(['self', 'args', 'kwargs']))
        self.assertIsNone(setupdefaults)

    def test_returns_whole_cluster_without_key(self):
        self.assertEquals(self.hosts.keys(), self.get_dbs(attr='test'))

    def test_get_dbs_handles_exception(self):
        with mock.patch.object(self.router, '_route') as _route:
            with mock.patch.object(self.router, '_handle_exception') as _handle_exception:
                _route.side_effect = self.TestException()

                self.get_dbs(attr='test', args=('foo',))

                self.assertTrue(_handle_exception.called)


class BaseBaseRouterTest(BaseRouterTest):
    def test__setup_router_returns_true(self):
        self.assertTrue(self.router._setup_router())

    def test__pre_routing_returns_args_and_kwargs(self):
        self.assertEqual((('foo',), {}), self.router._pre_routing(attr='test', args=('foo',)))

    def test__route_returns_first_db_num(self):
        self.assertEqual(self.cluster.hosts.keys()[0], self.router._route(attr='test', args=('foo',))[0])

    def test__post_routing_returns_db_nums(self):
        db_nums = self.hosts.keys()

        self.assertEqual(db_nums, self.router._post_routing(attr='test', db_nums=db_nums, args=('foo',)))

    def test__handle_exception_raises_same_exception(self):
        e = self.TestException()

        with self.assertRaises(self.TestException):
            self.router._handle_exception(e)

    def test_returns_sequence_with_one_item_when_given_key(self):
        self.assertEqual(len(self.get_dbs(attr='test', args=('foo',))), len(self.hosts))


class BaseRoundRobinRouterTest(BaseRouterTest):
    Router = RoundRobinRouter

    def setUp(self):
        super(BaseRoundRobinRouterTest, self).setUp()
        assert self.router._setup_router()

    def test_ensure_db_num(self):
        db_num = 0
        s_db_num = str(db_num)

        self.assertEqual(self.router.ensure_db_num(db_num), db_num)
        self.assertEqual(self.router.ensure_db_num(s_db_num), db_num)

    def test_esnure_db_num_raises(self):
        with self.assertRaises(RoundRobinRouter.InvalidDBNum):
            self.router.ensure_db_num('a')

    def test_flush_down_connections(self):
        self.router._get_db_attempts = 9001
        self._down_connections = {0: time.time()}

        self.router.flush_down_connections()

        self.assertEqual(self.router._get_db_attempts, 0)
        self.assertEqual(self.router._down_connections, {})

    def test_mark_connection_down(self):
        db_num = 0

        self.router.mark_connection_down(db_num)

        self.assertAlmostEqual(self.router._down_connections[db_num], time.time(), delta=10)

    def test_mark_connection_up(self):
        db_num = 0

        self.router.mark_connection_down(db_num)

        self.assertIn(db_num, self.router._down_connections)

        self.router.mark_connection_up(db_num)

        self.assertNotIn(db_num, self.router._down_connections)

    def test__pre_routing_updates__get_db_attempts(self):
        self.router._pre_routing(attr='test', args=('foo',))

        self.assertEqual(self.router._get_db_attempts, 1)

    @mock.patch('nydus.db.routers.base.RoundRobinRouter.check_down_connections')
    def test__pre_routing_check_down_connections(self, _check_down_connections):
        self.router._get_db_attempts = RoundRobinRouter.attempt_reconnect_threshold + 1

        self.router._pre_routing(attr='test', args=('foo',))

        self.assertTrue(_check_down_connections.called)

    @mock.patch('nydus.db.routers.base.RoundRobinRouter.mark_connection_down')
    def test__pre_routing_retry_for(self, _mark_connection_down):
        db_num = 0

        self.router._pre_routing(attr='test', args=('foo',), retry_for=db_num)

        _mark_connection_down.assert_called_with(db_num)

    @mock.patch('nydus.db.routers.base.RoundRobinRouter.mark_connection_up')
    def test_online_connections_dont_get_marked_as_up(self, mark_connection_up):
        db_nums = [0]

        self.assertEqual(self.router._post_routing(attr='test', db_nums=db_nums, args=('foo',)), db_nums)
        self.assertFalse(mark_connection_up.called)

    @mock.patch('nydus.db.routers.base.RoundRobinRouter.mark_connection_up')
    def test_offline_connections_get_marked_as_up(self, mark_connection_up):
        self.router.mark_connection_down(0)
        db_nums = [0]

        self.assertEqual(self.router._post_routing(attr='test', db_nums=db_nums, args=('foo',)), db_nums)
        mark_connection_up.assert_called_with(db_nums[0])


class RoundRobinRouterTest(BaseRoundRobinRouterTest):
    def test__setup_router(self):
        self.assertTrue(self.router._setup_router())
        self.assertIsInstance(self.router._hosts_cycler, Iterable)

    def test__route_cycles_through_keys(self):
        db_nums = self.hosts.keys() * 2
        results = [self.router._route(attr='test', args=('foo',))[0] for _ in db_nums]

        self.assertEqual(results, db_nums)

    def test__route_retry(self):
        self.router.retry_timeout = 0

        db_num = 0

        self.router.mark_connection_down(db_num)

        db_nums = self.router._route(attr='test', args=('foo',))

        self.assertEqual(db_nums, [db_num])

    def test__route_skip_down(self):
        db_num = 0

        self.router.mark_connection_down(db_num)

        db_nums = self.router._route(attr='test', args=('foo',))

        self.assertNotEqual(db_nums, [db_num])
        self.assertEqual(db_nums, [db_num + 1])

    def test__route_hostlistexhausted(self):
        [self.router.mark_connection_down(db_num) for db_num in self.hosts.keys()]

        with self.assertRaises(RoundRobinRouter.HostListExhausted):
            self.router._route(attr='test', args=('foo',))


class ConsistentHashingRouterTest(BaseRoundRobinRouterTest):
    Router = ConsistentHashingRouter

    def get_dbs(self, *args, **kwargs):
        kwargs['attr'] = 'test'
        return super(ConsistentHashingRouterTest, self).get_dbs(*args, **kwargs)

    def test_retry_gives_next_host_if_primary_is_offline(self):
        self.assertEquals([2], self.get_dbs(args=('foo',)))
        self.assertEquals([4], self.get_dbs(args=('foo',), retry_for=2))

    def test_retry_host_change_is_sticky(self):
        self.assertEquals([2], self.get_dbs(args=('foo',)))
        self.assertEquals([4], self.get_dbs(args=('foo',), retry_for=2))

        self.assertEquals([4], self.get_dbs(args=('foo',)))

    def test_raises_host_list_exhaused_if_no_host_can_be_found(self):
        # Kill the first 4
        [self.get_dbs(retry_for=i) for i in range(4)]

        # And the 5th should raise an error
        self.assertRaises(
            ConsistentHashingRouter.HostListExhausted,
            self.get_dbs, **dict(args=('foo',), retry_for=4))
