"""
tests.test_connections
~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""
from __future__ import absolute_import

from mock import Mock

from nydus.db.base import Cluster, create_cluster, EventualCommand
from nydus.db.routers.base import BaseRouter
from nydus.db.backends.base import BaseConnection

from tests import BaseTest


class DummyConnection(BaseConnection):
    def __init__(self, resp='foo', **kwargs):
        self.resp = resp
        super(DummyConnection, self).__init__(**kwargs)

    def foo(self, *args, **kwargs):
        return self.resp


class DummyRouter(BaseRouter):
    def get_dbs(self, cluster, attr, key=None, *args, **kwargs):
        if key == 'foo':
            return [1]
        return [0]



class BrokenRedisTest(BaseTest):
    def setUp(self):
        from nydus.db import create_cluster
        engine = 'nydus.db.backends.redis.Redis'
        router = 'nydus.db.routers.redis.PrefixPartitionRouter'
        nydus_config = dict(engine=engine, router=router, hosts={
            'default': {'db': 0, 'host': 'localhost', 'port': 6380, 'fail_silently': False},
            'simple_cache': {'db': 0, 'host': 'localhost', 'port': 6380, 'fail_silently': True},
            'app_critical': {'db': 0, 'host': 'localhost', 'port': 6380, 'fail_silently': False},
        })
        redis = create_cluster(nydus_config)
        self.redis = redis
    
    def test_broken_redis(self):
        #test silent failures
        key = 'simple_cache:test'
        set_result = self.redis.set(key, '1')
        assert not set_result
        result = self.redis.get(key)
        assert not result
        
        #assert by default we fail loudly
        from redis.exceptions import ConnectionError
        try:
            key = 'app_critical:test'
            set_result = self.redis.set(key, '1')
            result = self.redis.get(key)
        except ConnectionError, e:
            pass
        else:
            raise Exception, 'we were hoping for a connection error'
        
    def test_map(self):
        keys = ['simple_cache:test', 'simple_cache:test_two', 'app_critical:test']
        with self.redis.map() as conn:
            results = [conn.get(k) for k in keys]
        
        for result, key in zip(results, keys):
            result_object = result._wrapped
            if 'app_critical' in key:
                assert 'Error' in result_object
            else:
                assert result_object is None, 'we should get None when failing'
                
                
        

class ClusterTest(BaseTest):
    def test_create_cluster(self):
        c = create_cluster({
            'engine': DummyConnection,
            'router': DummyRouter,
            'hosts': {
                0: {'resp': 'bar'},
            }
        })
        self.assertEquals(len(c), 1)

    def test_init(self):
        c = Cluster(
            hosts={0: BaseConnection(num=1)},
        )
        self.assertEquals(len(c), 1)

    def test_proxy(self):
        c = DummyConnection(num=1, resp='bar')
        p = Cluster(
            hosts={0: c},
        )
        self.assertEquals(p.foo(), 'bar')

    def test_disconnect(self):
        c = Mock()
        p = Cluster(
            hosts={0: c},
        )
        p.disconnect()
        c.disconnect.assert_called_once()

    def test_with_router(self):
        c = DummyConnection(num=0, resp='foo')
        c2 = DummyConnection(num=1, resp='bar')

        # test dummy router
        r = DummyRouter
        p = Cluster(
            hosts={0: c, 1: c2},
            router=r,
        )
        self.assertEquals(p.foo(), 'foo')
        self.assertEquals(p.foo('foo'), 'bar')

        # test default routing behavior
        p = Cluster(
            hosts={0: c, 1: c2},
        )
        self.assertEquals(p.foo(), ['foo', 'bar'])
        self.assertEquals(p.foo('foo'), ['foo', 'bar'])

    def test_get_conn(self):
        c = DummyConnection(alias='foo', num=0, resp='foo')
        c2 = DummyConnection(alias='foo', num=1, resp='bar')

        # test dummy router
        r = DummyRouter
        p = Cluster(
            hosts={0: c, 1: c2},
            router=r,
        )
        self.assertEquals(p.get_conn(), c)
        self.assertEquals(p.get_conn('foo'), c2)

        # test default routing behavior
        p = Cluster(
            hosts={0: c, 1: c2},
        )
        self.assertEquals(p.get_conn(), [c, c2])
        self.assertEquals(p.get_conn('foo'), [c, c2])

    def test_map(self):
        c = DummyConnection(num=0, resp='foo')
        c2 = DummyConnection(num=1, resp='bar')

        # test dummy router
        r = DummyRouter
        p = Cluster(
            hosts={0: c, 1: c2},
            router=r,
        )
        with p.map() as conn:
            foo = conn.foo()
            bar = conn.foo('foo')
            self.assertEquals(foo, None)
            self.assertEquals(bar, None)

        self.assertEquals(bar, 'bar')
        self.assertEquals(foo, 'foo')

        # test default routing behavior
        p = Cluster(
            hosts={0: c, 1: c2},
        )
        with p.map() as conn:
            foo = conn.foo()
            bar = conn.foo('foo')
            self.assertEquals(foo, None)
            self.assertEquals(bar, None)

        self.assertEquals(foo, ['foo', 'bar'])
        self.assertEquals(bar, ['foo', 'bar'])


class FlakeyConnection(DummyConnection):

    retryable_exceptions = [Exception]

    def foo(self, *args, **kwargs):
        if hasattr(self, 'already_failed'):
            super(FlakeyConnection, self).foo()
        else:
            self.already_failed = True
            raise Exception('boom!')


class RetryableRouter(DummyRouter):
    retryable = True

    def __init__(self):
        self.kwargs_seen = []
        self.key_args_seen = []
        super(RetryableRouter, self).__init__()

    def get_dbs(self, cluster, func, key=None, *args, **kwargs):
        self.kwargs_seen.append(kwargs)
        self.key_args_seen.append(key)
        return [0]


class InconsistentRouter(DummyRouter):
    retryable = True

    def __init__(self):
        self.returned = False
        super(InconsistentRouter, self).__init__()

    def get_dbs(self, cluster, func, key=None, *args, **kwargs):
        if self.returned:
            return range(5)
        else:
            self.returned = True
            return [0]


class ScumbagConnection(DummyConnection):

    retryable_exceptions = [Exception]

    def foo(self):
        raise Exception("Says it's a connection / Never actually connects.")


class RetryClusterTest(BaseTest):

    def build_cluster(self, connection=FlakeyConnection, router=RetryableRouter):
        return create_cluster({
            'engine': connection,
            'router': router,
            'hosts': {
                0: {'resp': 'bar'},
            }
        })

    def test_returns_correctly(self):
        cluster = self.build_cluster(connection=DummyConnection)
        self.assertEquals(cluster.foo(), 'bar')

    def test_retry_router_when_receives_error(self):
        cluster = self.build_cluster()

        cluster.foo()
        self.assertEquals({'retry_for': 0}, cluster.router.kwargs_seen.pop())

    def test_protection_from_infinate_loops(self):
        cluster = self.build_cluster(connection=ScumbagConnection)
        with self.assertRaises(Exception):
            cluster.foo()


class EventualCommandTest(BaseTest):
    def test_unevaled_repr(self):
        ec = EventualCommand('foo')
        ec('bar', baz='foo')

        self.assertEquals(repr(ec), u"<EventualCommand: foo args=('bar',) kwargs={'baz': 'foo'}>")

    def test_evaled_repr(self):
        ec = EventualCommand('foo')
        ec('bar', baz='foo')
        ec._set_value('biz')

        self.assertEquals(repr(ec), u"'biz'")

    def test_coersion(self):
        ec = EventualCommand('foo')()
        ec._set_value('5')

        self.assertEquals(int(ec), 5)

    def test_nonzero(self):
        ec = EventualCommand('foo')()
        ec._set_value(None)

        self.assertEquals(int(ec or 0), 0)

    def test_evaled_unicode(self):
        ec = EventualCommand('foo')
        ec._set_value('biz')

        self.assertEquals(unicode(ec), u'biz')
