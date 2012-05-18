"""
tests.test_connections
~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""
from __future__ import absolute_import

import mock

from nydus.db.base import Cluster, create_cluster, EventualCommand, apply_defaults
from nydus.db.routers.base import BaseRouter
from nydus.db.routers.keyvalue import get_key
from nydus.db.backends.base import BaseConnection

from tests import BaseTest


class DummyConnection(BaseConnection):
    def __init__(self, resp='foo', **kwargs):
        self.resp = resp
        super(DummyConnection, self).__init__(**kwargs)

    def foo(self, *args, **kwargs):
        return self.resp


class DummyRouter(BaseRouter):
    def get_dbs(self, attr, args, kwargs, **fkwargs):
        key = get_key(args, kwargs)
        if key == 'foo':
            return [1]
        return [0]


class CreateClusterTest(BaseTest):
    def test_creates_cluster(self):
        c = create_cluster({
            'engine': DummyConnection,
            'router': DummyRouter,
            'hosts': {
                0: {'resp': 'bar'},
            }
        })
        self.assertEquals(len(c), 1)

    @mock.patch('nydus.db.base.apply_defaults')
    def test_does_call_apply_defaults(self, apply_defaults):
        create_cluster({
            'engine': DummyConnection,
            'defaults': {'foo': 'baz'},
            'hosts': {
                0: {'resp': 'bar'},
            }
        })
        apply_defaults.assert_called_once_with({'resp': 'bar'}, {'foo': 'baz'})


class ClusterTest(BaseTest):
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
        c = mock.Mock()
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

    def __init__(self, *args, **kwargs):
        self.kwargs_seen = []
        self.key_args_seen = []
        super(RetryableRouter, self).__init__(*args, **kwargs)

    def get_dbs(self, attr, args, kwargs, **fkwargs):
        key = get_key(args, kwargs)
        self.kwargs_seen.append(fkwargs)
        self.key_args_seen.append(key)
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


class ApplyDefaultsTest(BaseTest):
    def test_does_apply(self):
        host = {'port': 6379}
        defaults = {'host': 'localhost'}
        results = apply_defaults(host, defaults)
        self.assertEquals(results, {
            'port': 6379,
            'host': 'localhost',
        })

    def test_does_not_overwrite(self):
        host = {'port': 6379}
        defaults = {'port': 9000}
        results = apply_defaults(host, defaults)
        self.assertEquals(results, {
            'port': 6379,
        })
