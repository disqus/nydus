from __future__ import absolute_import

import mock

from nydus.db import create_cluster
from nydus.db.backends.base import BaseConnection
from nydus.db.base import BaseCluster, create_connection
from nydus.db.exceptions import CommandError
from nydus.db.routers.base import BaseRouter
from nydus.db.routers.keyvalue import get_key
from nydus.db.promise import EventualCommand
from nydus.testutils import BaseTest, fixture
from nydus.utils import apply_defaults


class DummyConnection(BaseConnection):
    def __init__(self, num, resp='foo', **kwargs):
        self.resp = resp
        super(DummyConnection, self).__init__(num, **kwargs)

    def foo(self, *args, **kwargs):
        return self.resp


class DummyErroringConnection(DummyConnection):
    def foo(self, *args, **kwargs):
        if self.resp == 'error':
            raise ValueError(self.resp)
        return self.resp


class DummyRouter(BaseRouter):
    def get_dbs(self, attr, args, kwargs, **fkwargs):
        key = get_key(args, kwargs)
        if key == 'foo':
            return [1]
        return [0]


class ConnectionTest(BaseTest):
    @fixture
    def connection(self):
        return BaseConnection(0)

    @mock.patch('nydus.db.backends.base.BaseConnection.disconnect')
    def test_close_calls_disconnect(self, disconnect):
        self.connection._connection = mock.Mock()
        self.connection.close()
        disconnect.assert_called_once_with()

    @mock.patch('nydus.db.backends.base.BaseConnection.disconnect', mock.Mock(return_value=None))
    def test_close_unsets_connection(self):
        self.connection.close()
        self.assertEquals(self.connection._connection, None)

    @mock.patch('nydus.db.backends.base.BaseConnection.disconnect')
    def test_close_propagates_noops_if_not_connected(self, disconnect):
        self.connection.close()
        self.assertFalse(disconnect.called)

    @mock.patch('nydus.db.backends.base.BaseConnection.connect')
    def test_connection_forces_connect(self, connect):
        self.connection.connection
        connect.assert_called_once_with()

    @mock.patch('nydus.db.backends.base.BaseConnection.connect')
    def test_connection_doesnt_reconnect_with_existing_connection(self, connect):
        self.connection._connection = mock.Mock()
        self.connection.connection
        self.assertFalse(connect.called)

    @mock.patch('nydus.db.backends.base.BaseConnection.connect')
    def test_connection_returns_result_of_connect(self, connect):
        val = self.connection.connection
        self.assertEquals(val, connect.return_value)

    def test_attrs_proxy(self):
        conn = mock.Mock()
        self.connection._connection = conn
        val = self.connection.foo(biz='baz')
        conn.foo.assert_called_once_with(biz='baz')
        self.assertEquals(val, conn.foo.return_value)


class CreateConnectionTest(BaseTest):
    def test_does_apply_defaults(self):
        conn = mock.Mock()
        create_connection(conn, 0, {'resp': 'bar'}, {'foo': 'baz'})
        conn.assert_called_once_with(0, foo='baz', resp='bar')

    def test_handles_arg_list_with_defaults(self):
        conn = mock.Mock()
        create_connection(conn, 0, ['localhost'], {'foo': 'baz'})
        conn.assert_called_once_with(0, 'localhost', foo='baz')


class CreateClusterTest(BaseTest):
    def test_creates_cluster(self):
        c = create_cluster({
            'backend': DummyConnection,
            'router': DummyRouter,
            'hosts': {
                0: {'resp': 'bar'},
            }
        })
        self.assertEquals(len(c), 1)

    @mock.patch('nydus.db.base.create_connection')
    def test_does_create_connection_with_defaults(self, create_connection):
        create_cluster({
            'backend': DummyConnection,
            'defaults': {'foo': 'baz'},
            'hosts': {
                0: {'resp': 'bar'},
            }
        })
        create_connection.assert_called_once_with(DummyConnection, 0, {'resp': 'bar'}, {'foo': 'baz'})


class ClusterTest(BaseTest):
    def test_len_returns_num_backends(self):
        p = BaseCluster(
            backend=BaseConnection,
            hosts={0: {}},
        )
        self.assertEquals(len(p), 1)

    def test_proxy(self):
        p = BaseCluster(
            backend=DummyConnection,
            hosts={0: {'resp': 'bar'}},
        )
        self.assertEquals(p.foo(), 'bar')

    def test_disconnect(self):
        c = mock.Mock()
        p = BaseCluster(
            backend=c,
            hosts={0: {'resp': 'bar'}},
        )
        p.disconnect()
        c.disconnect.assert_called_once()

    def test_with_split_router(self):
        p = BaseCluster(
            router=DummyRouter,
            backend=DummyConnection,
            hosts={
                0: {'resp': 'foo'},
                1: {'resp': 'bar'},
            },
        )
        self.assertEquals(p.foo(), 'foo')
        self.assertEquals(p.foo('foo'), 'bar')

    def test_default_routing_with_multiple_hosts(self):
        p = BaseCluster(
            backend=DummyConnection,
            hosts={
                0: {'resp': 'foo'},
                1: {'resp': 'bar'},
            },
        )
        self.assertEquals(p.foo(), ['foo', 'bar'])
        self.assertEquals(p.foo('foo'), ['foo', 'bar'])

    def test_get_conn_with_split_router(self):
        # test dummy router
        p = BaseCluster(
            backend=DummyConnection,
            hosts={
                0: {'resp': 'foo'},
                1: {'resp': 'bar'},
            },
            router=DummyRouter,
        )
        self.assertEquals(p.get_conn().num, 0)
        self.assertEquals(p.get_conn('foo').num, 1)

    def test_get_conn_default_routing_with_multiple_hosts(self):
        # test default routing behavior
        p = BaseCluster(
            backend=DummyConnection,
            hosts={
                0: {'resp': 'foo'},
                1: {'resp': 'bar'},
            },
        )
        self.assertEquals(map(lambda x: x.num, p.get_conn()), [0, 1])
        self.assertEquals(map(lambda x: x.num, p.get_conn('foo')), [0, 1])


class MapTest(BaseTest):
    @fixture
    def cluster(self):
        return BaseCluster(
            backend=DummyConnection,
            hosts={
                0: {'resp': 'foo'},
                1: {'resp': 'bar'},
            },
        )

    def test_handles_single_routing_results(self):
        self.cluster.install_router(DummyRouter)

        with self.cluster.map() as conn:
            foo = conn.foo()
            bar = conn.foo('foo')
            self.assertEquals(foo, None)
            self.assertEquals(bar, None)

        self.assertEquals(bar, 'bar')
        self.assertEquals(foo, 'foo')

    def test_handles_groups_of_results(self):
        with self.cluster.map() as conn:
            foo = conn.foo()
            bar = conn.foo('foo')
            self.assertEquals(foo, None)
            self.assertEquals(bar, None)

        self.assertEquals(foo, ['foo', 'bar'])
        self.assertEquals(bar, ['foo', 'bar'])


class MapWithFailuresTest(BaseTest):
    @fixture
    def cluster(self):
        return BaseCluster(
            backend=DummyErroringConnection,
            hosts={
                0: {'resp': 'foo'},
                1: {'resp': 'error'},
            },
            router=DummyRouter,
        )

    def test_propagates_errors(self):
        with self.assertRaises(CommandError):
            with self.cluster.map() as conn:
                foo = conn.foo()
                bar = conn.foo('foo')
                self.assertEquals(foo, None)
                self.assertEquals(bar, None)

    def test_fail_silenlty(self):
        with self.cluster.map(fail_silently=True) as conn:
            foo = conn.foo()
            bar = conn.foo('foo')
            self.assertEquals(foo, None)
            self.assertEquals(bar, None)

        self.assertEquals(len(conn.get_errors()), 1, conn.get_errors())
        self.assertEquals(type(conn.get_errors()[0][1]), ValueError)

        self.assertEquals(foo, 'foo')
        self.assertNotEquals(foo, 'bar')


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
            'backend': connection,
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
        ec.resolve_as('biz')

        self.assertEquals(repr(ec), u"'biz'")

    def test_coersion(self):
        ec = EventualCommand('foo')()
        ec.resolve_as('5')

        self.assertEquals(int(ec), 5)

    def test_nonzero(self):
        ec = EventualCommand('foo')()
        ec.resolve_as(None)

        self.assertEquals(int(ec or 0), 0)

    def test_evaled_unicode(self):
        ec = EventualCommand('foo')
        ec.resolve_as('biz')

        self.assertEquals(unicode(ec), u'biz')

    def test_command_error_returns_as_error(self):
        ec = EventualCommand('foo')
        ec.resolve_as(CommandError([ValueError('test')]))
        self.assertEquals(ec.is_error, True)

    def test_other_error_does_not_return_as_error(self):
        ec = EventualCommand('foo')
        ec.resolve_as(ValueError('test'))
        self.assertEquals(ec.is_error, False)

    def test_isinstance_check(self):
        ec = EventualCommand('foo')
        ec.resolve_as(['foo', 'bar'])

        self.assertEquals(isinstance(ec, list), True)


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
