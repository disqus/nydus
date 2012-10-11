from __future__ import absolute_import

from tests import BaseTest, fixture

from nydus.db import create_cluster
from nydus.db.base import BaseCluster
from nydus.db.promise import EventualCommand

from nydus.db.backends.memcache import Memcache, regroup_commands

import mock
import pylibmc


class RegroupCommandsTest(BaseTest):
    def get_grouped_results(self, commands, num_expected):
        grouped = regroup_commands(commands)
        self.assertEquals(len(grouped), num_expected, grouped)
        return grouped

    def test_set_basic(self):
        commands = [
            EventualCommand('set', ['foo', 1], {'timeout': 1}),
            EventualCommand('set', ['bar', 2], {'timeout': 1}),
            EventualCommand('set', ['baz', 3], {'timeout': 2}),
        ]

        items = self.get_grouped_results(commands, 2)

        new_command, grouped_commands = items[0]
        self.assertEquals(len(grouped_commands), 2)
        self.assertEquals(grouped_commands, commands[0:2])
        self.assertEquals(new_command.get_name(), 'set_multi')
        self.assertEquals(new_command.get_args(), ({
            'foo': 1,
            'bar': 2,
        },))
        self.assertEquals(new_command.get_kwargs(), {
            'timeout': 1,
        })

        new_command, grouped_commands = items[1]
        self.assertEquals(len(grouped_commands), 1)
        self.assertEquals(grouped_commands, commands[2:3])
        self.assertEquals(new_command.get_name(), 'set')
        self.assertEquals(new_command.get_args(), ['baz', 3])
        self.assertEquals(new_command.get_kwargs(), {
            'timeout': 2,
        })

    def test_get_basic(self):
        commands = [
            EventualCommand('get', ['foo'], {'key_prefix': 1}),
            EventualCommand('get', ['bar'], {'key_prefix': 1}),
            EventualCommand('get', ['baz'], {'key_prefix': 2}),
        ]
        items = self.get_grouped_results(commands, 2)

        new_command, grouped_commands = items[0]
        self.assertEquals(len(grouped_commands), 2)
        self.assertEquals(grouped_commands, commands[0:2])
        self.assertEquals(new_command.get_name(), 'get_multi')
        self.assertEquals(new_command.get_args(), (['foo', 'bar'],))
        self.assertEquals(new_command.get_kwargs(), {
            'key_prefix': 1,
        })

        new_command, grouped_commands = items[1]
        self.assertEquals(len(grouped_commands), 1)
        self.assertEquals(grouped_commands, commands[2:3])
        self.assertEquals(new_command.get_name(), 'get')
        self.assertEquals(new_command.get_args(), ['baz'])
        self.assertEquals(new_command.get_kwargs(), {
            'key_prefix': 2,
        })

    def test_delete_basic(self):
        commands = [
            EventualCommand('delete', ['foo'], {'key_prefix': 1}),
            EventualCommand('delete', ['bar'], {'key_prefix': 1}),
            EventualCommand('delete', ['baz'], {'key_prefix': 2}),
        ]
        items = self.get_grouped_results(commands, 2)

        new_command, grouped_commands = items[0]
        self.assertEquals(len(grouped_commands), 2)
        self.assertEquals(grouped_commands, commands[0:2])
        self.assertEquals(new_command.get_name(), 'delete_multi')
        self.assertEquals(new_command.get_args(), (['foo', 'bar'],))
        self.assertEquals(new_command.get_kwargs(), {
            'key_prefix': 1,
        })

        new_command, grouped_commands = items[1]
        self.assertEquals(len(grouped_commands), 1)
        self.assertEquals(grouped_commands, commands[2:3])
        self.assertEquals(new_command.get_name(), 'delete')
        self.assertEquals(new_command.get_args(), ['baz'])
        self.assertEquals(new_command.get_kwargs(), {
            'key_prefix': 2,
        })


class MemcacheTest(BaseTest):

    @fixture
    def memcache(self):
        return Memcache(num=0)

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
        p = BaseCluster(hosts={0: self.memcache})
        result = p.get('MemcacheTest_with_cluster')
        get.assert_called_once_with('MemcacheTest_with_cluster')
        self.assertEquals(result, get.return_value)

    @mock.patch('pylibmc.Client')
    def test_pipeline_behavior(self, Client):
        cluster = create_cluster({
            'engine': 'nydus.db.backends.memcache.Memcache',
            'hosts': {
                0: {'binary': True},
            }
        })

        with cluster.map() as conn:
            conn.set('a', 1)
            conn.set('b', 2)
            conn.set('c', 3)
            conn.get('a')
            conn.get('b')
            conn.get('c')

        Client.return_value.set_multi.assert_any_call({'a': 1, 'b': 2, 'c': 3})
        Client.return_value.get_multi.assert_any_call(['a', 'b', 'c'])

    def test_pipeline_integration(self):
        cluster = create_cluster({
            'engine': 'nydus.db.backends.memcache.Memcache',
            'hosts': {
                0: {'binary': True},
            }
        })

        with cluster.map() as conn:
            conn.set('a', 1)
            conn.set('b', 2)
            conn.set('c', 3)
            conn.get('a')
            conn.get('b')
            conn.get('c')

        results = conn.get_results()
        self.assertEquals(len(results), 6, results)
        self.assertEquals(results[0:3], [None, None, None])
        self.assertEquals(results[3:6], [1, 2, 3])
