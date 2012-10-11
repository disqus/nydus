"""
nydus.db.backends.memcache
~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

import pylibmc

from itertools import izip
from nydus.db.backends import BaseConnection, BasePipeline
from nydus.db.promise import EventualCommand


class Memcache(BaseConnection):

    retryable_exceptions = frozenset([pylibmc.Error])
    supports_pipelines = True

    def __init__(self, host='localhost', port=11211, binary=True,
            behaviors=None, **options):
        self.host = host
        self.port = port
        self.binary = binary
        self.behaviors = behaviors
        super(Memcache, self).__init__(**options)

    @property
    def identifier(self):
        mapping = vars(self)
        return "memcache://%(host)s:%(port)s/" % mapping

    def connect(self):
        host = "%s:%i" % (self.host, self.port)
        return pylibmc.Client([host], binary=self.binary, behaviors=self.behaviors)

    def disconnect(self):
        self.connection.disconnect_all()

    def get_pipeline(self, *args, **kwargs):
        return MemcachePipeline(self)


class MemcachePipeline(BasePipeline):
    def execute(self):
        grouped = regroup_commands(self.pending)
        results = resolve_grouped_commands(grouped, self.connection)
        return results


def peek(value):
    generator = iter(value)
    prev = generator.next()
    for item in generator:
        yield prev, item
        prev = item
    yield prev, None


def grouped_command(commands):
    base = commands[0]
    name = base.get_name()
    multi_command = EventualCommand('%s_multi' % name)
    if name in ('get', 'delete'):
        multi_command([c.get_args()[0] for c in commands], *base.get_args()[1:], **base.get_kwargs())
    elif base.get_name() == 'set':
        multi_command(dict(c.get_args()[0:2] for c in commands), *base.get_args()[2:], **base.get_kwargs())
    else:
        raise ValueError('Command not supported: %r' % (base.get_name(),))
    return multi_command


def can_group_commands(command, next_command):
    multi_capable_commands = ('get', 'set', 'delete')

    if next_command is None:
        return False

    name = command.get_name()

    # TODO: support multi commands
    if name not in multi_capable_commands:
        return False

    if name != next_command.get_name():
        return False

    if name == 'set':
        if command.get_args()[2:] != next_command.get_args()[2:]:
            return False
    else:
        if command.get_args()[1:] != next_command.get_args()[1:]:
            return False

    if command.get_kwargs() != next_command.get_kwargs():
        return False

    return True


def regroup_commands(commands):
    """
    Returns a list of tuples:

        [(command_to_run, [list, of, commands])]

    If the list of commands has a single item, the command was not grouped.
    """
    grouped = []
    pending = []

    def group_pending():
        if not pending:
            return

        new_command = grouped_command(pending)
        result = []
        while pending:
            result.append(pending.pop(0))
        grouped.append((new_command, result))

    for command, next_command in peek(commands):
        # if the previous command was a get, and this is a set we must execute
        # any pending commands
        # TODO: unless this command is a get_multi and it matches the same option
        # signature
        if can_group_commands(command, next_command):
            # if previous command does not match this command
            if pending and not can_group_commands(pending[0], command):
                group_pending()

            pending.append(command)
        else:
            # if pending exists for this command, group it
            if pending and can_group_commands(pending[0], command):
                pending.append(command)
            else:
                grouped.append((command.clone(), [command]))

            # We couldn't group with previous command, so ensure we bubble up
            group_pending()

    group_pending()

    return grouped


def resolve_grouped_commands(grouped, connection):
    results = {}

    for master_command, grouped_commands in grouped:
        result = master_command.resolve(connection)

        # this command was not grouped
        if len(grouped_commands) == 1:
            results[grouped_commands[0]] = result
        else:
            if isinstance(result, dict):
                # XXX: assume first arg is key
                for command in grouped_commands:
                    results[command] = result.get(command.get_args()[0])
            else:
                for command, value in izip(grouped_commands, result):
                    results[command] = value

    return results
