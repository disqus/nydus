"""
nydus.db.backends.memcache
~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

import pylibmc

from nydus.db.backends import BaseConnection, BasePipeline
from nydus.db.promise import EventualCommand


class Memcache(BaseConnection):

    retryable_exceptions = frozenset([pylibmc.Error])
    supports_pipelines = True

    def __init__(self, num, host='localhost', port=11211, binary=True,
            behaviors=None, **options):
        self.host = host
        self.port = port
        self.binary = binary
        self.behaviors = behaviors
        super(Memcache, self).__init__(num)

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
    def __init__(self, connection):
        self.pending = []
        self.connection = connection

    def add(self, command):
        # A feature of Memcache is a 'get_multi' command. Therefore we can merge
        # consecutive 'get' commands into one 'get_multi' command.

        # Need to merge this into one command
        name, args, kwargs = command.get_command()
        if name == 'get':
            if self.pending and self.pending[-1].get_name() == 'get_multi':
                prev_command = self.pending[-1]
                args = prev_command.get_args()
                args[0].append(command.get_args()[0])
                prev_command.set_args(args)

            else:
                key = command.get_args()[0]
                multi_command = EventualCommand('get_multi')
                multi_command([key])
                self.pending.append(multi_command)

        else:
            self.pending.append(command)

    def execute(self):
        ret = []
        for command in self.pending:
            ret.append(command.resolve(self.connection))

        return ret
