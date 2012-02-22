"""
nydus.db.backends.base
~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

__all__ = ('BaseConnection',)


class BasePipeline(object):
    """
    Base Pipeline class.

    This basically is absolutely useless, and just provides a sample
    API for dealing with pipelined commands.
    """
    def __init__(self, connection):
        self.pending = []
        self.connection = connection

    def add(self, command):
        self.pending.append(command)

    def execute(self):
        results = {}
        for command in self.pending:
            results[command._ident] = command(*command._args, **command._kwargs)
        return results


class BaseConnection(object):
    """
    Base connection class.

    Child classes should implement at least
    connect() and disconnect() methods.
    """

    retryable_exceptions = ()
    supports_pipelines = False

    def __init__(self, num, **options):
        self._connection = None
        self.num = num

    @property
    def identifier(self):
        return repr(self)

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.connect()
        return self._connection

    def close(self):
        if self._connection:
            self.disconnect()
        self._connection = None

    def connect(self):
        raise NotImplementedError

    def disconnect(self):
        raise NotImplementedError

    def get_pipeline(self):
        return BasePipeline(self)
