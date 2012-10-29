"""
nydus.db.backends.base
~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

__all__ = ('BaseConnection',)

from nydus.db.base import BaseCluster


class BasePipeline(object):
    """
    Base Pipeline class.

    This basically is absolutely useless, and just provides a sample
    API for dealing with pipelined commands.
    """
    def __init__(self, connection):
        self.connection = connection
        self.pending = []

    def add(self, command):
        """
        Add a command to the pending execution pipeline.
        """
        self.pending.append(command)

    def execute(self):
        """
        Execute all pending commands and return a list of the results
        ordered by call.
        """
        raise NotImplementedError


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

    def __getattr__(self, name):
        return getattr(self.connection, name)

    @property
    def identifier(self):
        return repr(self)

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.connect()
        return self._connection

    def close(self):
        """
        Close the connection if it is open.
        """
        if self._connection:
            self.disconnect()
        self._connection = None

    def connect(self):
        """
        Connect.

        Must return a connection object.
        """
        raise NotImplementedError

    def disconnect(self):
        """
        Disconnect.
        """
        raise NotImplementedError

    def get_pipeline(self):
        """
        Return a new pipeline instance (bound to this connection).
        """
        raise NotImplementedError

    @classmethod
    def get_cluster(cls):
        """
        Return the default cluster type for this backend.
        """
        return BaseCluster
