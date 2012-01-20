"""
nydus.db.backends
~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

class BaseConnection(object):
    """
    Base connection class.

    Child classes should implement at least
    connect() and disconnect() methods.
    """

    retryable_exceptions = ()

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