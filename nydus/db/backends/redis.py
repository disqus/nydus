"""
nydus.db.backends.redis
~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

from redis import Redis as RedisClient
from redis import exceptions as client_exceptions

from nydus.db.backends import BaseConnection

class Redis(BaseConnection):

    # Exceptions that can be retried by this backend
    retryable_exceptions = client_exceptions

    def __init__(self, host='localhost', port=6379, db=0, timeout=None, **options):
        self.host = host
        self.port = port
        self.db = db
        self.timeout = timeout
        super(Redis, self).__init__(**options)

    @property
    def identifier(self):
        mapping = vars(self)
        mapping['klass'] = self.__class__.__name__
        return "redis://%(host)s:%(port)s/%(db)s" % mapping

    def connect(self):
        return RedisClient(host=self.host, port=self.port, db=self.db, socket_timeout=self.timeout)

    def disconnect(self):
        self.connection.disconnect()

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            # Send to router
            return getattr(self.connection, attr)
