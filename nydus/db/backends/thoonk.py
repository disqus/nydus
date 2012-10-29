"""
nydus.db.backends.thoonk
~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

from thoonk import Pubsub

from redis import RedisError

from nydus.db.backends import BaseConnection


class Thoonk(BaseConnection):
    # Exceptions that can be retried by this backend
    retryable_exceptions = frozenset([RedisError])
    supports_pipelines = False

    def __init__(self, num, host='localhost', port=6379, db=0, timeout=None, listen=False):
        self.host = host
        self.port = port
        self.db = db
        self.timeout = timeout
        self.pubsub = None
        self.listen = listen
        super(Thoonk, self).__init__(num)

    @property
    def identifier(self):
        mapping = vars(self)
        mapping['klass'] = self.__class__.__name__
        return "redis://%(host)s:%(port)s/%(db)s" % mapping

    def connect(self):
        return Pubsub(host=self.host, port=self.port, db=self.db, listen=self.listen)

    def disconnect(self):
        self.connection.close()

    def flushdb(self):
        """the tests assume this function exists for all redis-like backends"""
        self.connection.redis.flushdb()
