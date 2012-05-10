"""
nydus.db.backends.redis
~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

from redis import Redis as RedisClient
from redis import RedisError

from nydus.db.backends import BaseConnection, BasePipeline


class RedisPipeline(BasePipeline):
    def __init__(self, connection):
        self.pending = []
        self.connection = connection
        self.pipe = connection.pipeline()

    def add(self, command):
        self.pending.append(command)
        getattr(self.pipe, command._attr)(*command._args, **command._kwargs)

    def execute(self):
        return self.pipe.execute()


class Redis(BaseConnection):
    # Exceptions that can be retried by this backend
    retryable_exceptions = frozenset([RedisError])
    supports_pipelines = True

    def __init__(self, host='localhost', port=6379, db=0, timeout=None, password=None, **options):
        self.host = host
        self.port = port
        self.db = db
        self.timeout = timeout
        self.__password = password
        super(Redis, self).__init__(**options)

    @property
    def identifier(self):
        mapping = vars(self)
        mapping['klass'] = self.__class__.__name__
        return "redis://%(host)s:%(port)s/%(db)s" % mapping

    def connect(self):
        return RedisClient(host=self.host, port=self.port, db=self.db, password=self.__password, socket_timeout=self.timeout)

    def disconnect(self):
        self.connection.disconnect()

    def get_pipeline(self, *args, **kwargs):
        return RedisPipeline(self)
