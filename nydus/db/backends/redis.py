"""
nydus.db.backends.redis
~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

from itertools import izip
from redis import Redis as RedisClient, StrictRedis as StrictRedisClient
from redis import RedisError

from nydus.db.backends import BaseConnection, BasePipeline


class RedisPipeline(BasePipeline):
    def __init__(self, connection):
        self.pending = []
        self.connection = connection
        self.pipe = connection.pipeline()

    def add(self, command):
        name, args, kwargs = command.get_command()
        self.pending.append(command)
        # ensure the command is executed in the pipeline
        getattr(self.pipe, name)(*args, **kwargs)

    def execute(self):
        return dict(izip(self.pending, self.pipe.execute()))


class RedisBase(BaseConnection):
    """
    Base class shared by Redis and StrictRedis
    Child classes should implement connect()
    """
    # Exceptions that can be retried by this backend
    retryable_exceptions = frozenset([RedisError])
    supports_pipelines = True

    def __init__(self, num, host='localhost', port=6379, db=0, timeout=None,
                 password=None, unix_socket_path=None):
        self.host = host
        self.port = port
        self.db = db
        self.unix_socket_path = unix_socket_path
        self.timeout = timeout
        self._password = password
        super(RedisBase, self).__init__(num)

    @property
    def identifier(self):
        mapping = vars(self)
        mapping['klass'] = self.__class__.__name__
        return "redis://%(host)s:%(port)s/%(db)s" % mapping

    def disconnect(self):
        self.connection.disconnect()

    def get_pipeline(self, *args, **kwargs):
        return RedisPipeline(self)


class Redis(RedisBase):

    def connect(self):
        return RedisClient(
            host=self.host, port=self.port, db=self.db,
            socket_timeout=self.timeout, password=self._password,
            unix_socket_path=self.unix_socket_path)


class StrictRedis(RedisBase):

    def connect(self):
        return StrictRedisClient(
            host=self.host, port=self.port, db=self.db,
            socket_timeout=self.timeout, password=self._password,
            unix_socket_path=self.unix_socket_path)
