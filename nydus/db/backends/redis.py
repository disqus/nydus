"""
nydus.db.backends.redis
~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

from redis import Redis as RedisClient, StrictRedis
from redis import ConnectionError, InvalidResponse

from nydus.db.backends import BaseConnection, BasePipeline
from nydus.compat import izip


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


class Redis(BaseConnection):
    # Exceptions that can be retried by this backend
    retryable_exceptions = frozenset([ConnectionError, InvalidResponse])
    supports_pipelines = True

    def __init__(self, num, host='localhost', port=6379, db=0, timeout=None,
                 password=None, unix_socket_path=None, identifier=None,
                 strict=True):
        self.host = host
        self.port = port
        self.db = db
        self.unix_socket_path = unix_socket_path
        self.timeout = timeout
        self.strict = strict
        self.__identifier = identifier
        self.__password = password
        super(Redis, self).__init__(num)

    @property
    def identifier(self):
        if self.__identifier is not None:
            return self.__identifier
        mapping = vars(self)
        mapping['klass'] = self.__class__.__name__
        return "redis://%(host)s:%(port)s/%(db)s" % mapping

    def connect(self):
        if self.strict:
            cls = StrictRedis
        else:
            cls = RedisClient
        return cls(
            host=self.host, port=self.port, db=self.db,
            socket_timeout=self.timeout, password=self.__password,
            unix_socket_path=self.unix_socket_path)

    def disconnect(self):
        self.connection.disconnect()

    def get_pipeline(self, *args, **kwargs):
        return RedisPipeline(self)
