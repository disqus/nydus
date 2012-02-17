"""
nydus.db.routers.redis
~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from binascii import crc32
from itertools import cycle

from nydus.db.routers import BaseRouter
from nydus.contrib.ketama import Ketama


class PartitionRouter(BaseRouter):
    def get_db(self, cluster, func, key=None, *args, **kwargs):
        # Assume first argument is a key
        if not key:
            return range(len(cluster))
        return [crc32(str(key)) % len(cluster)]


class RoundRobinRouter(BaseRouter):

    def _get_db__round_robin(self, cluster):
        c = cycle(range(len(cluster)))
        for x in c:
            yield x

    def get_db(self, cluster, *args, **kwargs):
        if not cluster:
            return []
        if not hasattr(self, 'cycler'):
            self.cycler = self._get_db__round_robin(cluster)
        return [self.cycler.next()]


class ConsistentHashingRouter(BaseRouter):
    '''
    Router that returns host number based on a consistent hashing algorithm.
    The consistent hashing algorithm only works if a key argument is provided.
    If a key is not provided, then all hosts are returned.
    '''

    # Raised if all hosts in the hash have been marked as down
    class HostListExhaused(Exception):
        pass

    # If this router can be retried on if a particular db index it gave out did
    # not work
    retryable = True

    # How many requests to serve in a situation when a host is down before
    # the down hosts are retried
    attempt_reconnect_threshold = 100000

    def __init__(self):
        self._get_db_attempts = 0
        self._down_connections = set()

    # There is one instance of this class that lives inside the Cluster object
    def get_db(self, cluster, func, key=None, *args, **kwargs):
        self._setup_hash_and_connections(cluster, *args, **kwargs)

        if not cluster:
            return []
        elif not key:
            return range(len(cluster))
        else:
            return self._host_indexes_for(key, cluster)

    def flush_down_connections(self):
        for connection in self._down_connections:
            self._hash.add_node(connection.identifier)

        self._down_connections = set()

    def _setup_hash_and_connections(self, cluster, *args, **kwargs):
        # Create the hash if it doesn't exist yet
        if not hasattr(self, '_hash'):
            strings = [h.identifier for (i, h) in cluster.hosts.items()]
            self._hash = Ketama(strings)

        self._handle_host_retries(cluster, retry_for=kwargs.get('retry_for'))

    def _handle_host_retries(self, cluster, retry_for):
        self._get_db_attempts += 1

        if self._get_db_attempts > self.attempt_reconnect_threshold:
            self.flush_down_connections()
            self._get_db_attempts = 0

        if retry_for is not None:
            self._mark_connection_as_down(cluster[retry_for])

    def _mark_connection_as_down(self, connection):
        self._hash.remove_node(connection.identifier)
        self._down_connections.add(connection)

    def _host_indexes_for(self, key, cluster):
        found = self._hash.get_node(key)

        if not found and len(self._down_connections) > 0:
            raise self.HostListExhaused

        return [i for (i, h) in cluster.hosts.items()
                if h.identifier == found]
