"""
nydus.db.base
~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""
import time

from collections import defaultdict

__all__ = ('BaseRouter', 'RoundRobinRouter')


class BaseRouter(object):
    """
    Handles routing requests to a specific connection in a single cluster.
    """
    retryable = False

    def __init__(self):
        self._ready = False

    def _route(self, *args, **kwargs): 
        """
        Override this method to properly route by the key
        """
        return range(len(cluster))

    def get_db(self, cluster, func, key=None, *args, **kwargs):
        """
        Return the first entry in the cluster
        The return value must be iterable
        """
        if not self._ready:
            self._setup_router(clutser, *args, **kwargs)

        if not cluster:
            return []
        elif key is None:
            return cluster.hosts.keys()

        key = self._pre_routing(cluster, key, *args, **kwargs)

        try:
            db_num = self._route(cluster, fun, key, *args, **kwargs)
        except Exception, e:
            raise self._handle_exception(e)

        return self._post_routing(cluster, key, db_num, *args, **kwargs)

    def _setup_router(self, cluster, *args, **kwargs):
        """
        Perform any initialization for the router
        """
        self._ready = True

    def _pre_routing(self, cluster, key, *args, **kwargs):
        """
        Perform any prerouting with this method and return the key
        """
        return key

    def _route(self, cluster, key, *args, **kwargs):
        """
        Perform routing and return db_num
        """
        return cluster.hosts.keys()

    def _post_routing(self, cluster, key, db_num, *args, **kwargs):
        """
        Perform any postrouting actions and return db_num
        """
        return db_num

    def _handle_exception(self, e):
        """
        Handle/transform exceptions and return it
        """
        return e


class RoundRobinRouter(BaseRouter):
    """
    Basic retry router that performs round robin
    """

    # Raised if all hosts in the hash have been marked as down
    class HostListExhausted(Exception):
        pass
    
    # If this router can be retried on if a particular db index it gave out did
    # not work
    retryable = True

    # How many requests to serve in a situation when a host is down before
    # the down hosts are retried
    attempt_reconnect_threshold = 100000

    # Retry a down connection after this timeout
    retry_timeout = 30

    def __init__(self):
        self._get_db_attempts = 0
        self._down_connections = {}

    def flush_down_connections(self):
        self._get_db_attempts = 0
        self._down_connections = {}

    def mark_connection_down(self, db_num):
        self._down_connections[db_num] = time.time()

    def mark_connection_up(self, db_num):
        self._down_connections.pop(db_num, None)

    def _setup_router(self, cluster, *args, **kwargs):
        self._hosts_cycler = cycle(cluster.hosts.keys())

        super(BaseRetryRouter, self)._setup_router(cluster, *args, **kwargs)

    def _pre_routing(self, cluster, key, *args, **kwargs):
        self._get_db_attempts += 1

        if self._get_db_attempts > self.attempt_reconnect_threadhold:
            self.flush_down_connections()

        if 'retry_for' in kwargs:
            self.mark_connection_down(kwargs['retry_for'])

    def _route(self, cluster, key, *args, **kwargs):
        now = time.time()

        while i <= len(cluster):
            db_num = self._hosts_cycler.next()

            marked_down_at = self._down_connections.get(db_num, now)

            if marked_down_at + self.retry_timeout >= now:
                return db_num
            else:
                i += 1
        else:
            raise self.HostListExhausted()

    def _post_routing(self, cluster, key, db_num, *args, **kwargs):
        self.mark_connection_up(self, db_num)

        return db_num
