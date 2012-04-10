"""
nydus.db.base
~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""
import time

from itertools import cycle

__all__ = ('BaseRouter', 'RoundRobinRouter')


class BaseRouter(object):
    """
    Handles routing requests to a specific connection in a single cluster.
    """
    retryable = False

    class UnableToSetupRouter(Exception):
        pass

    def __init__(self, *args, **kwargs):
        self._ready = False

    def get_dbs(self, cluster, attr, key=None, *args, **kwargs):
        """
        Perform setup and routing
        Always return an iterable
        Do not overload this method
        """
        if not self._ready:
            if not self.setup_router(cluster, *args, **kwargs):
                raise self.UnableToSetupRouter()

        key = self._pre_routing(cluster, attr, key, *args, **kwargs)

        if not key:
            return cluster.hosts.keys()

        try:
            db_nums = self._route(cluster, attr, key, *args, **kwargs)
        except Exception, e:
            self._handle_exception(e)
            db_nums = []

        return self._post_routing(cluster, attr, key, db_nums, *args, **kwargs)

    # Backwards compatibilty
    get_db = get_dbs

    def setup_router(self, cluster, *args, **kwargs):
        """
        Call method to perform any setup
        """
        self._ready = self._setup_router(cluster, *args, **kwargs)

        return self._ready

    def _setup_router(self, cluster, *args, **kwargs):
        """
        Perform any initialization for the router
        Returns False if setup could not be completed
        """
        return True

    def _pre_routing(self, cluster, attr, key, *args, **kwargs):
        """
        Perform any prerouting with this method and return the key
        """
        return key

    def _route(self, cluster, attr, key, *args, **kwargs):
        """
        Perform routing and return db_nums
        """
        return cluster.hosts.keys()

    def _post_routing(self, cluster, attr, key, db_nums, *args, **kwargs):
        """
        Perform any postrouting actions and return db_nums
        """
        return db_nums

    def _handle_exception(self, e):
        """
        Handle/transform exceptions and return it
        """
        raise e


class RoundRobinRouter(BaseRouter):
    """
    Basic retry router that performs round robin
    """

    # Raised if all hosts in the hash have been marked as down
    class HostListExhausted(Exception):
        pass

    class InvalidDBNum(Exception):
        pass
    
    # If this router can be retried on if a particular db index it gave out did
    # not work
    retryable = True

    # How many requests to serve in a situation when a host is down before
    # the down hosts are retried
    attempt_reconnect_threshold = 100000

    # Retry a down connection after this timeout
    retry_timeout = 30

    def __init__(self, *args, **kwargs):
        self._get_db_attempts = 0
        self._down_connections = {}

        super(RoundRobinRouter,self).__init__(*args, **kwargs)

    @classmethod
    def ensure_db_num(cls, db_num):
        try:
            return int(db_num)
        except ValueError:
            raise cls.InvalidDBNum()

    def flush_down_connections(self):
        self._get_db_attempts = 0
        self._down_connections = {}

    def mark_connection_down(self, db_num):
        db_num = self.ensure_db_num(db_num)
        self._down_connections[db_num] = time.time()

    def mark_connection_up(self, db_num):
        db_num = self.ensure_db_num(db_num)
        self._down_connections.pop(db_num, None)

    def _setup_router(self, cluster, *args, **kwargs):
        self._hosts_cycler = cycle(cluster.hosts.keys())

        return True

    def _pre_routing(self, cluster, attr, key, *args, **kwargs):
        self._get_db_attempts += 1

        if self._get_db_attempts > self.attempt_reconnect_threshold:
            self.flush_down_connections()

        if 'retry_for' in kwargs:
            self.mark_connection_down(kwargs['retry_for'])

        return key

    def _route(self, cluster, attr, key, *args, **kwargs):
        now = time.time()

        for i in xrange(len(cluster)):
            db_num = self._hosts_cycler.next()

            marked_down_at = self._down_connections.get(db_num, False)

            if not marked_down_at or (marked_down_at + self.retry_timeout <= now):
                return [db_num]
        else:
            raise self.HostListExhausted()

    def _post_routing(self, cluster, attr, key, db_nums, *args, **kwargs):
        if db_nums:
           self.mark_connection_up(db_nums[0])

        return db_nums

