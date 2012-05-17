"""
nydus.db.base
~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

__all__ = ('BaseRouter', 'RoundRobinRouter', 'routing_params')

import time

from functools import wraps
from itertools import cycle


def routing_params(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        if kwargs.get('kwargs') is None:
            kwargs['kwargs'] = {}

        if kwargs.get('args') is None:
            kwargs['args'] = ()

        return func(*args, **kwargs)
    wrapped.__wraps__ = getattr(func, '__wraps__', func)
    return wrapped


class BaseRouter(object):
    """
    Handles routing requests to a specific connection in a single cluster.
    """
    retryable = False

    class UnableToSetupRouter(Exception):
        pass

    def __init__(self, *args, **kwargs):
        self._ready = False

    @routing_params
    def get_dbs(self, cluster, attr, args, kwargs, **fkwargs):
        """
        Perform setup and routing
        Always return an iterable
        Do not overload this method
        """
        if not self._ready:
            if not self.setup_router(cluster=cluster, args=args, kwargs=kwargs, **fkwargs):
                raise self.UnableToSetupRouter()

        retval = self._pre_routing(cluster, attr, args=args, kwargs=kwargs, **fkwargs)
        if retval is not None:
            args, kwargs = retval

        if not (args or kwargs):
            return cluster.hosts.keys()

        try:
            db_nums = self._route(cluster, attr, args=args, kwargs=kwargs, **fkwargs)
        except Exception, e:
            self._handle_exception(e)
            db_nums = []

        return self._post_routing(cluster, attr, db_nums, args=args, kwargs=kwargs, **fkwargs)

    # Backwards compatibilty
    get_db = get_dbs

    @routing_params
    def setup_router(self, cluster, args, kwargs, **fkwargs):
        """
        Call method to perform any setup
        """
        self._ready = self._setup_router(cluster, args=args, kwargs=kwargs, **fkwargs)

        return self._ready

    @routing_params
    def _setup_router(self, cluster, args, kwargs, **fkwargs):
        """
        Perform any initialization for the router
        Returns False if setup could not be completed
        """
        return True

    @routing_params
    def _pre_routing(self, cluster, attr, args, kwargs, **fkwargs):
        """
        Perform any prerouting with this method and return the key
        """
        return args, kwargs

    @routing_params
    def _route(self, cluster, attr, args, kwargs, **fkwargs):
        """
        Perform routing and return db_nums
        """
        return cluster.hosts.keys()

    @routing_params
    def _post_routing(self, cluster, attr, db_nums, args, kwargs, **fkwargs):
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

        super(RoundRobinRouter, self).__init__(*args, **kwargs)

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

    @routing_params
    def _setup_router(self, cluster, args, kwargs, **fkwargs):
        self._hosts_cycler = cycle(cluster.hosts.keys())

        return True

    @routing_params
    def _pre_routing(self, cluster, attr, args, kwargs, retry_for=None, **fkwargs):
        self._get_db_attempts += 1

        if self._get_db_attempts > self.attempt_reconnect_threshold:
            self.flush_down_connections()

        if retry_for is not None:
            self.mark_connection_down(retry_for)

        return args, kwargs

    @routing_params
    def _route(self, cluster, attr, args, kwargs, **fkwargs):
        now = time.time()

        for i in xrange(len(cluster)):
            db_num = self._hosts_cycler.next()

            marked_down_at = self._down_connections.get(db_num, False)

            if not marked_down_at or (marked_down_at + self.retry_timeout <= now):
                return [db_num]
        else:
            raise self.HostListExhausted()

    @routing_params
    def _post_routing(self, cluster, attr, db_nums, args, kwargs, **fkwargs):
        if db_nums:
            self.mark_connection_up(db_nums[0])

        return db_nums
