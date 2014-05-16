"""
nydus.db.base
~~~~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
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

    For the most part, all public functions will receive arguments as ``key=value``
    pairs and should expect as much. Functions which receive ``args`` and ``kwargs``
    from the calling function will receive default values for those, and need not
    worry about handling missing arguments.
    """
    retryable = False

    class UnableToSetupRouter(Exception):
        pass

    def __init__(self, cluster=None, *args, **kwargs):
        self._ready = False
        self.cluster = cluster

    @routing_params
    def get_dbs(self, attr, args, kwargs, **fkwargs):
        """
        Returns a list of db keys to route the given call to.

        :param attr: Name of attribute being called on the connection.
        :param args: List of arguments being passed to ``attr``.
        :param kwargs: Dictionary of keyword arguments being passed to ``attr``.

        >>> redis = Cluster(router=BaseRouter)
        >>> router = redis.router
        >>> router.get_dbs('incr', args=('key name', 1))
        [0,1,2]

        """
        if not self._ready:
            if not self.setup_router(args=args, kwargs=kwargs, **fkwargs):
                raise self.UnableToSetupRouter()

        retval = self._pre_routing(attr=attr, args=args, kwargs=kwargs, **fkwargs)
        if retval is not None:
            args, kwargs = retval

        if not (args or kwargs):
            return self.cluster.hosts.keys()

        try:
            db_nums = self._route(attr=attr, args=args, kwargs=kwargs, **fkwargs)
        except Exception as e:
            self._handle_exception(e)
            db_nums = []

        return self._post_routing(attr=attr, db_nums=db_nums, args=args, kwargs=kwargs, **fkwargs)

    # Backwards compatibilty
    get_db = get_dbs

    @routing_params
    def setup_router(self, args, kwargs, **fkwargs):
        """
        Call method to perform any setup
        """
        self._ready = self._setup_router(args=args, kwargs=kwargs, **fkwargs)

        return self._ready

    @routing_params
    def _setup_router(self, args, kwargs, **fkwargs):
        """
        Perform any initialization for the router
        Returns False if setup could not be completed
        """
        return True

    @routing_params
    def _pre_routing(self, attr, args, kwargs, **fkwargs):
        """
        Perform any prerouting with this method and return the key
        """
        return args, kwargs

    @routing_params
    def _route(self, attr, args, kwargs, **fkwargs):
        """
        Perform routing and return db_nums
        """
        return self.cluster.hosts.keys()

    @routing_params
    def _post_routing(self, attr, db_nums, args, kwargs, **fkwargs):
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
    # the down hosts are assesed for readmittance back into the pool of serving
    # requests.
    #
    # If the attempt_reconnect_threshold is hit, it does not guarantee that the
    # down hosts will be put back - only that the router will CHECK to see if
    # the hosts CAN be put back.  The elegibility of a host being put back is
    # handlede in the check_down_connections method, which by default will
    # readmit a host if it was marked down more than retry_timeout seconds ago.
    attempt_reconnect_threshold = 100000

    # Number of seconds a host must be marked down before it is elligable to be
    # put back in the pool and retried.
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

    def check_down_connections(self):
        """
        Iterates through all connections which were previously listed as unavailable
        and marks any that have expired their retry_timeout as being up.
        """
        now = time.time()

        for db_num, marked_down_at in self._down_connections.items():
            if marked_down_at + self.retry_timeout <= now:
                self.mark_connection_up(db_num)

    def flush_down_connections(self):
        """
        Marks all connections which were previously listed as unavailable as being up.
        """
        self._get_db_attempts = 0
        for db_num in self._down_connections.keys():
            self.mark_connection_up(db_num)

    def mark_connection_down(self, db_num):
        db_num = self.ensure_db_num(db_num)
        self._down_connections[db_num] = time.time()

    def mark_connection_up(self, db_num):
        db_num = self.ensure_db_num(db_num)
        self._down_connections.pop(db_num, None)

    @routing_params
    def _setup_router(self, args, kwargs, **fkwargs):
        self._hosts_cycler = cycle(self.cluster.hosts.keys())

        return True

    @routing_params
    def _pre_routing(self, attr, args, kwargs, retry_for=None, **fkwargs):
        self._get_db_attempts += 1

        if self._get_db_attempts > self.attempt_reconnect_threshold:
            self.check_down_connections()

        if retry_for is not None:
            self.mark_connection_down(retry_for)

        return args, kwargs

    @routing_params
    def _route(self, attr, args, kwargs, **fkwargs):
        now = time.time()

        for i in xrange(len(self.cluster)):
            db_num = self._hosts_cycler.next()

            marked_down_at = self._down_connections.get(db_num, False)

            if not marked_down_at or (marked_down_at + self.retry_timeout <= now):
                return [db_num]
        else:
            raise self.HostListExhausted()

    @routing_params
    def _post_routing(self, attr, db_nums, args, kwargs, **fkwargs):
        if db_nums and db_nums[0] in self._down_connections:
            self.mark_connection_up(db_nums[0])

        return db_nums
