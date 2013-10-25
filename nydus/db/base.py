"""
nydus.db.base
~~~~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

__all__ = ('LazyConnectionHandler', 'BaseCluster')

import collections
from nydus.db.map import DistributedContextManager
from nydus.db.routers import BaseRouter, routing_params
from nydus.utils import apply_defaults


def iter_hosts(hosts):
    # this can either be a dictionary (with the key acting as the numeric
    # index) or it can be a sorted list.
    if isinstance(hosts, collections.Mapping):
        return hosts.iteritems()
    return enumerate(hosts)


def create_connection(Connection, num, host_settings, defaults):
    # host_settings can be an iterable or a dictionary depending on the style
    # of connection (some connections share options and simply just need to
    # pass a single host, or a list of hosts)
    if isinstance(host_settings, collections.Mapping):
        return Connection(num, **apply_defaults(host_settings, defaults or {}))
    elif isinstance(host_settings, collections.Iterable):
        return Connection(num, *host_settings, **defaults or {})
    return Connection(num, host_settings, **defaults or {})


class BaseCluster(object):
    """
    Holds a cluster of connections.
    """
    class MaxRetriesExceededError(Exception):
        pass

    def __init__(self, hosts, backend, router=BaseRouter, max_connection_retries=20, defaults=None):
        self.hosts = dict(
            (conn_number, create_connection(backend, conn_number, host_settings, defaults))
            for conn_number, host_settings
            in iter_hosts(hosts)
        )
        self.max_connection_retries = max_connection_retries
        self.install_router(router)

    def __len__(self):
        return len(self.hosts)

    def __getitem__(self, name):
        return self.hosts[name]

    def __getattr__(self, name):
        return CallProxy(self, name)

    def __iter__(self):
        for name in self.hosts.iterkeys():
            yield name

    def install_router(self, router):
        self.router = router(self)

    def execute(self, path, args, kwargs):
        connections = self.__connections_for(path, args=args, kwargs=kwargs)

        results = []
        for conn in connections:
            for retry in xrange(self.max_connection_retries):
                func = conn
                for piece in path.split('.'):
                    func = getattr(func, piece)
                try:
                    results.append(func(*args, **kwargs))
                except tuple(conn.retryable_exceptions), e:
                    if not self.router.retryable:
                        raise e
                    elif retry == self.max_connection_retries - 1:
                        raise self.MaxRetriesExceededError(e)
                    else:
                        conn = self.__connections_for(path, retry_for=conn.num, args=args, kwargs=kwargs)[0]
                else:
                    break

        # If we only had one db to query, we simply return that res
        if len(results) == 1:
            return results[0]
        else:
            return results

    def disconnect(self):
        """Disconnects all connections in cluster"""
        for connection in self.hosts.itervalues():
            connection.disconnect()

    def get_conn(self, *args, **kwargs):
        """
        Returns a connection object from the router given ``args``.

        Useful in cases where a connection cannot be automatically determined
        during all steps of the process. An example of this would be
        Redis pipelines.
        """
        connections = self.__connections_for('get_conn', args=args, kwargs=kwargs)

        if len(connections) is 1:
            return connections[0]
        else:
            return connections

    def map(self, workers=None, **kwargs):
        return DistributedContextManager(self, workers, **kwargs)

    @routing_params
    def __connections_for(self, attr, args, kwargs, **fkwargs):
        return [self[n] for n in self.router.get_dbs(attr=attr, args=args, kwargs=kwargs, **fkwargs)]


class CallProxy(object):
    """
    Handles routing function calls to the proper connection.
    """
    def __init__(self, cluster, path):
        self.__cluster = cluster
        self.__path = path

    def __call__(self, *args, **kwargs):
        return self.__cluster.execute(self.__path, args, kwargs)

    def __getattr__(self, name):
        return CallProxy(self.__cluster, self.__path + '.' + name)


class LazyConnectionHandler(dict):
    """
    Maps clusters of connections within a dictionary.
    """
    def __init__(self, conf_callback):
        self.conf_callback = conf_callback
        self.conf_settings = {}
        self.__is_ready = False

    def __getitem__(self, key):
        if not self.is_ready():
            self.reload()
        return super(LazyConnectionHandler, self).__getitem__(key)

    def is_ready(self):
        return self.__is_ready

    def reload(self):
        from nydus.db import create_cluster

        for conn_alias, conn_settings in self.conf_callback().iteritems():
            self[conn_alias] = create_cluster(conn_settings)
        self._is_ready = True

    def disconnect(self):
        """Disconnects all connections in cluster"""
        for connection in self.itervalues():
            connection.disconnect()
