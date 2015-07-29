"""
nydus.db
~~~~~~~~

Disqus generic connections wrappers.

>>> from nydus.db import create_cluster
>>> redis = create_cluster({
>>>     'backend': 'nydus.db.backends.redis.Redis',
>>> })
>>> res = conn.incr('foo')
>>> assert res == 1

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""
import copy

from nydus import conf
from nydus.compat import string_types
from nydus.db.base import LazyConnectionHandler
from nydus.db.routers.base import BaseRouter
from nydus.utils import import_string


__all__ = ('create_cluster', 'connections', 'Cluster')


def create_cluster(settings):
    """
    Creates a new Nydus cluster from the given settings.

    :param settings: Dictionary of the cluster settings.
    :returns: Configured instance of ``nydus.db.base.Cluster``.

    >>> redis = create_cluster({
    >>>     'backend': 'nydus.db.backends.redis.Redis',
    >>>     'router': 'nydus.db.routers.redis.PartitionRouter',
    >>>     'defaults': {
    >>>         'host': 'localhost',
    >>>         'port': 6379,
    >>>     },
    >>>     'hosts': {
    >>>         0: {'db': 0},
    >>>         1: {'db': 1},
    >>>         2: {'db': 2},
    >>>     }
    >>> })
    """
    # Pull in our client
    settings = copy.deepcopy(settings)
    backend = settings.pop('engine', settings.pop('backend', None))
    if isinstance(backend, string_types):
        Conn = import_string(backend)
    elif backend:
        Conn = backend
    else:
        raise KeyError('backend')

    # Pull in our cluster
    cluster = settings.pop('cluster', None)
    if not cluster:
        Cluster = Conn.get_cluster()
    elif isinstance(cluster, string_types):
        Cluster = import_string(cluster)
    else:
        Cluster = cluster

    # Pull in our router
    router = settings.pop('router', None)
    if not router:
        Router = BaseRouter
    elif isinstance(router, string_types):
        Router = import_string(router)
    else:
        Router = router

    # Build the connection cluster
    return Cluster(
        router=Router,
        backend=Conn,
        **settings
    )

connections = LazyConnectionHandler(lambda: conf.CONNECTIONS)
