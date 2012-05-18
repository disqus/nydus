"""
nydus.db
~~~~~~~~

Disqus generic connections wrappers.

>>> from nydus.db import create_cluster
>>> redis = create_cluster({
>>>     'engine': 'nydus.db.backends.redis.Redis',
>>> })
>>> res = conn.incr('foo')
>>> assert res == 1

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

__all__ = ('create_cluster', 'connections', 'Cluster')

from nydus import conf
from nydus.db.base import LazyConnectionHandler, BaseCluster
from nydus.db.routers.base import BaseRouter
from nydus.utils import import_string, apply_defaults


def create_cluster(settings):
    """
    Creates a new Nydus cluster from the given settings.

    :param settings: Dictionary of the cluster settings.
    :returns: Configured instance of ``nydus.db.base.Cluster``.

    >>> redis = create_cluster({
    >>>     'engine': 'nydus.db.backends.redis.Redis',
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
    # Pull in our cluster
    cluster = settings.get('cluster')
    if not cluster:
        Cluster = BaseCluster
    elif isinstance(cluster, basestring):
        Conn = import_string(cluster)
    else:
        Conn = cluster

    # Pull in our client
    if isinstance(settings['engine'], basestring):
        Conn = import_string(settings['engine'])
    else:
        Conn = settings['engine']

    # Pull in our router
    router = settings.get('router')
    if isinstance(router, basestring):
        router = import_string(router)
    elif router:
        router = router
    else:
        router = BaseRouter

    defaults = settings.get('defaults', {})

    # Build the connection cluster
    return Cluster(
        router=router,
        hosts=dict(
            (conn_number, Conn(num=conn_number, **apply_defaults(host_settings, defaults)))
            for conn_number, host_settings
            in settings['hosts'].iteritems()
        ),
    )

connections = LazyConnectionHandler(lambda: conf.CONNECTIONS)
