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
from .base import LazyConnectionHandler, create_cluster, Cluster

connections = LazyConnectionHandler(lambda: conf.CONNECTIONS)
