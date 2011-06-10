"""
Disqus generic connections wrappers.

>>> from nydus.db import create_cluster
>>> redis = create_cluster({
>>>     'engine': 'nydus.db.backends.redis.Redis',
>>> })
>>> res = conn.incr('foo')
>>> assert res == 1
"""

from nydus.utils import import_string
from nydus.db.routers import BaseRouter

def create_cluster(settings):
    """
    redis = create_cluster({
        'engine': 'nydus.db.backends.redis.Redis',
        'router': 'nydus.db.routers.redis.PartitionRouter',
        'hosts': {
            0: {'db': 0},
            1: {'db': 1},
            2: {'db': 2},
        }
    })
    """
    # Pull in our client
    if isinstance(settings['engine'], basestring):
        conn = import_string(settings['engine'])
    else:
        conn = settings['engine']

    # Pull in our router
    router = settings.get('router')
    if isinstance(router, basestring):
        router = import_string(router)()
    elif router:
        router = router()
    else:
        router = BaseRouter()
        
    # Build the connection pool
    return Cluster(
        router=router,
        hosts=dict(
            (conn_number, conn(num=conn_number, **host_settings))
            for conn_number, host_settings
            in settings['hosts'].iteritems()
        ),
    )

class Cluster(object):
    """
    Holds a cluster of connections.
    """
    def __init__(self, hosts, router=None):
        self.hosts = hosts
        self.router = router
    
    def __len__(self):
        return len(self.hosts)

    def __getitem__(self, name):
        return self.hosts[name]

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return ConnectionProxy(self, attr)

    def disconnect(self):
        """Disconnects all connections in pool"""
        for connection in self.hosts.itervalues():
            connection.disconnect()

class ConnectionProxy(object):
    """
    Handles routing function calls to the proper connection.
    """
    def __init__(self, pool, attr):
        self.pool = pool
        self.attr = attr
    
    def __call__(self, *args, **kwargs):
        if self.pool.router:
            db_nums = self.pool.router.get_db(self.pool, self.attr, *args, **kwargs)
        else:
            db_nums = range(len(self.pool))

        results = [getattr(self.pool[n], self.attr)(*args, **kwargs) for n in db_nums]

        # If we only had one db to query, we simply return that res
        if len(results) == 1:
            return results[0]

        return results
