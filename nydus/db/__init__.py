"""
nydus.contrib.django.models
~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

from nydus import conf
from nydus.db.routers import BaseRouter
from nydus.utils import import_string, ThreadPool

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
        
    # Build the connection cluster
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
            return CallProxy(self, attr)

    def _execute(self, attr, args, kwargs):
        if self.router:
            db_nums = self.router.get_db(self, attr, *args, **kwargs)
        else:
            db_nums = range(len(self))

        results = [getattr(self.hosts[num], attr)(*args, **kwargs) for num in db_nums]

        # If we only had one db to query, we simply return that res
        if len(results) == 1:
            return results[0]

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
        if self.router:
            db_nums = self.router.get_db(self, 'get_conn', *args, **kwargs)
        else:
            db_nums = range(len(self))
        
        if len(db_nums) == 1:
            return self[db_nums[0]]
        return [self[n] for n in db_nums]

    def map(self, workers=None):
        return DistributedContextManager(self, workers)

class CallProxy(object):
    """
    Handles routing function calls to the proper connection.
    """
    def __init__(self, cluster, attr):
        self._cluster = cluster
        self._attr = attr
    
    def __call__(self, *args, **kwargs):
        return self._cluster._execute(self._attr, args, kwargs)

class EventualCommand(object):
    _attr = None
    _wrapped = None

    # introspection support:
    __members__ = property(lambda self: self.__dir__())

    def __init__(self, attr):
        self.__dict__.update({
            '_attr': attr,
            '_wrapped': None,
            '_args': [],
            '_kwargs': {},
            '_ident': None,
        })
        
    def __call__(self, *args, **kwargs):
        self.__dict__.update({
            '_args': args,
            '_kwargs': kwargs,
            '_ident': ':'.join(map(str, [id(self._attr), id(self._args), id(self._kwargs)])),
        })
        return self

    def __repr__(self):
        return repr(self._wrapped)

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        return getattr(self._wrapped, name)

    def __setattr__(self, name, value):
        if name in self.__dict__:
            self.__dict__[name] = value
        else:
            setattr(self._wrapped, name, value)

    def __delattr__(self, name):
        if name == "_wrapped":
            raise TypeError("can't delete _wrapped.")
        delattr(self._wrapped, name)

    def __dir__(self):
        return dir(self._wrapped)

    def __str__(self):
        return str(self._wrapped)

    def __unicode__(self):
        return unicode(self._wrapped)

    def __deepcopy__(self, memo):
        # Changed to use deepcopy from copycompat, instead of copy
        # For Python 2.4.
        from django.utils.copycompat import deepcopy
        return deepcopy(self._wrapped, memo)

    # Need to pretend to be the wrapped class, for the sake of objects that care
    # about this (especially in equality tests)
    def __get_class(self):
        return self._wrapped.__class__
    __class__ = property(__get_class)

    def __eq__(self, other):
        return self._wrapped == other

    def __hash__(self):
        return hash(self._wrapped)

class DistributedConnection(object):
    def __init__(self, cluster, workers=None):
        self._cluster = cluster
        self._workers = min(workers or len(cluster), 16)
        self._commands = []
        self._complete = False

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            command = EventualCommand(attr)
            self._commands.append(command)
            return command

    def _execute(self):
        num_commands = len(self._commands)
        if num_commands == 0:
            self._results = []
            return

        pool = None

        for command in self._commands:
            if self._cluster.router:
                db_nums = self._cluster.router.get_db(self._cluster, command._attr, *command._args, **command._kwargs)
            else:
                db_nums = range(len(self._cluster))

            num_commands += len(db_nums)

            # Don't bother with the ThreadPool if we only need to do one operation
            if num_commands == 1:
                self._results = [getattr(self._cluster[db_num], command._attr)(*command._args, **command._kwargs) for db_num in db_nums]
                return

            elif not pool:
                pool = ThreadPool(self._workers)

            for db_num in db_nums:
                pool.add(command._ident, getattr(self._cluster[db_num], command._attr), command._args, command._kwargs)

        result_map = pool.join()
        for command in self._commands:
            result = result_map[command._ident]
            if len(result) == 1:
                result = result[0]
            command._wrapped = result
    
        self._complete = True
    
    def get_results(self):
        assert self._complete, 'you must execute the commands before fetching results'

        return self._commands

class DistributedContextManager(object):
    def __init__(self, cluster, workers=None):
        self._workers = workers
        self._cluster = cluster

    def __enter__(self):
        self._handler = DistributedConnection(self._cluster, self._workers)
        return self._handler

    def __exit__(self, exc_type, exc_value, tb):
        # we need to break up each command and route it
        self._handler._execute()

class LazyConnectionHandler(dict):
    """
    Maps clusters of connections within a dictionary.
    """
    def __init__(self, conf_callback):
        self.conf_callback = conf_callback
        self.conf_settings = {}
        self._is_ready = False
    
    def __getitem__(self, key):
        if not self.is_ready():
            self.reload()
        return super(LazyConnectionHandler, self).__getitem__(key)

    def __getattr__(self, key):
        if not self.is_ready():
            self.reload()
        return super(LazyConnectionHandler, self).__getattr__(key)

    def is_ready(self):
        return self._is_ready
        # if self.conf_settings != self.conf_callback():
        #     return False
        # return True

    def reload(self):
        for conn_alias, conn_settings in self.conf_callback().iteritems():
            self[conn_alias] = create_cluster(conn_settings)        
        self._is_ready = True

    def disconnect(self):
        """Disconnects all connections in cluster"""
        for connection in self.itervalues():
            connection.disconnect()

connections = LazyConnectionHandler(lambda:conf.CONNECTIONS)