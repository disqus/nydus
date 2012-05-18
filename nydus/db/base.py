"""
nydus.db.base
~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

__all__ = ('LazyConnectionHandler', 'BaseCluster')

from collections import defaultdict
from nydus.db.routers import BaseRouter, routing_params
from nydus.utils import ThreadPool


class BaseCluster(object):
    """
    Holds a cluster of connections.
    """
    class MaxRetriesExceededError(Exception):
        pass

    def __init__(self, hosts, router=BaseRouter, max_connection_retries=20):
        self.hosts = hosts
        self.router = router(self)
        self.max_connection_retries = max_connection_retries

    def __len__(self):
        return len(self.hosts)

    def __getitem__(self, name):
        return self.hosts[name]

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return CallProxy(self, attr)

    def __iter__(self):
        for name in self.hosts.iterkeys():
            yield name

    def _execute(self, attr, args, kwargs):
        connections = self._connections_for(attr, args=args, kwargs=kwargs)

        results = []
        for conn in connections:
            for retry in xrange(self.max_connection_retries):
                try:
                    results.append(getattr(conn, attr)(*args, **kwargs))
                except tuple(conn.retryable_exceptions), e:
                    if not self.router.retryable:
                        raise e
                    elif retry == self.max_connection_retries - 1:
                        raise self.MaxRetriesExceededError(e)
                    else:
                        conn = self._connections_for(attr, retry_for=conn.num, args=args, kwargs=kwargs)[0]
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
        connections = self._connections_for('get_conn', args=args, kwargs=kwargs)

        if len(connections) is 1:
            return connections[0]
        else:
            return connections

    def map(self, workers=None):
        return DistributedContextManager(self, workers)

    @routing_params
    def _connections_for(self, attr, args, kwargs, **fkwargs):
        return [self[n] for n in self.router.get_dbs(attr=attr, args=args, kwargs=kwargs, **fkwargs)]


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
    _evaled = False

    # introspection support:
    __members__ = property(lambda self: self.__dir__())

    def __init__(self, attr):
        self._attr = attr
        self._wrapped = None
        self._evaled = False
        self._args = []
        self._kwargs = {}
        self._ident = None

    def __call__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._ident = ':'.join(map(str, [id(self._attr), id(self._args), id(self._kwargs)]))
        return self

    def __repr__(self):
        if self._evaled:
            return repr(self._wrapped)
        return u'<EventualCommand: %s args=%s kwargs=%s>' % (self._attr, self._args, self._kwargs)

    def __getattr__(self, name):
        if name in ('_attr', '_wrapped', '_evaled', '_args', '_kwargs', '_ident'):
            return getattr(self, name)
        return getattr(self._wrapped, name)

    def __setattr__(self, name, value):
        if name in ('_attr', '_wrapped', '_evaled', '_args', '_kwargs', '_ident'):
            return object.__setattr__(self, name, value)
        return setattr(self._wrapped, name, value)

    def __delattr__(self, name):
        if name in ('_attr', '_wrapped', '_evaled', '_args', '_kwargs', '_ident'):
            raise TypeError("can't delete %s." % name)
        delattr(self._wrapped, name)

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

    def _set_value(self, value):
        self._wrapped = value
        self._evaled = True

    def _execute(self, conn):
        return getattr(conn, self._attr)(*self._args, **self._kwargs)

    def __dict__(self):
        try:
            return self._current_object.__dict__
        except RuntimeError:
            return AttributeError('__dict__')
    __dict__ = property(__dict__)

    def __setitem__(self, key, value):
        self._wrapped[key] = value

    def __delitem__(self, key):
        del self._wrapped[key]

    def __setslice__(self, i, j, seq):
        self._wrapped[i:j] = seq

    def __delslice__(self, i, j):
        del self._wrapped[i:j]

    __lt__ = lambda x, o: x._wrapped < o
    __le__ = lambda x, o: x._wrapped <= o
    __eq__ = lambda x, o: x._wrapped == o
    __ne__ = lambda x, o: x._wrapped != o
    __gt__ = lambda x, o: x._wrapped > o
    __ge__ = lambda x, o: x._wrapped >= o
    __cmp__ = lambda x, o: cmp(x._wrapped, o)
    __hash__ = lambda x: hash(x._wrapped)
    # attributes are currently not callable
    # __call__ = lambda x, *a, **kw: x._wrapped(*a, **kw)
    __nonzero__ = lambda x: bool(x._wrapped)
    __len__ = lambda x: len(x._wrapped)
    __getitem__ = lambda x, i: x._wrapped[i]
    __iter__ = lambda x: iter(x._wrapped)
    __contains__ = lambda x, i: i in x._wrapped
    __getslice__ = lambda x, i, j: x._wrapped[i:j]
    __add__ = lambda x, o: x._wrapped + o
    __sub__ = lambda x, o: x._wrapped - o
    __mul__ = lambda x, o: x._wrapped * o
    __floordiv__ = lambda x, o: x._wrapped // o
    __mod__ = lambda x, o: x._wrapped % o
    __divmod__ = lambda x, o: x._wrapped.__divmod__(o)
    __pow__ = lambda x, o: x._wrapped ** o
    __lshift__ = lambda x, o: x._wrapped << o
    __rshift__ = lambda x, o: x._wrapped >> o
    __and__ = lambda x, o: x._wrapped & o
    __xor__ = lambda x, o: x._wrapped ^ o
    __or__ = lambda x, o: x._wrapped | o
    __div__ = lambda x, o: x._wrapped.__div__(o)
    __truediv__ = lambda x, o: x._wrapped.__truediv__(o)
    __neg__ = lambda x: -(x._wrapped)
    __pos__ = lambda x: +(x._wrapped)
    __abs__ = lambda x: abs(x._wrapped)
    __invert__ = lambda x: ~(x._wrapped)
    __complex__ = lambda x: complex(x._wrapped)
    __int__ = lambda x: int(x._wrapped)
    __long__ = lambda x: long(x._wrapped)
    __float__ = lambda x: float(x._wrapped)
    __str__ = lambda x: str(x._wrapped)
    __unicode__ = lambda x: unicode(x._wrapped)
    __oct__ = lambda x: oct(x._wrapped)
    __hex__ = lambda x: hex(x._wrapped)
    __index__ = lambda x: x._wrapped.__index__()
    __coerce__ = lambda x, o: x.__coerce__(x, o)
    __enter__ = lambda x: x.__enter__()
    __exit__ = lambda x, *a, **kw: x.__exit__(*a, **kw)


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
            self._commands = []
            return

        command_map = {}
        pipelined = all(self._cluster[n].supports_pipelines for n in self._cluster)
        pending_commands = defaultdict(list)

        # used in pipelining
        if pipelined:
            pipe_command_map = defaultdict(list)

            pipes = dict()  # db -> pipeline

        # build up a list of pending commands and their routing information
        for command in self._commands:
            cmd_ident = command._ident

            command_map[cmd_ident] = command

            if self._cluster.router:
                db_nums = self._cluster.router.get_dbs(
                    cluster=self._cluster,
                    attr=command._attr,
                    args=command._args,
                    kwargs=command._kwargs,
                )
            else:
                db_nums = self._cluster.keys()

            # The number of commands is based on the total number of executable commands
            num_commands += len(db_nums)

            # Don't bother with the pooling if we only need to do one operation on a single machine
            if num_commands == 1:
                self._commands = [command._execute(self._cluster[n]) for n in n]
                return

            # update the pipelined dbs
            for db_num in db_nums:
                # map the ident to a db
                if pipelined:
                    pipe_command_map[db_num].append(cmd_ident)

                # add to pending commands
                pending_commands[db_num].append(command)

        # Create the threadpool and pipe jobs into it
        pool = ThreadPool(min(self._workers, len(pending_commands)))

        # execute our pending commands either in the pool, or using a pipeline
        for db_num, command_list in pending_commands.iteritems():
            if pipelined:
                pipes[db_num] = self._cluster[db_num].get_pipeline()
            for command in command_list:
                if pipelined:
                    # add to pipeline
                    pipes[db_num].add(command)
                else:
                    # execute in pool
                    pool.add(command._ident, command._execute, [self._cluster[db_num]])

        # We need to finalize our commands with a single execute in pipelines
        if pipelined:
            for db, pipe in pipes.iteritems():
                pool.add(db, pipe.execute, (), {})

        # Consolidate commands with their appropriate results
        result_map = pool.join()

        # Results get grouped by their command signature, so we have to separate the logic
        if pipelined:
            for db, result in result_map.iteritems():
                if len(result) == 1:
                    result = result[0]
                for i, value in enumerate(result):
                    command_map[pipe_command_map[db][i]]._set_value(value)

        else:
            for command in self._commands:
                result = result_map[command._ident]
                if len(result) == 1:
                    result = result[0]
                command._set_value(result)

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
        from nydus.db import create_cluster

        for conn_alias, conn_settings in self.conf_callback().iteritems():
            self[conn_alias] = create_cluster(conn_settings)
        self._is_ready = True

    def disconnect(self):
        """Disconnects all connections in cluster"""
        for connection in self.itervalues():
            connection.disconnect()
