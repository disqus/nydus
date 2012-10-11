"""
nydus.db.base
~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

__all__ = ('LazyConnectionHandler', 'BaseCluster')

from functools import wraps
from itertools import izip
from collections import defaultdict
from nydus.db.routers import BaseRouter, routing_params
from nydus.utils import ThreadPool


class CommandError(Exception):
    def __init__(self, errors):
        self.errors = errors

    def __repr__(self):
        return '<%s (%d): %r>' % (type(self), len(self.errors), self.errors)

    def __str__(self):
        return '%d command(s) failed: %r' % (len(self.errors), self.errors)


class BaseCluster(object):
    """
    Holds a cluster of connections.
    """
    class MaxRetriesExceededError(Exception):
        pass

    def __init__(self, hosts, router=BaseRouter, max_connection_retries=20):
        self.hosts = hosts
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
            func = conn
            for piece in path.split('.'):
                func = getattr(func, piece)

            for retry in xrange(self.max_connection_retries):
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


def promise_method(func):
    """
    A decorator which ensures that once a method has been marked as resolved
    (via Class.__resolved)) will then propagate the attribute (function) call
    upstream.
    """
    name = func.__name__

    @wraps(func)
    def wrapped(self, *args, **kwargs):
        if getattr(self, '_%s__resolved' % (type(self).__name__,)):
            return getattr(self.__wrapped, name)(*args, **kwargs)
        return func(self, *args, **kwargs)
    return wrapped


def change_resolution(command, value):
    """
    Public API to change the resolution of an already resolved EventualCommand result value.
    """
    command._EventualCommand__wrapped = value
    command._EventualCommand__resolved = True


class EventualCommand(object):
    # introspection support:
    __members__ = property(lambda self: self.__dir__())

    def __init__(self, attr, args=None, kwargs=None):
        self.__attr = attr
        self.__wrapped = None
        self.__resolved = False
        self.__args = args or []
        self.__kwargs = kwargs or {}
        self.__ident = ':'.join(map(str, [id(self.__attr), id(self.__args), id(self.__kwargs)]))

    def __call__(self, *args, **kwargs):
        self.__args = args
        self.__kwargs = kwargs
        self.__ident = ':'.join(map(str, [id(self.__attr), id(self.__args), id(self.__kwargs)]))
        return self

    def __hash__(self):
        # We return our ident
        return hash(self.__ident)

    def __repr__(self):
        if self.__resolved:
            return repr(self.__wrapped)
        return u'<EventualCommand: %s args=%s kwargs=%s>' % (self.__attr, self.__args, self.__kwargs)

    def __getattr__(self, name):
        return getattr(self.__wrapped, name)

    def __setattr__(self, name, value):
        if name.startswith('_%s' % (type(self).__name__,)):
            return object.__setattr__(self, name, value)
        return setattr(self.__wrapped, name, value)

    def __delattr__(self, name):
        if name.startswith('_%s' % (type(self).__name__,)):
            raise TypeError("can't delete %s." % name)
        delattr(self.__wrapped, name)

    def __deepcopy__(self, memo):
        # Changed to use deepcopy from copycompat, instead of copy
        # For Python 2.4.
        from django.utils.copycompat import deepcopy
        return deepcopy(self.__wrapped, memo)

    # Need to pretend to be the wrapped class, for the sake of objects that care
    # about this (especially in equality tests)
    def __get_class(self):
        return self.__wrapped.__class__
    __class__ = property(__get_class)

    def __dict__(self):
        try:
            return vars(self.__wrapped)
        except RuntimeError:
            return AttributeError('__dict__')
    __dict__ = property(__dict__)

    def __setitem__(self, key, value):
        self.__wrapped[key] = value

    def __delitem__(self, key):
        del self.__wrapped[key]

    def __setslice__(self, i, j, seq):
        self.__wrapped[i:j] = seq

    def __delslice__(self, i, j):
        del self.__wrapped[i:j]

    __lt__ = lambda x, o: x.__wrapped < o
    __le__ = lambda x, o: x.__wrapped <= o
    __eq__ = lambda x, o: x.__wrapped == o
    __ne__ = lambda x, o: x.__wrapped != o
    __gt__ = lambda x, o: x.__wrapped > o
    __ge__ = lambda x, o: x.__wrapped >= o
    __cmp__ = lambda x, o: cmp(x.__wrapped, o)
    # attributes are currently not callable
    # __call__ = lambda x, *a, **kw: x.__wrapped(*a, **kw)
    __nonzero__ = lambda x: bool(x.__wrapped)
    __len__ = lambda x: len(x.__wrapped)
    __getitem__ = lambda x, i: x.__wrapped[i]
    __iter__ = lambda x: iter(x.__wrapped)
    __contains__ = lambda x, i: i in x.__wrapped
    __getslice__ = lambda x, i, j: x.__wrapped[i:j]
    __add__ = lambda x, o: x.__wrapped + o
    __sub__ = lambda x, o: x.__wrapped - o
    __mul__ = lambda x, o: x.__wrapped * o
    __floordiv__ = lambda x, o: x.__wrapped // o
    __mod__ = lambda x, o: x.__wrapped % o
    __divmod__ = lambda x, o: x.__wrapped.__divmod__(o)
    __pow__ = lambda x, o: x.__wrapped ** o
    __lshift__ = lambda x, o: x.__wrapped << o
    __rshift__ = lambda x, o: x.__wrapped >> o
    __and__ = lambda x, o: x.__wrapped & o
    __xor__ = lambda x, o: x.__wrapped ^ o
    __or__ = lambda x, o: x.__wrapped | o
    __div__ = lambda x, o: x.__wrapped.__div__(o)
    __truediv__ = lambda x, o: x.__wrapped.__truediv__(o)
    __neg__ = lambda x: -(x.__wrapped)
    __pos__ = lambda x: +(x.__wrapped)
    __abs__ = lambda x: abs(x.__wrapped)
    __invert__ = lambda x: ~(x.__wrapped)
    __complex__ = lambda x: complex(x.__wrapped)
    __int__ = lambda x: int(x.__wrapped)
    __long__ = lambda x: long(x.__wrapped)
    __float__ = lambda x: float(x.__wrapped)
    __str__ = lambda x: str(x.__wrapped)
    __unicode__ = lambda x: unicode(x.__wrapped)
    __oct__ = lambda x: oct(x.__wrapped)
    __hex__ = lambda x: hex(x.__wrapped)
    __index__ = lambda x: x.__wrapped.__index__()
    __coerce__ = lambda x, o: x.__coerce__(x, o)
    __enter__ = lambda x: x.__enter__()
    __exit__ = lambda x, *a, **kw: x.__exit__(*a, **kw)

    @property
    def is_error(self):
        return isinstance(self.__wrapped, CommandError)

    @promise_method
    def resolve(self, conn):
        value = getattr(conn, self.__attr)(*self.__args, **self.__kwargs)
        return self.resolve_as(value)

    @promise_method
    def resolve_as(self, value):
        self.__wrapped = value
        self.__resolved = True
        return value

    @promise_method
    def get_command(self):
        return (self.__attr, self.__args, self.__kwargs)

    @promise_method
    def get_name(self):
        return self.__attr

    @promise_method
    def get_args(self):
        return self.__args

    @promise_method
    def get_kwargs(self):
        return self.__kwargs

    @promise_method
    def set_args(self, args):
        self.__args = args

    @promise_method
    def set_kwargs(self, kwargs):
        self.__kwargs = kwargs

    @promise_method
    def clone(self):
        return EventualCommand(self.__attr, self.__args, self.__kwargs)


class DistributedConnection(object):
    def __init__(self, cluster, workers=None, fail_silently=False):
        self.__cluster = cluster
        self.__workers = min(workers or len(cluster), 16)
        self.__commands = []
        self.__complete = False
        self.__errors = []
        self.__fail_silently = fail_silently
        self.__resolved = False

    def __getattr__(self, attr):
        command = EventualCommand(attr)
        self.__commands.append(command)
        return command

    @promise_method
    def resolve(self):
        num_commands = len(self.__commands)
        if num_commands == 0:
            self.__commands = []
            return

        command_map = {}
        pipelined = all(self.__cluster[n].supports_pipelines for n in self.__cluster)
        pending_commands = defaultdict(list)

        # used in pipelining
        if pipelined:
            pipe_command_map = defaultdict(list)
            pipes = dict()  # db -> pipeline

        # build up a list of pending commands and their routing information
        for command in self.__commands:
            cmd_ident = hash(command)

            command_map[cmd_ident] = command

            if self.__cluster.router:
                name, args, kwargs = command.get_command()
                db_nums = self.__cluster.router.get_dbs(
                    cluster=self.__cluster,
                    attr=name,
                    args=args,
                    kwargs=kwargs,
                )
            else:
                db_nums = self.__cluster.keys()

            # The number of commands is based on the total number of executable commands
            num_commands += len(db_nums)

            # Don't bother with the pooling if we only need to do one operation on a single machine
            if num_commands == 1:
                self._commands = [command.resolve(self.__cluster[n]) for n in n]
                return

            # update the pipelined dbs
            for db_num in db_nums:
                # map the ident to a db
                if pipelined:
                    pipe_command_map[db_num].append(cmd_ident)

                # add to pending commands
                pending_commands[db_num].append(command)

        # Create the threadpool and pipe jobs into it
        pool = ThreadPool(min(self.__workers, len(pending_commands)))

        # execute our pending commands either in the pool, or using a pipeline
        for db_num, command_list in pending_commands.iteritems():
            if pipelined:
                pipes[db_num] = self.__cluster[db_num].get_pipeline()
            for command in command_list:
                if pipelined:
                    # add to pipeline
                    pipes[db_num].add(command)
                else:
                    # execute in pool
                    pool.add(hash(command), command.clone().resolve, [self.__cluster[db_num]])

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
                for ident, value in izip(pipe_command_map[db], result):
                    if isinstance(value, Exception):
                        self.__errors.append((command_map[ident], value))
                    command_map[ident].resolve_as(value)

        else:
            for command in self.__commands:
                # we explicitly use the hash as the identifier as that is how it was added to the
                # pool originally
                result = result_map[hash(command)]
                for value in result:
                    if isinstance(value, Exception):
                        self.__errors.append((command, value))

                if len(result) == 1:
                    result = result[0]

                change_resolution(command, result)

        if not self.__fail_silently and self.__errors:
            raise CommandError(self.__errors)

        self.__resolved = True

    def get_results(self):
        assert self.__resolved, 'you must execute the commands before fetching results'

        return self.__commands

    def get_errors(self):
        assert self.__resolved, 'you must execute the commands before fetching results'

        return self.__errors


class DistributedContextManager(object):
    def __init__(self, cluster, workers=None, **kwargs):
        self.connection = DistributedConnection(cluster, workers, **kwargs)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_value, tb):
        # we need to break up each command and route it
        self.connection.resolve()


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
