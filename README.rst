Nydus
=====

Generic database utilities, including connection clustering and routing so you can scale like a pro.

The following example creates a Redis connection cluster which will distribute reads and writes based on a simple modulus lookup of the hashed key::

    >>> from nydus.db import create_cluster
    >>>
    >>> redis = create_cluster({
    >>>     'engine': 'nydus.db.backends.redis.Redis',
    >>>     'router': 'nydus.db.routers.redis.PartitionRouter',
    >>>     'hosts': {
    >>>         0: {'db': 0},
    >>>         1: {'db': 1},
    >>>         2: {'db': 2},
    >>>     }
    >>> })
    >>>
    >>> res = redis.incr('foo')
    >>>
    >>> assert res == 1


nydus.db.create_cluster
-----------------------

The create_cluster function is a simple helper to configure a ``Cluster`` based on a simple dict config.

There are two required keyword arguments:

* ``engine``: full path to the backend class, which should extend ``nydus.db.backends.BaseConnection``.

* ``hosts``: a dictionary, where the key is an ordered numeric value, and the result is a dict of connection options.

  (the keys are numeric values simply for readability in configuration)

Optionally, you may also specify a value for ``router``, which is the full path to the router class,
which must extend ``nydus.db.routers.BaseRouter``.

Distributed Queries
-------------------

In some cases you may want to execute a query on many nodes (in parallel). Nydus has built-in support for this when any routing function
returns a cluster of nodes::

    >>> from nydus.db import create_cluster
    >>>
    >>> # by not specifying a router all queries are executed on all hosts
    >>> redis = create_cluster({
    >>>     'engine': 'nydus.db.backends.redis.Redis',
    >>>     'hosts': {
    >>>         0: {'db': 0},
    >>>         1: {'db': 1},
    >>>         2: {'db': 2},
    >>>     }
    >>> })
    >>>
    >>> # map the query over all connections returned by the default router
    >>> res = redis.incr('foo')
    >>>
    >>> assert type(res) == list
    >>> assert len(res) == 3

You can also map many queries (utilizing an internal queue) over connections (again, returned by the router)::

    >>> with redis.map() as conn:
    >>>     results = [conn.incr(k) for k in keys]

As of release 0.5.0, the map() function now supports pipelines, and the included Redis backend will pipeline commands
wherever possible.

Redis
-----

Nydus was originally designed as a toolkit to expand on the usage of Redis at DISQUS. While it does provide
a framework for building clusters that are not Redis, much of the support has gone into providing utilities
for routing and querying on Redis clusters.

You can configure the Redis client for a connection by specifying it's full path::

    >>> redis = create_cluster({
    >>>     'engine': 'nydus.db.backends.redis.Redis',
    >>>     'hosts': {
    >>>         0: {'db': 0},
    >>>     },
    >>> )}

The available host options are:

* host
* port
* db
* timeout

The Redis client also supports pipelines via the map command. This means that all commands will hit servers at most
as of once::

    >>> with redis.map() as conn:
    >>>     conn.set('a', 1)
    >>>     conn.incr('a')
    >>>     results = [conn.get('a'), conn.get('b')]
    >>> results['a'] == 2
    >>> results['b'] == None

Simple Partition Router
~~~~~~~~~~~~~~~~~~~~~~~

There are also several options for builtin routing. The easiest is a simple partition router, which is just a simple
hash on the key::

    >>> redis = create_cluster({
    >>>     'engine': 'nydus.db.backends.redis.Redis',
    >>>     'router': 'nydus.db.routers.redis.PartitionRouter',
    >>>     'hosts': {
    >>>         0: {'db': 0},
    >>>     },
    >>> )}

Consistent Hashing Router
~~~~~~~~~~~~~~~~~~~~~~~~~

An improvement upon hashing, Nydus provides a Ketama-based consistent hashing router::

    >>> redis = create_cluster({
    >>>     'engine': 'nydus.db.backends.redis.Redis',
    >>>     'router': 'nydus.db.routers.redis.ConsistentHashingRouter',
    >>>     'hosts': {
    >>>         0: {'db': 0},
    >>>     },
    >>> )}

Round Robin Router
~~~~~~~~~~~~~~~~~~

An additional option for distributing queries is the round robin router::

    >>> redis = create_cluster({
    >>>     'engine': 'nydus.db.backends.redis.Redis',
    >>>     'router': 'nydus.db.routers.redis.RoundRobinRouter',
    >>>     'hosts': {
    >>>         0: {'db': 0},
    >>>     },
    >>> )}
