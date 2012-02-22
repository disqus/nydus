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

You can also map many queries (utilizing an internal queue) over connections (again, returned by the router):

    >>> with redis.map() as conn:
    >>>     results = [conn.incr(k) for k in keys]

As of release 0.5.0, the map() function now supports pipelines, and the included Redis backend will pipeline commands
wherever possible.