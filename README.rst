Nydus
=====

Generic database utilities, including connection clustering and routing so you can scale like a pro.

The following example creates a Redis connection cluster which will distribute reads and writes based on a simple modulus lookup of the hashed key::

    >>> from nydus.db import create_cluster
    >>> redis = create_cluster({
    >>>     'engine': 'nydus.db.backends.redis.Redis',
    >>>     'router': 'nydus.db.routers.redis.PartitionRouter',
    >>>     'hosts': {
    >>>         0: {'db': 0},
    >>>         1: {'db': 1},
    >>>         2: {'db': 2},
    >>>     }
    >>> })
    >>> res = redis.incr('foo')
    >>> assert res == 1