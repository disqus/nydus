"""
nydus.db.routers.keyvalue
~~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from binascii import crc32

from nydus.contrib.ketama import Ketama
from nydus.db.routers import BaseRouter, RoundRobinRouter, routing_params

__all__ = ('ConsistentHashingRouter', 'PartitionRouter')


def get_key(args, kwargs):
    if 'key' in kwargs:
        return kwargs['key']
    elif args:
        return args[0]
    return None


class ConsistentHashingRouter(RoundRobinRouter):
    """
    Router that returns host number based on a consistent hashing algorithm.
    The consistent hashing algorithm only works if a key argument is provided.

    If a key is not provided, then all hosts are returned.

    The first argument is assumed to be the ``key`` for routing. Keyword arguments
    are not supported.
    """

    def __init__(self, *args, **kwargs):
        self._db_num_id_map = {}
        super(ConsistentHashingRouter, self).__init__(*args, **kwargs)

    def mark_connection_down(self, db_num):
        db_num = self.ensure_db_num(db_num)
        self._hash.remove_node(self._db_num_id_map[db_num])

        super(ConsistentHashingRouter, self).mark_connection_down(db_num)

    def mark_connection_up(self, db_num):
        db_num = self.ensure_db_num(db_num)
        self._hash.add_node(self._db_num_id_map[db_num])

        super(ConsistentHashingRouter, self).mark_connection_up(db_num)

    @routing_params
    def _setup_router(self, args, kwargs, **fkwargs):
        self._db_num_id_map = dict([(db_num, host.identifier) for db_num, host in self.cluster.hosts.iteritems()])
        self._hash = Ketama(self._db_num_id_map.values())

        return True

    @routing_params
    def _pre_routing(self, *args, **kwargs):
        self.check_down_connections()

        return super(ConsistentHashingRouter, self)._pre_routing(*args, **kwargs)

    @routing_params
    def _route(self, attr, args, kwargs, **fkwargs):
        """
        The first argument is assumed to be the ``key`` for routing.
        """

        key = get_key(args, kwargs)

        found = self._hash.get_node(key)

        if not found and len(self._down_connections) > 0:
            raise self.HostListExhausted()

        return [i for i, h in self.cluster.hosts.iteritems()
                if h.identifier == found]


class PartitionRouter(BaseRouter):
    @routing_params
    def _route(self, attr, args, kwargs, **fkwargs):
        """
        The first argument is assumed to be the ``key`` for routing.
        """
        key = get_key(args, kwargs)

        return [crc32(str(key)) % len(self.cluster)]
