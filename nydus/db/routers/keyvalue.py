"""
nydus.db.routers.keyvalue
~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from binascii import crc32

from nydus.contrib.ketama import Ketama
from nydus.db.routers import BaseRouter, RoundRobinRouter

__all__ = ('ConsistentHashingRouter', 'PartitionRouter')


class ConsistentHashingRouter(RoundRobinRouter):
    '''
    Router that returns host number based on a consistent hashing algorithm.
    The consistent hashing algorithm only works if a key argument is provided.
    If a key is not provided, then all hosts are returned.
    '''

    def __init__(self, *args, **kwargs):
        self._db_num_id_map = {}
        super(ConsistentHashingRouter, self).__init__(*args, **kwargs)

    def flush_down_connections(self):
        for db_num in self._down_connections:
            self._hash.add_node(self._db_num_id_map[db_num])

        super(ConsistentHashingRouter, self).flush_down_connections()

    def mark_connection_down(self, db_num):
        db_num = self.ensure_db_num(db_num)
        self._hash.remove_node(self._db_num_id_map[db_num])

        super(ConsistentHashingRouter, self).mark_connection_down(db_num)

    def mark_conenction_up(self, db_num):
        db_num = self.ensure_db_num(db_num)
        self._hash.add_node(self._db_num_id_map[db_num])

        super(ConsistentHashingRouter, self).mark_connection_up(db_num)

    def _setup_router(self, cluster, *args, **kwargs):
        self._db_num_id_map = dict([(db_num, host.identifier) for db_num, host in cluster.hosts.iteritems()])
        self._hash = Ketama(self._db_num_id_map.values())

        return True

    def _route(self, cluster, attr, key, *args, **kwargs):
        found = self._hash.get_node(key)

        if not found and len(self._down_connections) > 0:
            raise self.HostListExhausted()

        return [i for i, h in cluster.hosts.iteritems()
                if h.identifier == found]


class PartitionRouter(BaseRouter):
    def _route(self, cluster, attr, key, *args, **kwargs):
        return [crc32(str(key)) % len(cluster)]

