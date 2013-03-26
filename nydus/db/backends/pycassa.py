"""
nydus.db.backends.pycassa
~~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

import collections
from nydus.db.base import BaseCluster
from nydus.db.backends import BaseConnection
from pycassa import ConnectionPool


class Pycassa(BaseConnection):
    def __init__(self, num, keyspace, hosts=['localhost'], **options):
        self.keyspace = keyspace
        self.hosts = hosts
        self.options = options
        super(Pycassa, self).__init__(num)

    @property
    def identifier(self):
        return "pycassa://%(hosts)s/%(keyspace)s" % {
            'hosts': ','.join(self.hosts),
            'keyspace': self.keyspace,
        }

    def connect(self):
        return ConnectionPool(keyspace=self.keyspace, server_list=self.hosts, **self.options)

    def disconnect(self):
        self.connection.dispose()

    @classmethod
    def get_cluster(cls):
        return PycassaCluster


class PycassaCluster(BaseCluster):
    """
    A PycassaCluster has a single host as pycassa internally handles routing
    and communication within a set of nodes.
    """
    def __init__(self, hosts=None, keyspace=None, backend=Pycassa, **kwargs):
        assert isinstance(hosts, collections.Iterable), 'hosts must be an iterable'
        assert keyspace, 'keyspace must be set'

        return super(PycassaCluster, self).__init__(
            hosts={
                0: {
                    'hosts': hosts,
                    'keyspace': keyspace,
                },
            },
            backend=backend,
            **kwargs
        )
