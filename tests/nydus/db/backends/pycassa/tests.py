from __future__ import absolute_import

from nydus.db import create_cluster
from nydus.db.backends.pycassa import Pycassa, PycassaCluster
from nydus.testutils import BaseTest, fixture
import mock


class PycassCreateClusterTest(BaseTest):
    @fixture
    def cluster(self):
        return create_cluster({
            'backend': 'nydus.db.backends.pycassa.Pycassa',
            'hosts': ['localhost'],
            'keyspace': 'test',
        })

    def test_is_pycassa_cluster(self):
        self.assertEquals(type(self.cluster), PycassaCluster)


class PycassClusterTest(BaseTest):
    @fixture
    def cluster(self):
        return PycassaCluster(
            hosts=['localhost'],
            keyspace='test',
        )

    def test_has_one_connection(self):
        self.assertEquals(len(self.cluster), 1)

    def test_backend_is_pycassa(self):
        self.assertEquals(type(self.cluster[0]), Pycassa)


class PycassaTest(BaseTest):
    @fixture
    def connection(self):
        return Pycassa(num=0, keyspace='test', hosts=['localhost'])

    @mock.patch('nydus.db.backends.pycassa.ConnectionPool')
    def test_client_instantiates_with_kwargs(self, ConnectionPool):
        client = Pycassa(
            keyspace='test', hosts=['localhost'], prefill=True,
            timeout=5, foo='bar', num=0,
        )
        client.connect()
        ConnectionPool.assert_called_once_with(
            keyspace='test', prefill=True, timeout=5,
            server_list=['localhost'], foo='bar'
        )

    @mock.patch('nydus.db.backends.pycassa.ConnectionPool')
    def test_disconnect_calls_dispose(self, ConnectionPool):
        self.connection.disconnect()
        ConnectionPool().dispose.assert_called_once_with()
