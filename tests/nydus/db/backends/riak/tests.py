from __future__ import absolute_import

import mock
from httplib import HTTPException
from nydus.db.backends.riak import Riak
from nydus.testutils import BaseTest
from riak import RiakClient, RiakError
from socket import error as SocketError


class RiakTest(BaseTest):
    def setUp(self):
        self.expected_defaults = {
            'host': '127.0.0.1',
            'port': 8098,
            'prefix': 'riak',
            'mapred_prefix': 'mapred',
            'client_id': None,
        }

        self.modified_props = {
            'host': '127.0.0.254',
            'port': 8908,
            'prefix': 'kair',
            'mapred_prefix': 'derpam',
            'client_id': 'MjgxMDg2MzQx',
            'transport_options': {},
            'transport_class': mock.Mock,
            'solr_transport_class': mock.Mock,
        }

        self.conn = Riak(0)
        self.modified_conn = Riak(1, **self.modified_props)

    def test_init_defaults(self):
        self.assertDictContainsSubset(self.expected_defaults, self.conn.__dict__)

    def test_init_properties(self):
        self.assertDictContainsSubset(self.modified_props, self.modified_conn.__dict__)

    def test_identifier(self):
        expected_identifier = 'http://%(host)s:%(port)s/%(prefix)s' % self.conn.__dict__
        self.assertEquals(expected_identifier, self.conn.identifier)

    def test_identifier_properties(self):
        expected_identifier = 'http://%(host)s:%(port)s/%(prefix)s' % self.modified_props
        self.assertEquals(expected_identifier, self.modified_conn.identifier)

    @mock.patch('nydus.db.backends.riak.RiakClient')
    def test_connect_riakclient_options(self, _RiakClient):
        self.conn.connect()

        _RiakClient.assert_called_with(host=self.conn.host, port=self.conn.port, prefix=self.conn.prefix, \
                                        mapred_prefix=self.conn.mapred_prefix, client_id=self.conn.client_id, \
                                        transport_options=self.conn.transport_options, transport_class=self.conn.transport_class, \
                                        solr_transport_class=self.conn.solr_transport_class)

    @mock.patch('nydus.db.backends.riak.RiakClient')
    def test_connect_riakclient_modified_options(self, _RiakClient):
        self.modified_conn.connect()

        _RiakClient.assert_called_with(host=self.modified_conn.host, port=self.modified_conn.port, prefix=self.modified_conn.prefix, \
                                        mapred_prefix=self.modified_conn.mapred_prefix, client_id=self.modified_conn.client_id, \
                                        transport_options=self.modified_conn.transport_options, transport_class=self.modified_conn.transport_class, \
                                        solr_transport_class=self.modified_conn.solr_transport_class)

    def test_connect_returns_riakclient(self):
        client = self.conn.connect()

        self.assertIsInstance(client, RiakClient)

    def test_provides_retryable_exceptions(self):
        self.assertItemsEqual([RiakError, HTTPException, SocketError], self.conn.retryable_exceptions)
