from __future__ import absolute_import

import mock
from inspect import getargspec
from riak import RiakClient, RiakError

from tests import BaseTest

from nydus.db.backends.riak import Riak
from nydus.db.base import Cluster, create_cluster


class RiakTest(BaseTest):
    def setUp(self):
        self.expected = {
            'host': '127.0.0.1',
            'port': 8098,
            'prefix': 'riak',
            'mapred_prefix': 'mapred',
            'client_id': None,
        }
        
        self.conn = Riak(num=0)

    def test_init(self):
        args, _, _, defaults = getargspec(Riak.__init__)
        args = [arg for arg in args if arg != 'self']

        self.assertItemsEqual(args, self.expected.keys())
        self.assertItemsEqual(defaults, self.expected.values())

        self.assertDictContainsSubset(self.expected, self.conn.__dict__)

    def test_identifier(self):
        self.assertEquals('http://127.0.0.1:8098/riak', self.conn.identifier)

    @mock.patch('nydus.db.backends.riak.RiakClient')
    def test_connect_riakclient_options(self, _RiakClient):
        self.conn.connect()

        _RiakClient.assert_called_with(**self.expected)

    def test_connect_returns_riakclient(self):
        client = self.conn.connect()

        self.assertIsInstance(client, RiakClient)
        
    def test_provides_retryable_exceptions(self):
        self.assertIn(RiakError, self.conn.retryable_exceptions)

