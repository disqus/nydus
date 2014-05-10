"""
nydus.db.backends.riak
~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

import socket
import httplib

from riak import RiakClient, RiakError

from nydus.db.backends import BaseConnection


class Riak(BaseConnection):
    # Exceptions that can be retried by this backend
    retryable_exceptions = frozenset([socket.error, httplib.HTTPException, RiakError])
    supports_pipelines = False

    def __init__(self, num, host='127.0.0.1', port=8098, prefix='riak', mapred_prefix='mapred', client_id=None,
        transport_class=None, solr_transport_class=None, transport_options=None, **options):

        self.host = host
        self.port = port
        self.prefix = prefix
        self.mapred_prefix = mapred_prefix
        self.client_id = client_id
        self.transport_class = transport_class
        self.solr_transport_class = solr_transport_class
        self.transport_options = transport_options or {}
        super(Riak, self).__init__(num)

    @property
    def identifier(self):
        mapping = vars(self)
        return "http://%(host)s:%(port)s/%(prefix)s" % mapping

    def connect(self):
        return RiakClient(
            host=self.host, port=self.port, prefix=self.prefix,
            mapred_prefix=self.mapred_prefix, client_id=self.client_id,
            transport_class=self.transport_class, solr_transport_class=self.solr_transport_class,
            transport_options=self.transport_options)

    def disconnect(self):
        pass
