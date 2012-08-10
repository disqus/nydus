"""
nydus.db.backends.memcache
~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

import pylibmc

from nydus.db.backends import BaseConnection

class Memcache(BaseConnection):

    retryable_exceptions = frozenset([pylibmc.Error])
    supports_pipelines = False

    def __init__(self, host='localhost', port=11211, binary=True,
            behaviors=None, **options):
        self.host = host
        self.port = port
        self.binary = binary
        self.behaviors = behaviors
        super(Memcache, self).__init__(**options)

    @property
    def identifier(self):
        mapping = vars(self)
        return "memcache://%(host)s:%(port)s/" % mapping

    def connect(self):
        host = "%s:%i" % (self.host, self.port)
        return pylibmc.Client([host], binary=self.binary, behaviors=self.behaviors)

    def disconnect(self):
        self.connection.disconnect_all()
