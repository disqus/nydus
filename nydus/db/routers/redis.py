"""
nydus.db.routers.redis
~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from binascii import crc32

from nydus.db.routers import BaseRouter

class PartitionRouter(BaseRouter):
    def get_db(self, cluster, func, key=None, *args, **kwargs):
        # Assume first argument is a key
        if not key:
           return range(len(cluster))
        return [crc32(str(key)) % len(cluster)]
