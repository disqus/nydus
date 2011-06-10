from binascii import crc32

from nydus.db.routers import BaseRouter

class RedisRouter(BaseRouter):
    def get_db(self, pool, func, key=None, *args, **kwargs):
        # Assume first argument is a key
        if not key:
           return range(len(pool))
        return [crc32(str(key)) % len(pool)]
