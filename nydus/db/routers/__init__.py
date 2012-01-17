"""
nydus.db.routers
~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

class BaseRouter(object):
    """
    Handles routing requests to a specific connection in a single cluster.
    """
    # def __init__(self):
    #     pass

    retryable = False

    def get_db(self, cluster, func, *args, **kwargs):
        """Return the first entry in the cluster"""
        return range(len(cluster))

