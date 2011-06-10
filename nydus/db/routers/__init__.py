class BaseRouter(object):
    """
    Handles routing requests to a specific connection in a single cluster.
    """
    # def __init__(self):
    #     pass

    def get_db(self, pool, func, *args, **kwargs):
        """Return the first entry in the pool"""
        return range(len(pool))

