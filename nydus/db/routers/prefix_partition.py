from nydus.db.routers import BaseRouter

class PrefixPartitionRouter(BaseRouter):
    '''
    Routes based on the configured prefixes
    
    Example config:
    
    'redis': {
        'engine': 'nydus.db.backends.redis.Redis',
        'router': 'nydus.db.routers.redis.PrefixPartitionRouter',
        'hosts': {
            'default': {'db': 0, 'host': 'default.redis.goteam.be', 'port': 6379},
            'user:loves:': {'db': 1, 'host': 'default.redis.goteam.be', 'port': 6379},
            'loves:': {'db': 2, 'host': 'default.redis.goteam.be', 'port': 6379},
            'hash:entity:': {'db': 0, 'host': 'entities.redis.goteam.be', 'port': 6379},
        }
    }
    
    We route to one and only one redis.
    Use a seperate config if you want hashing based partitioning.
    '''
    
    def _pre_routing(self, cluster, attr, key, *args, **kwargs):
        """
        Requesting a pipeline without a key to partition on is just plain wrong.
        We raise a valueError if you try
        """
        if not key and attr == 'pipeline':
            raise ValueError('Pipelines requires a key for proper routing')
        return key
    
    def _route(self, cluster, attr, key, *args, **kwargs):
        """
        Perform routing and return db_nums
        """
        if 'default' not in cluster.hosts:
            error_message = 'The prefix router requires a default host'
            raise ValueError(error_message)
        hosts = None
        if key:
            for host in cluster.hosts:
                if key.startswith(str(host)):
                    hosts = [host]
            if not hosts:
                hosts = ['default']
            
        #sanity check, dont see how this can happen
        if not hosts:
            error_message = 'The prefix partition router couldnt find a host for command %s and key %s' % (attr, key)
            raise ValueError(error_message)
        
        return hosts
        
