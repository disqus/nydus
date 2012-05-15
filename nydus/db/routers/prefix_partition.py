from nydus.db.routers import BaseRouter

class PrefixPartitionRouter(BaseRouter):
    '''
    Routes based on the configured prefixes
    
    Example config:
    
    'redis': {
        'engine': 'nydus.db.backends.redis.Redis',
        'router': 'django_redis.nydus_router.PrefixPartitionRouter',
        'hosts': {
            0: {'db': 0, 'host': 'default.redis.goteam.be', 'port': 6379},
            'user:loves:': {'db': 1, 'host': 'default.redis.goteam.be', 'port': 6379},
            'loves:': {'db': 2, 'host': 'default.redis.goteam.be', 'port': 6379},
            'hash:entity:': {'db': 0, 'host': 'entities.redis.goteam.be', 'port': 6379},
        }
    }
    
    We route to one and only one redis.
    Use a seperate config if you want hashing based partitioning.
    '''
    
    def _route(self, cluster, attr, key, *args, **kwargs):
        """
        Perform routing and return db_nums
        """
        assert 'default' in cluster.hosts, 'The prefix router requires a default key to route to'
        hosts = None
        if key:
            for host in cluster.hosts:
                if key.startswith(str(host)):
                    hosts = [host]
            if not hosts:
                hosts = ['default']
        elif func == 'pipeline':
            raise ValueError('Pipelines requires a key for proper routing')
        
        if not hosts:
            raise ValueError, 'I didnt expect this while writing the code so lets fail'
        
        return hosts
        
