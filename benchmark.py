from nydus.db import create_cluster
import time

partition_cluster = create_cluster({
    'engine': 'nydus.db.backends.redis.Redis',
    'router': 'nydus.db.routers.keyvalue.PartitionRouter',
    'hosts': {
        0: {'db': 0},
        1: {'db': 1},
        2: {'db': 2},
        3: {'db': 3},
    },
})

ketama_cluster = create_cluster({
    'engine': 'nydus.db.backends.redis.Redis',
    'router': 'nydus.db.routers.keyvalue.ConsistentHashingRouter',
    'hosts': {
        0: {'db': 0},
        1: {'db': 1},
        2: {'db': 2},
        3: {'db': 3},
    },
})

roundrobin_cluster = create_cluster({
    'engine': 'nydus.db.backends.redis.Redis',
    'router': 'nydus.db.routers.RoundRobinRouter',
    'hosts': {
        0: {'db': 0},
        1: {'db': 1},
        2: {'db': 2},
        3: {'db': 3},
    },
})


def test_redis_normal(cluster):
    cluster.set('foo', 'bar')
    cluster.get('foo')
    cluster.set('biz', 'bar')
    cluster.get('biz')
    cluster.get('bar')


def test_redis_map(cluster):
    with cluster.map() as conn:
        for n in range(5):
            conn.set('foo', 'bar')
            conn.get('foo')
            conn.set('biz', 'bar')
            conn.get('biz')
            conn.get('bar')


def main(iterations=1000):
    for cluster in ('partition_cluster', 'ketama_cluster', 'roundrobin_cluster'):
        for func in ('test_redis_normal', 'test_redis_map'):
            print("Running %r on %r" % (func, cluster))
            s = time.time()
            for x in range(iterations):
                globals()[func](globals()[cluster])
            t = (time.time() - s) * 1000
            print("  %.3fms per iteration" % (t / iterations,))


if __name__ == '__main__':
    main()
