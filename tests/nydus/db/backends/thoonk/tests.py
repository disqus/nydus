from __future__ import absolute_import

from nydus.db.backends.thoonk import Thoonk
from nydus.db.base import create_cluster
import unittest2
from uuid import uuid4


class TestThoonkPubsub(unittest2.TestCase):
    """docstring for ThoonkPubsub"""
    def get_rand_name(self, prefix=None, suffix=None):
        return prefix if prefix else "" + uuid4.hex + suffix if suffix else ""

    def get_cluster(self, router):
        cluster = create_cluster({
            'engine': 'nydus.db.backends.thoonk.Thoonk',
            'router': router,
            'hosts': {
                0: {'db': 5},
                1: {'db': 6},
                2: {'db': 7},
                3: {'db': 8},
                4: {'db': 9},
            }
        })
        self.flush_custer(cluster)
        return cluster

    def flush_custer(self, cluster):
        for x in range(len(cluster)):
            if isinstance(cluster.get_conn(), list):
                c = cluster.get_conn()[x]
            else:
                c = cluster.get_conn()
            c.redis.flushdb()

    def setUp(self):
        self.ps = Thoonk(num=0, db_num=1)
        self.redis = self.ps.redis
        self.redis.flushdb()

    def tearDown(self):
        pass

    def test_flush_db(self):
        pubsub = self.get_cluster('nydus.db.routers.redis.ConsistentHashingRouter')
        pubsub.flushdb()

    def test_job_with_ConsistentHashingRouter(self):
        pubsub = self.get_cluster('nydus.db.routers.redis.ConsistentHashingRouter')
        job = pubsub.job("test1")
        jid = job.put("10")

        jid_found = False

        for ps in pubsub.get_conn():
            jps = ps.job('test1')
            if jid in jps.get_ids():
                self.assertFalse(jid_found)
                jid_found = True

        self.assertTrue(jid_found)

    def test_job_with_RoundRobinRouter(self):
        pubsub = self.get_cluster('nydus.db.routers.RoundRobinRouter')

        jobs = {}
        size = 20

        # put jobs onto the queue
        for x in xrange(0, size):
            jps = pubsub.job('testjob')
            jid = jps.put(str(x))
            if id(jps) not in jobs:
                jobs[id(jps)] = []
            jobs[id(jps)].append(jid)

        # make sure that we are reusing the job items
        self.assertEqual(len(jobs), 5)
        for k, v in jobs.iteritems():
            self.assertEqual(len(v), size / 5)

        # make sure we fishi
        for x in xrange(0, size):
            jps = pubsub.job('testjob')
            jid, job, cancel_count = jps.get()
            jps.finish(jid)

        self.assertEqual(len(jobs), 5)

        for x in range(len(pubsub)):
            ps = pubsub.get_conn('testjob')
            jps = ps.job('testjob')
            self.assertEqual(jps.get_ids(), [])


test_suite = TestThoonkPubsub
