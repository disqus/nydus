"""
    Ketama consistent hash algorithm.

    Rewrited from the original source: http://www.audioscrobbler.net/development/ketama/

"""
__author__  = "Andrey Nikishaev"
__email__   = "creotiv@gmail.com"
__version__ = 0.1
__status__  = "productrion"

__all__     = ['Ketama']

import hashlib
import math
from bisect import bisect

class Ketama(object):

    def __init__(self, nodes=None, weights=None):
        """
            nodes   - List of nodes(strings)
            weights - Dictionary of node wheights where keys are nodes names.
                      if not set, all nodes will be equal.
        """
        self._hashring      = dict()
        self._sorted_keys   = []

        self._nodes         = nodes
        self._weights       = weights if weights else {}

        self._build_circle()

    def _build_circle(self):
        """
            Creates hash ring.
        """
        total_weight = 0
        for node in self._nodes:
            total_weight += self._weights.get(node, 1)

        for node in self._nodes:
            weight = self._weights.get(node,1)

            ks = math.floor((40*len(self._nodes) * weight) / total_weight);

            for i in xrange(0, int(ks)):
                b_key = self._md5_digest( '%s-%s-salt' % (node, i) )

                for l in xrange(0, 4):
                    key = (( b_key[3+l*4] << 24 )
                         | ( b_key[2+l*4] << 16 )
                         | ( b_key[1+l*4] << 8  )
                         |   b_key[l*4]         )

                    self._hashring[key] = node
                    self._sorted_keys.append(key)

        self._sorted_keys.sort()

    def _get_node_pos(self, key):
        """
            Return node position(integer) for a given key. Else return None
        """
        if not self._hashring:
            return None

        key = self._gen_key(key)

        nodes = self._sorted_keys
        pos = bisect(nodes, key)

        if pos == len(nodes):
            return 0
        return pos

    def _gen_key(self, key):
        """
            Return long integer for a given key, that represent it place on
            the hash ring.
        """
        b_key = self._md5_digest(key)
        return self._hashi(b_key, lambda x: x)

    def _hashi(self, b_key, fn):
        return (( b_key[fn(3)] << 24 )
              | ( b_key[fn(2)] << 16 )
              | ( b_key[fn(1)] << 8  )
              |   b_key[fn(0)]       )

    def _md5_digest(self, key):
        return map(ord, hashlib.md5(key).digest())

    def remove_node(self,node):
        """
            Removes node from circle and rebuild it.
        """
        try:
            self._nodes.remove(node)
            del self._weights[node]
        except KeyError,e:
            pass
        except ValueError,e:
            pass
        self._hashring    = dict()
        self._sorted_keys = []

        self._build_circle()

    def add_node(self,node,weight=1):
        """
            Adds node to circle and rebuild it.
        """
        self._nodes.append(node)
        self._weights[node] = weight
        self._hashring      = dict()
        self._sorted_keys   = []

        self._build_circle()

    def get_node(self, key):
        """
            Return node for a given key. Else return None.
        """
        pos = self._get_node_pos(key)
        if pos is None:
            return None
        return self._hashring[ self._sorted_keys[pos] ]


if __name__ == '__main__':
    def test(k):
        data = {}
        for i in xrange(REQUESTS):
            tower = k.get_node('a'+str(i))
            data.setdefault(tower,0)
            data[tower] += 1
        print 'Number of caches on each node: '
        print data
        print ''

        print k.get_node('Aplple');
        print k.get_node('Hello');
        print k.get_node('Data');
        print k.get_node('Computer');

    NODES = ['192.168.0.1:6000','192.168.0.1:6001','192.168.0.1:6002',
            '192.168.0.1:6003','192.168.0.1:6004','192.168.0.1:6005',
            '192.168.0.1:6006','192.168.0.1:6008','192.168.0.1:6007'
           ]
    REQUESTS = 1000

    k = Ketama(NODES)

    test(k)

    k.remove_node('192.168.0.1:6007')

    test(k)

    k.add_node('192.168.0.1:6007')

    test(k)
