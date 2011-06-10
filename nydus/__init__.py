"""
Nydus
~~~~~
"""

try:
    VERSION = __import__('pkg_resources') \
        .get_distribution('nydus').version
except Exception, e:
    VERSION = 'unknown'