"""
nydus
~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

try:
    VERSION = __import__('pkg_resources') \
        .get_distribution('nydus').version
except Exception, e:
    VERSION = 'unknown'

#Just make sure we don't clash with the source project
VERSION = '10.0.1'