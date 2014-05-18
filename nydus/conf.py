"""
nydus.conf
~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

import warnings
import six

CONNECTIONS = {}


def configure(kwargs):
    for k, v in six.iteritems(kwargs):
        if k.upper() != k:
            warnings.warn('Invalid setting, \'%s\' which is not defined by Nydus' % k)
        elif k not in globals():
            warnings.warn('Invalid setting, \'%s\' which is not defined by Nydus' % k)
        else:
            globals()[k] = v
