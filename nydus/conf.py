"""
nydus.conf
~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

try:
    VERSION = __import__('pkg_resources') \
        .get_distribution('nydus').version
except Exception, e:
    VERSION = 'unknown'

"""
Represents the default values for all Nydus settings.
"""

"""
Configuring Django's database connections, in your ``settings.py`` you would
do the following::

    DATABASES = {
        'default': {
            'ENGINE': 'nydus.contrib.django.backend',
            'NAME': 'django/default',
        },
    }

    NYDUS_CONFIG = {
        'CONNECTIONS': {
            'django/default': {
                'engine': 'nydus.contrib.django.DjangoDatabase',
                'hosts': {
                    0: {'host': 'localhost', 'backend': 'django.db.backends.sqlite3'},
                    1: {'host': 'localhost', 'backend': 'django.db.backends.sqlite3'},
                    2: {'host': 'localhost', 'backend': 'django.db.backends.sqlite3'},
                },
            },
        },
    }
"""

import warnings


CONNECTIONS = {
    # 'django/default': {
    #     'engine': 'nydus.contrib.django.backend.DjangoDatabase',
    #     'hosts': {
    #         0: {'host': 'localhost', 'backend': 'django.db.backends.sqlite3'},
    #         1: {'host': 'localhost', 'backend': 'django.db.backends.sqlite3'},
    #         2: {'host': 'localhost', 'backend': 'django.db.backends.sqlite3'},
    #     },
    # }
    # 'redis': {
    #     'engine': 'nydus.db.backends.redis.Redis',
    #     'hosts': {
    #         0: {},
    #         1: {},
    #         2: {},
    #     },
    # }
}


def configure(kwargs):
    for k, v in kwargs.iteritems():
        if k.upper() != k:
            warnings.warn('Invalid setting, \'%s\' which is not defined by Nydus' % k)
        elif k not in globals():
            warnings.warn('Invalid setting, \'%s\' which is not defined by Nydus' % k)
        else:
            globals()[k] = v
