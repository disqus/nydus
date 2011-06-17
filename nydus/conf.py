# Copyright 2011 DISQUS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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