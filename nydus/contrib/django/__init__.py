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

from __future__ import absolute_import

from nydus import conf
from nydus.db.backends import BaseConnection
from nydus.utils import import_string

from django.conf import settings

conf.configure(getattr(settings, 'NYDUS_CONFIG', {}))

class DjangoDatabase(BaseConnection):
    def __init__(self, backend, name, host=None, port=None, test_name=None, 
                       user=None, password=None, options={}, **kwargs):
        """
        Given an alias (which is defined in DATABASES), creates a new connection
        that proxies the original database engine.
        """
        if isinstance(backend, basestring):
            backend = import_string(backend)
        self.backend = backend
        self.settings_dict = {
            'HOST': host,
            'PORT': port,
            'NAME': name,
            'TEST_NAME': test_name,
            'OPTIONS': options,
            'USER': user,
            'PASSWORD': password,
        }
        self.wrapper = __import__('%s.base' % (backend.__name__,), {}, {}, ['DatabaseWrapper']).DatabaseWrapper(self.settings_dict)
        super(DjangoDatabase, self).__init__(**kwargs)

    def connect(self):
        # force django to connect
        self.wrapper.cursor()
        return self.wrapper.connection

    def disconnect(self):
        self.connection.close()

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            # Send to router
            return getattr(self.connection, attr)