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

from django.db import DEFAULT_DB_ALIAS
from django.core.exceptions import ImproperlyConfigured

from nydus.db import connections

class DatabaseWrapper(object):
    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS):
        self.settings_dict = settings_dict
        self.alias = alias
        try:
            nydus_alias = settings_dict['NAME']
            self.backend = connections[nydus_alias]
        except KeyError, e:
            # TODO: give a better exception
            raise ImproperlyConfigured(e)

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        return getattr(self.backend, name)
