"""
nydus.contrib.django.backend.base
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

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
