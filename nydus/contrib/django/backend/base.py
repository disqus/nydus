from __future__ import absolute_import

from django.db import DEFAULT_DB_ALIAS
from django.core.exceptions import ImproperlyConfigured

from nydus.db import connections

class BackendConnectionProxy(object):
    """
    Proxies a ``BaseConnection`` with an additional ``alias``
    attribute.
    """
    def __init__(self, backend, alias):
        self.backend = backend
        self.alias = alias
    
    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.backend, attr)

class DatabaseWrapper(object):
    def __init__(self, settings_dict, alias=DEFAULT_DB_ALIAS):
        self.settings_dict = settings_dict
        try:
            nydus_alias = settings_dict['NAME']
            self.backend = BackendConnectionProxy(connections[nydus_alias], alias)
        except KeyError, e:
            # TODO: give a better exception
            raise ImproperlyConfigured(e)

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        return getattr(self.backend, name)
