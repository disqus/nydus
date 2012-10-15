"""
nydus.db.exceptions
~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2011-2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""


class CommandError(Exception):
    def __init__(self, errors):
        self.errors = errors

    def __repr__(self):
        return '<%s (%d): %r>' % (type(self), len(self.errors), self.errors)

    def __str__(self):
        return '%d command(s) failed: %r' % (len(self.errors), self.errors)
