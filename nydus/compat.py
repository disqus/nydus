import sys

try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # noqa


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3


if PY3:
    string_types = str,
else:
    string_types = basestring,


try:
    # Python 2
    from itertools import izip
except ImportError:
    # Python 3
    izip = zip


if PY3:
    def iteritems(d, **kw):
        return iter(d.items(**kw))
else:
    def iteritems(d, **kw):
        return iter(d.iteritems(**kw))
