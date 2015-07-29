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
    string_types = basestring,  # noqa


try:
    # Python 2
    from itertools import izip
except ImportError:
    # Python 3
    izip = zip


if PY3:
    def iteritems(d, **kw):
        return iter(d.items(**kw))

    def itervalues(d, **kw):
        return iter(d.values(**kw))

    def iterkeys(d, **kw):
        return iter(d.keys(**kw))
else:
    def iteritems(d, **kw):
        return iter(d.iteritems(**kw))

    def iterkeys(d, **kw):
        return iter(d.iterkeys(**kw))

    def itervalues(d, **kw):
        return iter(d.itervalues(**kw))


try:
    xrange = xrange
except NameError:
    xrange = range


def python_2_unicode_compatible(klass):
    """
    A decorator that defines __unicode__ and __str__ methods under Python 2.
    Under Python 3 it does nothing.
    To support Python 2 and 3 with a single code base, define a __str__ method
    returning text and apply this decorator to the class.
    """
    if PY2:
        klass.__unicode__ = klass.__str__
        klass.__str__ = lambda self: self.__unicode__().encode('utf-8')
    return klass
