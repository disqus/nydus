"""
nydus.db.promise
~~~~~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from nydus.db.exceptions import CommandError
from functools import wraps


def promise_method(func):
    """
    A decorator which ensures that once a method has been marked as resolved
    (via Class.__resolved)) will then propagate the attribute (function) call
    upstream.
    """
    name = func.__name__

    @wraps(func)
    def wrapped(self, *args, **kwargs):
        cls_name = type(self).__name__
        if getattr(self, '_%s__resolved' % (cls_name,)):
            return getattr(getattr(self, '_%s__wrapped' % (cls_name,)), name)(*args, **kwargs)
        return func(self, *args, **kwargs)
    return wrapped


def change_resolution(command, value):
    """
    Public API to change the resolution of an already resolved EventualCommand result value.
    """
    command._EventualCommand__wrapped = value
    command._EventualCommand__resolved = True


class EventualCommand(object):
    # introspection support:
    __members__ = property(lambda self: self.__dir__())

    def __init__(self, attr, args=None, kwargs=None):
        self.__attr = attr
        self.__called = False
        self.__wrapped = None
        self.__resolved = False
        self.__args = args or []
        self.__kwargs = kwargs or {}
        self.__ident = ':'.join(map(lambda x: str(hash(str(x))), [self.__attr, self.__args, self.__kwargs]))

    def __call__(self, *args, **kwargs):
        self.__called = True
        self.__args = args
        self.__kwargs = kwargs
        self.__ident = ':'.join(map(lambda x: str(hash(str(x))), [self.__attr, self.__args, self.__kwargs]))
        return self

    def __hash__(self):
        # We return our ident
        return hash(self.__ident)

    def __repr__(self):
        if self.__resolved:
            return repr(self.__wrapped)
        return u'<EventualCommand: %s args=%s kwargs=%s>' % (self.__attr, self.__args, self.__kwargs)

    def __str__(self):
        if self.__resolved:
            return str(self.__wrapped)
        return repr(self)

    def __unicode__(self):
        if self.__resolved:
            return unicode(self.__wrapped)
        return unicode(repr(self))

    def __getattr__(self, name):
        return getattr(self.__wrapped, name)

    def __setattr__(self, name, value):
        if name.startswith('_%s' % (type(self).__name__,)):
            return object.__setattr__(self, name, value)
        return setattr(self.__wrapped, name, value)

    def __delattr__(self, name):
        if name.startswith('_%s' % (type(self).__name__,)):
            raise TypeError("can't delete %s." % name)
        delattr(self.__wrapped, name)

    def __deepcopy__(self, memo):
        from copy import deepcopy
        return deepcopy(self.__wrapped, memo)

    # Need to pretend to be the wrapped class, for the sake of objects that care
    # about this (especially in equality tests)
    def __get_class(self):
        return self.__wrapped.__class__
    __class__ = property(__get_class)

    def __dict__(self):
        try:
            return vars(self.__wrapped)
        except RuntimeError:
            return AttributeError('__dict__')
    __dict__ = property(__dict__)

    def __setitem__(self, key, value):
        self.__wrapped[key] = value

    def __delitem__(self, key):
        del self.__wrapped[key]

    def __setslice__(self, i, j, seq):
        self.__wrapped[i:j] = seq

    def __delslice__(self, i, j):
        del self.__wrapped[i:j]

    def __instancecheck__(self, cls):
        if self._wrapped is None:
            return False
        return isinstance(self._wrapped, cls)

    __lt__ = lambda x, o: x.__wrapped < o
    __le__ = lambda x, o: x.__wrapped <= o
    __eq__ = lambda x, o: x.__wrapped == o
    __ne__ = lambda x, o: x.__wrapped != o
    __gt__ = lambda x, o: x.__wrapped > o
    __ge__ = lambda x, o: x.__wrapped >= o
    __cmp__ = lambda x, o: cmp(x.__wrapped, o)
    # attributes are currently not callable
    # __call__ = lambda x, *a, **kw: x.__wrapped(*a, **kw)
    __nonzero__ = lambda x: bool(x.__wrapped)
    __len__ = lambda x: len(x.__wrapped)
    __getitem__ = lambda x, i: x.__wrapped[i]
    __iter__ = lambda x: iter(x.__wrapped)
    __contains__ = lambda x, i: i in x.__wrapped
    __getslice__ = lambda x, i, j: x.__wrapped[i:j]
    __add__ = lambda x, o: x.__wrapped + o
    __sub__ = lambda x, o: x.__wrapped - o
    __mul__ = lambda x, o: x.__wrapped * o
    __floordiv__ = lambda x, o: x.__wrapped // o
    __mod__ = lambda x, o: x.__wrapped % o
    __divmod__ = lambda x, o: x.__wrapped.__divmod__(o)
    __pow__ = lambda x, o: x.__wrapped ** o
    __lshift__ = lambda x, o: x.__wrapped << o
    __rshift__ = lambda x, o: x.__wrapped >> o
    __and__ = lambda x, o: x.__wrapped & o
    __xor__ = lambda x, o: x.__wrapped ^ o
    __or__ = lambda x, o: x.__wrapped | o
    __div__ = lambda x, o: x.__wrapped.__div__(o)
    __truediv__ = lambda x, o: x.__wrapped.__truediv__(o)
    __neg__ = lambda x: -(x.__wrapped)
    __pos__ = lambda x: +(x.__wrapped)
    __abs__ = lambda x: abs(x.__wrapped)
    __invert__ = lambda x: ~(x.__wrapped)
    __complex__ = lambda x: complex(x.__wrapped)
    __int__ = lambda x: int(x.__wrapped)
    __long__ = lambda x: long(x.__wrapped)
    __float__ = lambda x: float(x.__wrapped)
    __oct__ = lambda x: oct(x.__wrapped)
    __hex__ = lambda x: hex(x.__wrapped)
    __index__ = lambda x: x.__wrapped.__index__()
    __coerce__ = lambda x, o: x.__coerce__(x, o)
    __enter__ = lambda x: x.__enter__()
    __exit__ = lambda x, *a, **kw: x.__exit__(*a, **kw)

    @property
    def is_error(self):
        return isinstance(self.__wrapped, CommandError)

    @promise_method
    def was_called(self):
        return self.__called

    @promise_method
    def resolve(self, conn):
        value = getattr(conn, self.__attr)(*self.__args, **self.__kwargs)
        return self.resolve_as(value)

    @promise_method
    def resolve_as(self, value):
        self.__wrapped = value
        self.__resolved = True
        return value

    @promise_method
    def get_command(self):
        return (self.__attr, self.__args, self.__kwargs)

    @promise_method
    def get_name(self):
        return self.__attr

    @promise_method
    def get_args(self):
        return self.__args

    @promise_method
    def get_kwargs(self):
        return self.__kwargs

    @promise_method
    def set_args(self, args):
        self.__args = args

    @promise_method
    def set_kwargs(self, kwargs):
        self.__kwargs = kwargs

    @promise_method
    def clone(self):
        return EventualCommand(self.__attr, self.__args, self.__kwargs)
