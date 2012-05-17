from collections import defaultdict, Mapping
from Queue import Queue, Empty
from threading import Thread


# import_string comes form Werkzeug
# http://werkzeug.pocoo.org
def import_string(import_name, silent=False):
    """Imports an object based on a string. If *silent* is True the return
    value will be None if the import fails.

    Simplified version of the function with same name from `Werkzeug`_.

    :param import_name:
        The dotted name for the object to import.
    :param silent:
        If True, import errors are ignored and None is returned instead.
    :returns:
        The imported object.
    """
    import_name = str(import_name)
    try:
        if '.' in import_name:
            module, obj = import_name.rsplit('.', 1)
            return getattr(__import__(module, None, None, [obj]), obj)
        else:
            return __import__(import_name)
    except (ImportError, AttributeError):
        if not silent:
            raise


class frozendict(Mapping):
    """
    An immutable dictionary.

    >>> d = frozendict(foo=bar)
    >>> d['foo']
    'bar'
    >>> d['foo'] = 'bar'
    (some kind of error saying you cant do this)
    """
    def __init__(self, *args, **kwargs):
        self._d = dict(*args, **kwargs)

    def __repr__(self):
        return '<%s: %s>' % (type(self).__name__, self._d)

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def __getitem__(self, key):
        return self._d[key]

    def __hash__(self):
        if self._hash is None:
            self._hash = 0
            for key, value in self.iteritems():
                self._hash ^= hash(key)
                self._hash ^= hash(value)
        return self._hash


class Worker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        self.results = defaultdict(list)

    def run(self):
        while True:
            try:
                ident, func, args, kwargs = self.queue.get_nowait()
            except Empty:
                break

            try:
                result = func(*args, **kwargs)
                self.results[ident].append(result)
            except Exception, e:
                self.results[ident].append(e)
            finally:
                self.queue.task_done()

        return self.results


class ThreadPool(object):
    def __init__(self, workers=10):
        self.queue = Queue()
        self.workers = []
        self.tasks = []
        for worker in xrange(workers):
            self.workers.append(Worker(self.queue))

    def add(self, ident, func, args=None, kwargs=None):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        task = (ident, func, args, kwargs)
        self.tasks.append(ident)
        self.queue.put_nowait(task)

    def join(self):
        for worker in self.workers:
            worker.start()

        results = defaultdict(list)
        for worker in self.workers:
            worker.join()
            for k, v in worker.results.iteritems():
                results[k].extend(v)
        return results
