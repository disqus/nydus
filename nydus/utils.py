from collections import defaultdict

from threading import Thread

from nydus.compat import Queue, Empty, iteritems, xrange, next


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


def apply_defaults(host, defaults):
    for key, value in iteritems(defaults):
        if key not in host:
            host[key] = value
    return host


def peek(value):
    generator = iter(value)
    prev = next(generator)
    for item in generator:
        yield prev, item
        prev = item
    yield prev, None


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
            except Exception as e:
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
            for k, v in iteritems(worker.results):
                results[k].extend(v)
        return results
