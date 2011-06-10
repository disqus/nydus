import unittest2

class BaseTest(unittest2.TestCase):
    def setUp(self):
        pass

def dingus_calls_to_dict(obj):
    # remap dingus calls into a useable dict
    calls = {}
    for name, args, kwargs, obj in obj:
        if name not in calls:
            calls[name] = []
        calls[name].append((args, kwargs, obj))
    return calls