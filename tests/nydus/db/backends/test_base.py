from __future__ import absolute_import

from nydus.db.backends.base import BaseConnection
from mock import patch
from mock import Mock

import unittest2


class TestBaseConnection(unittest2.TestCase):
    def test_getattr_break_recursion(self):

        with patch.object(BaseConnection, 'connection') as mock_connection:
            b = BaseConnection(0)
            mock_connection.__get__ = Mock(return_value=b)

            with self.assertRaises(AttributeError):
                print b.foo
