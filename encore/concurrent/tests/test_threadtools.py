#
# (C) Copyright 2012-13 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#

# standard library imports
import unittest

# local imports
from encore.concurrent.threadtools import synchronized

class SynchronizedTest(unittest.TestCase):

    def test_synchronized(self):
        # simple test that decorator works

        @synchronized
        def my_func(x):
            return x

        result = my_func('test')

        self.assertEqual(result, 'test')
