#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#

# standard library imports
import threading
import time
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

    def test_two_threads(self):
        # test that we don't run at the same time

        result = []

        @synchronized
        def my_func(thread):
            result.append('call started on {}'.format(thread))
            time.sleep(0.3)
            result.append('call finished on {}'.format(thread))

        thread1 = threading.Thread(target=lambda: my_func("thread 1"))
        thread2 = threading.Thread(target=lambda: my_func("thread 2"))

        thread1.start()
        time.sleep(0.1)
        thread2.start()

        thread1.join()
        thread2.join()

        self.assertEqual(result, ['call started on thread 1',
                                  'call finished on thread 1',
                                  'call started on thread 2',
                                  'call finished on thread 2'])
