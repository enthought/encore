#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

# System library imports.
import os
import unittest
import tempfile

# Local imports.
from ..file_lock import FileLock, LockError


class FileLockTest(unittest.TestCase):
    def setUp(self):
        self.path = os.tempnam(tempfile.gettempdir(), 'share')
        self.lock = FileLock(self.path)

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_acquire(self):
        self.lock.acquire()
        self.assertTrue(self.lock.acquired())
        self.assertTrue(self.lock.locked())
        self.assertTrue(os.path.exists(self.lock.full_path))
        self.lock.timeout = 0.1
        self.assertFalse(self.lock.acquire())

    def test_release(self):
        self.lock.acquire()
        self.lock.release()
        self.assertFalse(self.lock.acquired())
        self.lock.acquire()
        self.assertTrue(self.lock.acquired())

    def test_acquired(self):
        l2 = FileLock(self.path, timeout=0.1)
        self.lock.acquire()
        self.assertTrue(l2.locked())
        self.assertFalse(l2.acquired())
        self.assertFalse(l2.acquire())
        self.assertRaises(LockError, l2.release)
        self.lock.release()
        l2.acquire()
        self.assertTrue(l2.acquired())
        self.assertFalse(self.lock.acquired())
        self.assertTrue(self.lock.locked())


if __name__ == '__main__':
    unittest.main()
