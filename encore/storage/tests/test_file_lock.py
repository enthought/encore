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
import glob

# Local imports.
from ..file_lock import FileLock, LockError


class FileLockTest(unittest.TestCase):
    def setUp(self):
        self.path = os.tempnam(tempfile.gettempdir(), 'share')
        self.lock = FileLock(self.path)

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    @staticmethod
    def tearDownClass():
        # Clean up all stray lock files.
        for path in glob.iglob(os.path.join(tempfile.gettempdir(), 'share*.lock')):
            try:
                os.remove(path)
            except OSError as e:
                pass

    # This is needed to clean up stray lock file in case of killing a test run.
    # Note: tempnam seems buggy on Windows msys system; existing filenames are
    # reused resulting in failure if previous files are not cleaned.
    setUpClass = tearDownClass

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
        l2.release()

    def test_force_timeout(self):
        self.lock.acquire()
        self.assertTrue(self.lock.acquired())
        lock2 = FileLock(self.path, force_timeout=0.1)
        lock2.acquire()
        self.assertTrue(lock2.acquired())
        self.assertFalse(self.lock.acquired())
        self.assertTrue(self.lock.locked())
        lock2.release()
        self.assertFalse(self.lock.locked())

if __name__ == '__main__':
    unittest.main()
