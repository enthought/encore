#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

# System library imports.
import os
import unittest
import tempfile
import glob
import shutil

# Local imports.
from ..file_lock import FileLock, SharedFileLock, LockError


def cleanup_files():
    # Clean up all stray lock files.
    for path in glob.iglob(os.path.join(tempfile.gettempdir(), 'share*.lock')):
        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
        except OSError as e:
            pass


class FileLockTest(unittest.TestCase):

    def setUp(self):
        self.fd, self.path = tempfile.mkstemp(
            suffix='share', dir=tempfile.gettempdir()
        )
        self.lock = FileLock(self.path)

    def tearDown(self):
        os.close(self.fd)
        os.remove(self.path)

    # This is needed to clean up stray lock file in case of killing a test run.
    # Note: tempnam seems buggy on Windows msys system; existing filenames are
    # reused resulting in failure if previous files are not cleaned.
    setUpClass = tearDownClass = staticmethod(cleanup_files)

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
        self.assertRaises(LockError, self.lock.release)
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

    def test_data(self):
        self.lock = FileLock(self.path, data=b"%i\n" % os.getpid())
        self.lock.acquire()
        self.assertTrue(self.lock.acquired())
        self.assertTrue(self.lock.locked())
        self.assertTrue(os.path.exists(self.lock.full_path))
        self.assertEqual(self.lock.get_data(), b"%i\n" % os.getpid())


class SharedFileLockTest(unittest.TestCase):
    def setUp(self):
        _, self.path = tempfile.mkstemp(
            suffix='share', dir=tempfile.gettempdir()
        )
        self.lock = SharedFileLock(self.path)
        self.lock2 = SharedFileLock(self.path)

    setUpClass = tearDownClass = staticmethod(cleanup_files)

    def test_acquire(self):
        self.lock.acquire()
        self.assertTrue(self.lock.acquired())
        self.assertFalse(self.lock.locked())
        self.assertTrue(os.path.exists(self.lock.full_path))

        self.lock2.acquire()
        self.assertTrue(self.lock2.acquired())
        self.assertFalse(self.lock2.locked())
        self.assertTrue(os.path.exists(self.lock2.full_path))

    def test_release(self):
        self.lock.acquire()
        self.lock.acquire()
        self.lock.release()
        self.assertTrue(self.lock.acquired())
        self.lock.release()
        self.assertFalse(self.lock.acquired())
        self.assertRaises(LockError, self.lock.release)


class MixedFileLockTest(unittest.TestCase):
    """ Test mixed usage of shared and exclusive locks. """
    def setUp(self):
        _, self.path = tempfile.mkstemp(
            suffix='share', dir=tempfile.gettempdir()
        )
        self.elock = FileLock(self.path) # Exclusive lock
        self.slock = SharedFileLock(self.path) # Shared lock
        self.slock2 = SharedFileLock(self.path)

    setUpClass = tearDownClass = staticmethod(cleanup_files)

    def test_acquire(self):
        self.elock.acquire()
        self.slock.timeout = 0.1
        self.assertFalse(self.slock.acquire())

        self.elock.release()
        self.slock.acquire()
        self.assertTrue(self.slock.acquired())
        self.assertTrue(self.elock.locked())

        self.slock2.acquire()
        self.assertTrue(self.slock2.acquired())

        self.slock.release()
        self.slock2.release()
        self.assertFalse(self.slock.locked())
        self.assertFalse(self.elock.locked())

        self.elock.acquire()
        self.assertTrue(self.elock.acquired())
        self.assertTrue(self.slock.locked())

if __name__ == '__main__':
    unittest.main()
