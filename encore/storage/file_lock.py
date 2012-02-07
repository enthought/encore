#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#
""" This module defines a file based lock.

The `FileLock` is implemented by creating a file in the specified directory,
with some content to identify the host/process which acquired the lock.

The `FileLock` is also expected to work on NFS shared directories, in
Linux kernel version 2.6.5 and above.

"""

# System library imports.
import os
import errno
import socket
import time
import getpass


class LockError(Exception):
    pass


class FileLock(object):
    """ A simple file-based lock. """
    def __init__(self, name, dir=None, poll_interval=1e-2, timeout=0,
                 force_timeout=0, uid=None):
        """ Constructor.

        Parameters
        ----------
        name - str
            An identifier for the lock.
        dir - str
            The directory where the lock file is stored.
        poll_interval - float
            The interval to check for change in status of the lock.
        timeout - float
            The time to wait before failing to acquire a lock.
        force_timeout - float
            The time to wait before forcefully breaking a lock.
        uid - str or None
            A unique identifier for the lock. This will uniquely identify a
            lock within the same process. For example, a filesystem store
            could use its id as the identifier, so that all lock instances
            behave same within the store but any other store instance will
            have a different lock. If the uid is None, then the id of the
            lock instance is used, which means every lock instance is unique.

        Notes
        -----
        The lock is not released when it is garbage collected to avoid
        erroneous release when used as anonymous lock within a `with`
        context. Hence always use it within a `with` context to avoid
        stale unreleased locks.

        """
        self.name = name
        if dir is None:
            self.full_path = name + '.lock'
        else:
            name = name.lstrip('/').lstrip('\\')
            self.full_path = os.path.join(dir, name + '.lock')
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.force_timeout = force_timeout
        self.uid = id(self) if uid is None else uid
        self._open_mode = os.O_CREAT | os.O_EXCL | os.O_RDWR
        if hasattr(os, 'O_BINARY'):
            self._open_mode |= os.O_BINARY
        self._check_text = '%s\n%s\n%s\n%s\n%s'%(socket.gethostname(),
                                 os.getpid(), getpass.getuser(), self.uid,
                                 'LOCK')

    def acquire(self):
        """ Acquire the lock.

        Returns False if timeout is exceeded, else keeps trying.

        """
        start_time = time.time()
        while True:
            try:
                fd = os.open(self.full_path, self._open_mode)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
            else:
                os.write(fd, self._check_text)
                os.close(fd)
                return True
            if 0 < self.timeout < time.time()-start_time:
                return False
            if 0 < self.force_timeout < time.time()-start_time:
                self.force_break()
                continue
            time.sleep(self.poll_interval)

    def release(self):
        """ Release an acquired lock.

        Raises LockError if the lock is acquired by someone else
        or not acquired at all.

        """
        try:
            with open(self.full_path, 'rb') as f:
                text = f.read()
                if text != self._check_text:
                    raise LockError('Releasing an unacquired lock')
                else:
                    f.close()
                    os.remove(self.full_path)
        except IOError as e:
            raise LockError('Releasing an unlocked lock')

    def locked(self):
        """ Whether the lock is acquired by anyone (including self). """
        return os.path.exists(self.full_path)

    def acquired(self):
        """ Whether the lock is acquired by self. """
        try:
            with open(self.full_path, 'rb') as f:
                text = f.read()
                if text == self._check_text:
                    return True
                else:
                    return False
        except IOError as e:
            return False

    def force_break(self):
        """ Force-break a lock by deleting the lock-file. """
        try:
            os.remove(self.full_path)
        except OSError as e:
            if e.errno == errno.ENOENT:
                pass
            else:
                return False
        return True

    def wait(self):
        """ Wait till the lock is released.

        Returns False if the lock is not released before timeout.

        """
        start_time = time.time()
        while True:
            if self.locked():
                if 0 < self.timeout < time.time()-start_time:
                    return False
                if 0 < self.force_timeout < time.time()-start_time:
                    self.force_break()
                time.sleep(self.poll_interval)
            else:
                return True

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.release()
