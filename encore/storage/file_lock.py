#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

# System library imports.
import os
import errno
import socket
import time


class LockError(Exception):
    pass


class FileLock(object):
    """ A simple file-based lock. """
    def __init__(self, name, dir=None, poll_interval=1e-2, timeout=0,
                 force_timeout=0, uid=None):
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
        self._check_text = '%s\n%s\n%s\n%s\n%s'%(socket.gethostname(),
                                 os.getpid(), os.getuid(), self.uid, 'LOCK')

    def acquire(self):
        """ Acquire the lock.

        Returns False if timeout is exceeded, else keeps trying.

        """
        start_time = time.time()
        while True:
            try:
                fd = os.open(self.full_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
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
            with open(self.full_path) as f:
                text = f.read()
                if text != self._check_text:
                    raise LockError('Releasing an unacquired lock')
                else:
                    os.remove(self.full_path)
        except IOError as e:
            raise LockError('Releasing an unlocked lock')

    def locked(self):
        """ Whether the lock is acquired by anyone (including self). """
        return os.path.exists(self.full_path)

    def acquired(self):
        """ Whether the lock is acquired by self. """
        try:
            with open(self.full_path) as f:
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
        """ Wait till the lock in released. """
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
