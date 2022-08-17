#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#
""" This module defines a file based lock.

The `FileLock` is implemented by creating a file in the specified directory,
with some content to identify the host/process which acquired the lock.

The `FileLock` is also expected to work on NFS shared directories, in
Linux kernel version 2.6.5 and above.

Basic Usage
-----------

::

    lock = FileLock('resource_1', '/tmp')
    lock.acquire()
    lock.release()

    slock = SharedFileLock('resource_1', '/tmp')
    slock.acquire()
    slock2 = SharedFileLock('resource_1', '/tmp')
    slock2.acquire() # Succeeds because lock is shared.

"""

# System library imports.
import os
import errno
import socket
import time
import getpass
import shutil


class LockError(Exception):
    pass


class FileLock(object):
    """ A simple file-based discretionary (advisory) exclusive lock. """
    def __init__(self, name, dir=None, poll_interval=1e-2, timeout=0,
                 force_timeout=0, uid=None, data=None):
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
        data - str or None
            The data to add to the lock file that can be used to obtain
            details about the locking process.  If set to None, this adds
            a default set of values as the hostname, pid, username and given
            uid to the lock file.  This data is also used to check if
            a different process is releasing the lock.

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

        if data is None:
            self._data = b'%s\n%i\n%s\n%i\n%s' % (
                socket.gethostname().encode('ascii'), os.getpid(),
                getpass.getuser().encode('ascii'),
                self.uid, b'LOCK'
            )
        else:
            self._data = data

    def acquire(self):
        """ Acquire the lock.

        Returns False if timeout is exceeded, else keeps trying.

        """
        start_time = time.time()
        while True:
            try:
                fd = os.open(self.full_path, self._open_mode)
            except OSError as e:
                if e.errno in (errno.EISDIR, errno.EEXIST, errno.EACCES):
                    try:
                        os.rmdir(self.full_path)
                        continue
                    except OSError as e:
                        pass
                else:
                    raise
            else:
                os.write(fd, self._data)
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
                data = f.read()
            if data != self._data:
                raise LockError('Releasing an unacquired lock')
            else:
                while True:
                    # While loop is needed here because delete may be
                    # denied on windows in case file is open by some other
                    # lock for checking by reading the file contents.
                    try:
                        os.remove(self.full_path)
                        break
                    except OSError as e:
                        if e.errno == errno.EACCES:
                            time.sleep(0.01)
                        else:
                            raise
        except IOError:
            raise LockError('Releasing an unlocked lock')

    def locked(self):
        """ Returns true if someone has an exclusive lock on the resource, i.e.
        someone created a LockFile for the given name. """
        if os.path.isfile(self.full_path):
            return True
        elif os.path.isdir(self.full_path):
            return len(os.listdir(self.full_path))>0
        else:
            return False

    def acquired(self):
        """ Whether the lock is acquired by self. """
        text = self.get_data()
        if text == self._data:
            return True
        else:
            return False

    def force_break(self):
        """ Force-break a lock by deleting the lock-file/directory.

        Returns True if the break was successful.

        """
        if os.path.isfile(self.full_path):
            os.remove(self.full_path)
        else:
            try:
                shutil.rmtree(self.full_path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    return True
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

    def get_data(self):
        """Return the data stored in the lock file.

        If None is returned the lock has not been acquired.
        """
        try:
            with open(self.full_path, 'rb') as f:
                text = f.read()
        except IOError:
            text = None
        return text

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.release()


class SharedFileLock(object):
    """ A simple file-based discretionary (advisory) shared lock. """
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
            self.dir_path = name + '.lock'
        else:
            name = name.lstrip('/').lstrip('\\')
            self.dir_path = os.path.join(dir, name + '.lock')
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.force_timeout = force_timeout
        self.uid = id(self) if uid is None else uid
        self.file_name = '%s__%s__%s__%s.lock'%(socket.gethostname(),
                                    os.getpid(), getpass.getuser(), self.uid)
        self.full_path = os.path.join(self.dir_path, self.file_name)
        self._level = 0
        self._open_mode = os.O_CREAT | os.O_EXCL | os.O_RDWR
        if hasattr(os, 'O_BINARY'):
            self._open_mode |= os.O_BINARY

    def acquire(self):
        """ Acquire the lock.

        Returns False if timeout is exceeded, else keeps trying.

        """
        if self._level > 0:
            self._level += 1
            return True
        start_time = time.time()
        while True:
            # Try creating the shared lock file.
            try:
                fd = os.open(self.full_path, self._open_mode)
            except OSError as e:
                if e.errno == errno.ENOTDIR:
                    # Exclusive lock exists.
                    pass
                elif e.errno == errno.ENOENT:
                    # The shared lock directory does not exist.
                    try:
                        os.mkdir(self.dir_path)
                    except OSError as e:
                        if not os.path.exists(self.dir_path):
                            continue
            else:
                os.close(fd)
                self._level += 1
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
        if self._level == 0:
            raise LockError('Releasing an unlocked lock')

        if self._level > 1:
            self._level -= 1
            return

        try:
            os.remove(self.full_path)
            self._level = 0
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise LockError('Releasing an unlocked lock')
            else:
                raise

    def locked(self):
        """ Returns true if someone has an exclusive lock on the resource, i.e.
        someone created a LockFile for the given name. """
        return os.path.isfile(self.dir_path)

    def acquired(self):
        """ Whether the lock is acquired by self. """
        return os.path.exists(self.full_path)

    def force_break(self):
        """ Force-break a lock by deleting the lock-file.

        Returns True if the break was successful.
        """
        if os.path.isfile(self.dir_path):
            os.remove(self.dir_path)
        else:
            try:
                os.rmdir(self.full_path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    return True
                else:
                    return False
        return True
