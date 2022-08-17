#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#
"""
Locking File System Store
=========================

This file defines a locking filesystem store.  This modifies the filesystem
store to lock files before modifying so that multiple clients cannot
simultaneously modify the same key. This also provides a transaction context
for the store.

Warning
-------
The implementation of transaction gets an exclusive lock on keys used in a
transaction to prevent changes to data/metadata corresponding to the key used
in a transaction. This means there can be *deadlock* between clients which
attempt to access keys in different sequence within a transaction.

Ex. Client A reads file1 and then reads file2, whereas client B reads file2
    then file1. This may result in a deadlock when client A is waiting for
    lock on file2, whereas client B is waiting for lock on file1.

"""

# System library imports.
from contextlib import contextmanager
import datetime
from functools import wraps
import glob
import io
import os
import time
import threading

# ETS library imports.
from .events import StoreSetEvent, StoreUpdateEvent,\
    StoreDeleteEvent, StoreKeyEvent
from .filesystem_store import FileSystemStore
from .file_lock import FileLock
from .utils import SimpleTransactionContext


def transact(function, on_commit=True):
    """ Wrap a store method to add command to transaction instead of executing
    it immediately if within a transaction context. If not in a transaction
    context, the command is executed immediately.

    Parameters
    ----------
    function - callable
        The function to wrap.
    on_commit - bool (default True)
        Whether the operation is to be performed when the transaction is
        committed or whether the transaction is to be performed immediately
        and the result returned.
        Note: 'Read' operations need to set on_commit to True,
              'Write' operations need to set it to False.

    """
    @wraps(function)
    def wrapper(self, key, *args, **kwds):
        if self._transaction is None:
            return function(self, key, *args, **kwds)
        elif not on_commit:
            context = self._locking(key, recurse=True)
            self._transaction_locks.append(context)
            context.__enter__()
            return function(self, key, *args, **kwds)
        else:
            context = self._locking(key, recurse=True)
            self._transaction_locks.append(context)
            context.__enter__()

            self._transaction.commands.append((function, (self, key)+args, kwds))
            return len(self._transaction.commands)
    return wrapper

def locking(function, recurse=True, shared=False):
    """ Wrap a store method to lock the corresponding key in store from
    modification by other clients before executing the command.

    Parameters
    ----------
    function - callable
        The function to wrap.
    recurse - bool
        Whether the lock can be acquired recursively.
    shared - bool
        Whether the lock is a shared (non-exclusive/read) lock. If false, the
        lock is an exclusive (write) lock.

    """
    @wraps(function)
    def wrapper(self, key, *args, **kwds):
        with self._locking(key, recurse, shared):
            ret = function(self, key, *args, **kwds)
        return ret
    return wrapper


def write_log(func, event='w'):
    """ Write an entry for the call into a log file.

    Parameters
    ----------
    func - callable
        The callable which is to be called for performing the action.
    event - str ('w')
        A single character code for the type of event. Current convention is:
            'w' - write event
            'u' - update event
            'd' - delete event

    """
    @wraps(func)
    def wrapper(store, key, *args, **kwds):
        log_path = store._log_file
        with store._locking(log_path, recurse=True):
            time = datetime.datetime.utcnow()
            ret = func(store, key, *args, **kwds)
            log_file = open(log_path, 'a+b')
            end_pos = os.stat(log_path).st_size
            if end_pos > 0:
                # FIXME: Assuming max line length is 1024
                log_file.seek(max(0, end_pos-1024))
                etext = log_file.read()
                log_file.seek(0) # Needed on windows to be able to write.
                text = etext.splitlines()[-1]
                id = int(text.split()[0]) + 1
            else:
                id = 1
            new_line = '%d %s %s %s\n' % (id, event, time, key)
            log_file.write(new_line.encode('utf-8'))
            log_file.close()

            if id % store._logrotate_limit == 0:
                store._log_rotate()
        return ret
    return wrapper

################################################################################
# LockingFileSystemStore class.
################################################################################
class LockingFileSystemStore(FileSystemStore):

    ##########################################################################
    # Basic Create/Read/Update/Delete Methods
    ##########################################################################

    def __init__(self, path, force_lock_timeout=10.0,
                 magic_fname='.FSStore',
                 remote_event_poll_interval=5.0,
                 max_time_delta=datetime.timedelta(minutes=1)):
        """Initializes the store given a path to a store.

        Parameters
        ----------
        event_manager : Event Manager instance
            This is used to emit suitable events.
        path : str:
            A path to the root of the file system store.
        force_lock_timeout: float
            The maximum time a transaction may take. The FileLock is forcibly
            unlocked if it cannot be acquired within this time.
        magic_fname :
            The name of the magic file in that directory,
        remote_event_poll_interval : float
            The time interval to poll for changes to store by remote clients.
            StoreEvents are polled and emitted at this interval.
        max_time_delta : datetime.timedelta(minutes=1)
            The maximum permissible timedelta between different clients.
            This value is used to return the keys in a query_key when the
            parameters are ``last_modified__gte=timedelta`` , in this case the
            max_time_delta is subtracted from the given query timedelta.

        """
        super(LockingFileSystemStore, self).__init__(path, magic_fname)
        self._force_lock_timeout = force_lock_timeout
        self._transaction = None
        self._transaction_locks = []

        self._max_time_delta = max_time_delta

        # The store commit log file.
        self._log_file = os.path.join(path, '__log__.txt')
        # The approx number of entries to keep in rotated log files.
        # The active log file may contain double this number of entries.
        self._logrotate_limit = 10000
        # The maximum number of old log files to keep.
        self._log_keep = 5
        try:
            # Create the log file atomically.
            os.close(os.open(self._log_file, os.O_CREAT|os.O_EXCL|os.O_RDWR))
        except OSError:
            pass

        self._remote_event_poll_interval = remote_event_poll_interval
        self._remote_poll_thread = threading.Thread(target=self._remote_event_emit)
        self._remote_poll_thread.daemon = True
        self._remote_poll_thread.start()

    def transaction(self, notes):
        """ Provide a transaction context manager"""
        transaction = SimpleTransactionContext(self)
        transaction.commands = []
        return transaction

    set = transact(locking(write_log(FileSystemStore.set)))

    delete = transact(locking(write_log(FileSystemStore.delete, event='d')))

    get_data = transact(locking(FileSystemStore.get_data, shared=True),
                        on_commit=False)

    get_metadata = transact(locking(FileSystemStore.get_metadata, shared=True),
                            on_commit=False)

    set_data = transact(locking(write_log(FileSystemStore.set_data)))

    set_metadata = transact(locking(write_log(FileSystemStore.set_metadata)))

    update_metadata = transact(locking(write_log(FileSystemStore.update_metadata,
                                                 event='u')))


    def query(self, select=None, **kwargs):
        """ Query for keys and metadata matching metadata provided as keyword arguments

        This provides a very simple querying interface that returns precise
        matches with the metadata.  If no arguments are supplied, the query
        will return the complete set of metadata for the key-value store.

        Parameters
        ----------
        select : iterable of strings or None
            An optional list of metadata keys to return.  If this is not None,
            then the metadata dictionaries will only have values for the specified
            keys populated.
        kwargs :
            Arguments where the keywords are metadata keys, and values are
            possible values for that metadata item.

        Returns
        -------
        result : iterable
            An iterable of (key, metadata) tuples where metadata matches
            all the specified values for the specified metadata keywords.
            If a key specified in select is not present in the metadata of a
            particular key, then it will not be present in the returned value.

        """
        all_metadata = glob.glob(os.path.join(self._root, '*.metadata'))
        items = [(os.path.splitext(os.path.basename(x))[0], x) for x in all_metadata]
        if select is not None:
            for key, path in items:
                self._wait_if_locked(key)
                metadata = self._get_metadata(path)
                if all(metadata.get(arg) == value for arg, value in kwargs.items()):
                    yield key, dict((metadata_key, metadata[metadata_key])
                        for metadata_key in select if metadata_key in metadata)
        else:
            for key, path in items:
                self._wait_if_locked(key)
                metadata = self._get_metadata(path)
                if all(metadata.get(arg) == value for arg, value in kwargs.items()):
                    yield key, metadata.copy()

    def query_keys(self, **kwargs):
        """ Query for keys matching metadata provided as keyword arguments

        This provides a very simple querying interface that returns precise
        matches with the metadata.  If no arguments are supplied, the query
        will return the complete set of keys for the key-value store.

        This is equivalent to ``self.query(**kwargs).keys()``, but potentially
        more efficiently implemented.

        Parameters
        ----------
        kwargs :
            Arguments where the keywords are metadata keys, and values are
            possible values for that metadata item.

        Returns
        -------
        result : iterable
            An iterable of key-value store keys whose metadata matches all the
            specified values for the specified metadata keywords.

        """
        # Optimize for special cases.
        basename = os.path.basename
        if 'type' in kwargs:
            typ = kwargs['type']
            if typ in ('file', 'dir'):
                pattern = '{0}.*.metadata'.format(typ)
                for i in glob.iglob(os.path.join(self._root, pattern)):
                    yield basename(i)[:-9]
                return

        all_metadata = glob.glob(os.path.join(self._root, '*.metadata'))
        if kwargs:
            items = [(os.path.splitext(os.path.basename(x))[0], x) for x in all_metadata]
            for key, path in items:
                self._wait_if_locked(key)
                metadata = self._get_metadata(path)
                if all(metadata.get(arg) == value for arg, value in kwargs.items()):
                    yield key
        else:
            for x in all_metadata:
                yield os.path.splitext(basename(x))[0]

    def get_modified_keys(self, since):
        """ Get all keys which were modified since the specified time.

        NOTE: The `since` time specified is subtracted with the
        `_max_time_delta` for comparison.

        """
        timestamp = since
        if isinstance(timestamp, str):
            ISO_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
            timestamp = datetime.datetime.strptime(timestamp, ISO_FORMAT)
        timestamp -= self._max_time_delta
        write = False
        modified_keys = []
        for id, typ, mtime, key in self._log_iter():
            if write is False and mtime > timestamp:
                write = True
            if write is True:
                modified_keys.append(key)
        return modified_keys

    ##########################################################################
    # Private methods
    ##########################################################################

    def _get_lockfile_path(self, key):
        return os.path.join(self._root, key)

    @contextmanager
    def _locking(self, key, recurse=True, shared=False):
        """ Simple implementation of a recursive lock context. """
        lock = self._lock(key)
        if shared:
            if lock.acquired():
                yield lock
                return
            else:
                with self._lock(key, True) as lock:
                    yield lock
                return

        if recurse and lock.acquired():
            yield lock
        else:
            with lock:
                yield lock

    def _locked(self, key):
        """ Whether the specified is locked for writing by someone. """
        return self._lock(key).locked()

    def _lock(self, key, shared=False):
        """ Return a lock corresponding to the specified key.

        Lock is unique for the store instance, so another lock returned
        by this method for same key will also return same results for
        acquired() method.

        """
        return FileLock(self._get_lockfile_path(key), uid=id(self),
                        force_timeout=self._force_lock_timeout)

    def _wait_if_locked(self, key):
        """ Wait until the specified key is no longer acquired by someone else
        or writing. It may still be acquired by self. """
        l = self._lock(key)
        return l.acquired() or l.wait()

    def _begin_transaction_(self):
        self._transaction.commands = []

    def _rollback_transaction(self):
        self._release_transaction_locks()
        self._transaction.commands = []

    def _commit_transaction(self):
        """ Execute the methods in the transaction, releasing all locks in end.

        """
        commands = self._transaction.commands
        try:
            for command in commands:
                command[0](*command[1], **command[2])
        finally:
            self._release_transaction_locks()

    def _release_transaction_locks(self):
        for context in self._transaction_locks:
            context.__exit__(None, None, None)
        self._transaction_locks = []

    def _log_rotate(self):
        """ Rotate commit-log files. """
        log_path = self._log_file
        with self._locking(self._log_file, recurse=True):
            size = os.stat(log_path).st_size
            with open(log_path, 'rb') as f:
                first_log = f.readline()
                f.seek(size//2)
                f.readline()
                split_pos = f.tell()
                f.seek(0)
                file_name = '{}.{}'.format(log_path, first_log.split(b' ', 1)[0].decode('ascii'))
                with open(file_name, 'wb') as f2:
                    f2.write(f.read(split_pos))
                new_text = f.read()
            with open(log_path, 'wb') as f:
                f.write(new_text)

            log_files = [path for path in glob.iglob(self._log_file+'.*')
                                if not path.endswith('.lock')]
            log_files.sort(key=lambda s: int(s.rsplit('.')[-1]), reverse=True)
            for log_file in log_files[self._log_keep:]:
                os.remove(log_file)

    # Emit events for changed by other users. #################################

    def _remote_event_emit(self):
        """ Emit events due to change in store by other users. """
        last_log = None
        last_emit = 0
        while self._remote_poll_thread:
            if last_emit >= self._remote_event_poll_interval:
                try:
                    last_log = self._check_remote_event(last_log)
                except OSError:
                    # Store got deleted
                    return
                last_emit -= self._remote_event_poll_interval
            # So that application does not wait too long for poller thread to exit.
            to_sleep = min(self._remote_event_poll_interval, 0.1)
            last_emit += to_sleep
            time.sleep(to_sleep)

    def _check_remote_event(self, id=None):
        """ Check if any new remote changes occurred and emit events. """
        if id is None:
            with self._locking(self._log_file, recurse=True):
                try:
                    f = open(self._log_file)
                    size = os.stat(self._log_file).st_size
                    f.seek(max(size - 1024, 0))
                    text = f.read(1024)
                    lines = text.splitlines()
                    if lines:
                        id = lines[-1][0]
                except IOError:
                    return None
                finally:
                    f.close()
        else:
            with self._locking(self._log_file, recurse=True):
                try:
                    f = open(self._log_file)
                    text = f.read()
                    seek = self._search_log(id, text)
                    if seek < 0:
                        return None
                    text = text[seek:]
                    for line in text.splitlines():
                        try:
                            id, typ, date, time, key = line.split(' ', 4)
                            self._emit_remote_event(id, typ, date, time, key)
                        except ValueError:
                            pass
                except IOError:
                    return None
                finally:
                    f.close()
        return id

    def _search_log(self, id, text):
        """ Return the index of the given id in the log text. """
        id += ' '
        if text.startswith(id):
            return 0
        try:
            return text.index('\n'+id)+1
        except ValueError:
            return -1

    def _emit_remote_event(self, id, typ, date, time, key):
        """ Emit an event of appropriate type based on the log entries. """
        cls_map = {'w':StoreSetEvent,
                   'u':StoreUpdateEvent,
                   'd':StoreDeleteEvent}
        cls = cls_map.get(typ, StoreKeyEvent)
        ISO_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
        event = cls(key=key,
                    timestamp=datetime.datetime.strptime(date+' '+time,
                                                         ISO_FORMAT))
        self.event_manager.emit(event)

    def _log_iter(self):
        """ Iterate over each entry in the commit log.

        The yielded value is a tuple of (id, type, timestamp, key), where
        id is an increasing int identifier for the commit,
        type is a single character commit type (see `write_log` docstring)
        timestamp is the datatime.datetime instance of the commit,
        key is the str key which is modified in the commit.

        """
        ISO_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
        try:
            with io.open(self._log_file, 'r', encoding='utf-8') as log_file:
                for line in log_file:
                    line = line.rstrip('\n')
                    id, typ, date, time, key = line.split(' ', 4)
                    tstamp = datetime.datetime.strptime(
                        '%s %s' % (date, time), ISO_FORMAT
                    )
                    yield int(id), typ, tstamp, key
        except (IOError, ValueError) as exc:
            pass

    def __del__(self):
        self._remote_poll_thread = None
