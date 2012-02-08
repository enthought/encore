#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
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
import os
import glob
from contextlib import contextmanager
from functools import wraps

# ETS library imports.
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


################################################################################
# LockingFileSystemStore class.
################################################################################
class LockingFileSystemStore(FileSystemStore):

    ##########################################################################
    # Basic Create/Read/Update/Delete Methods
    ##########################################################################

    def __init__(self, event_manager, path, force_lock_timeout=10.0,
                 magic_fname='.FSStore'):
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

        """
        super(LockingFileSystemStore, self).__init__(event_manager, path, magic_fname)
        self._force_lock_timeout = force_lock_timeout
        self._transaction = None
        self._transaction_locks = []

    def transaction(self, notes):
        """ Provide a transaction context manager"""
        transaction = SimpleTransactionContext(self)
        transaction.commands = []
        return transaction

    set = transact(locking(FileSystemStore.set))

    delete = transact(locking(FileSystemStore.delete))

    get_data = transact(locking(FileSystemStore.get_data, shared=True),
                        on_commit=False)

    get_metadata = transact(locking(FileSystemStore.get_metadata, shared=True),
                            on_commit=False)

    set_data = transact(locking(FileSystemStore.set_data))

    set_metadata = transact(locking(FileSystemStore.set_metadata))

    update_metadata = transact(locking(FileSystemStore.update_metadata))


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
                yield os.path.splitext(os.path.basename(x))[0]

    ##########################################################################
    # Private methods
    ##########################################################################

    def _get_lockfile_path(self, key):
        return os.path.join(self._root, key + '.lock')

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

