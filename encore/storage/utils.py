#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

"""
Utils
=====

Utilities for key-value stores.

"""

import sys
import itertools
from types import MethodType

from encore.events.api import ProgressManager
from .events import (StoreTransactionStartEvent, StoreTransactionEndEvent,
    StoreProgressStartEvent, StoreProgressStepEvent, StoreProgressEndEvent,
    StoreModificationEvent)


def add_context_manager_support(obj):
    """ Add empty __enter__ and __exit__ methods on a given object.

    This function is required to be called on any object used by the data()
    methods on the stores. They are supposed to be file-like byte streams.
    Adding the support for using them as context managers allows us to make
    sure we clean the resources when a proper __exit__ method is available.

    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    if not hasattr(obj, '__enter__'):
        obj.__enter__ = MethodType(__enter__, obj)
    if not hasattr(obj, '__exit__'):
        obj.__exit__ = MethodType(__exit__, obj)

    return obj

class StoreProgressManager(ProgressManager):
    """ :py:class:`encore.events.progress_events.ProgressManager` subclass that
    generates :py:class:`encore.storage.events.StoreProgressEvent`
    instances
    """
    StartEventType = StoreProgressStartEvent
    StepEventType = StoreProgressStepEvent
    EndEventType = StoreProgressEndEvent


class DummyTransactionContext(object):
    """ A dummy class that can be returned by stores which don't support transactions

    This class guarantees that there is only one transaction object for each
    store instance.

    Parameters
    ----------
    store : key-value store instance
        The store that this transaction context is associated with.

    """
    def __new__(cls, store):
        if getattr(store, '_transaction', None) is None:
            obj = object.__new__(cls)
            obj.store = store
            store._transaction = obj
        return store._transaction

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        return False


class SimpleTransactionContext(object):
    """ A simple class that adds support for simple transactions

    This is a base class that ensures transactions are appropriately handled in
    terms of nesting and event generation.  Subclasses should override the
    start, commit and rollback methods to perform appropriate implementation-specific
    actions.

    This class correctly handles nested transactions by ensuring that each store
    has precisely one active transaction context and by tracking the number of
    times the context has been entered and exited.  The transaction is only
    committed once the top-level context has exited.

    Parameters
    ----------
    store : key-value store instance
        The store that this transaction context is associated with.

    """
    def __new__(cls, store):
        if getattr(store, '_transaction', None) is None:
            obj = object.__new__(cls)
            obj.store = store
            obj._context_depth = 0
            obj._events = []
            store._transaction = obj
        return store._transaction

    def __enter__(self):
        self._context_depth += 1
        if self._context_depth == 1:
            self.begin()
            self.store.event_manager.emit(StoreTransactionStartEvent(
                source=self.store))
            # grab Set & veto events for later emission
            self.store.event_manager.connect(StoreModificationEvent, self._handle_event,
                {'source': self.store}, sys.maxsize)

    def _handle_event(self, event):
        self._events.append(event)
        event.mark_as_handled()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._context_depth -= 1
        if self._context_depth <= 0:
            if exc_value is None:
                self.commit()
                state = 'done'
            else:
                self.rollback()
                state = 'failed'

            self.store.event_manager.emit(StoreTransactionEndEvent(
                source=self.store, state=state))
            self.store.event_manager.disconnect(StoreModificationEvent, self._handle_event)
            self.store._transaction = None

            if exc_value is None:
                for event in self._events:
                    event._handled = False # Yikes!
                    self.store.event_manager.emit(event)
        return False

    def begin(self):
        """ Begin a transaction

        By default, this calls the store's ``_begin_transaction`` method.
        Override in subclasses if you need different behaviour.
        """
        getattr(self.store, '_begin_transaction', lambda: None)()

    def commit(self):
        """ Commit a transaction

        By default, this calls the store's ``_commit_transaction`` method.
        Override in subclasses if you need different behaviour.
        """
        getattr(self.store, '_commit_transaction', lambda: None)()

    def rollback(self):
        """ Roll back a transaction

        By default, this calls the store's ``_rollback_transaction`` method.
        Override in subclasses if you need different behaviour.
        """
        getattr(self.store, '_rollback_transaction', lambda: None)()


class BufferIteratorIO(object):
    """ A file-like object based on an iterable of buffers

    This takes an iterator of bytes objects, such as produced by the
    buffer_iterator function, and wraps it in a file-like interface which
    is usable with the store API.

    This uses less memory than a StringIO, at the cost of some flexibility.

    Parameters
    ----------
    iterator : iterator of bytes objects
        An iterator that produces a bytes object on each iteration.

    """

    def __init__(self, iterator):
        self.iterator = iterator
        self.buffer = b''

    def read(self, buffer_size=1048576):
        """Read at most buffer_size bytes, returned as a string.

        """
        while len(self.buffer) < buffer_size:
            try:
                data = next(self.iterator)
            except StopIteration:
                break
            self.buffer += data
        result = self.buffer[:buffer_size]
        self.buffer = self.buffer[buffer_size:]
        return result

    def close(self):
        self.iterator = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


def buffer_iterator(filelike, buffer_size=1048576, progress=None, max_bytes=None):
    """ Return an iterator of byte buffers

    The buffers of bytes default to the provided buffer_size.  This is a useful
    method when copying one data stream to another.

    Parameters
    ----------
    filelike : a file-like object
        An object which implements the :py:meth:`read(buffer_size)` method.
    buffer_size : int
        The number of bytes to read at a time.
    progress : callable
        A callback for progress indication.  A StoreProgressManager instance
        inside a ``with`` block would be appropriate, but anthing that takes a
        `step` parameter which is the total number of bytes read so far will
        work.
    max_bytes : int
        The maximum number of bytes to return.

    """
    progress = progress if progress is not None else lambda *args, **kwargs: None
    bytes_iterated = 0
    while max_bytes is None or bytes_iterated < max_bytes:
        chunk = filelike.read(buffer_size)
        if max_bytes is not None and max_bytes - bytes_iterated < len(chunk):
            chunk = chunk[:max_bytes - bytes_iterated]
        bytes_iterated += len(chunk)
        progress(step=bytes_iterated)
        if not chunk:
            break
        yield chunk


def tee(filelike, n=2, buffer_size=1048576):
    """ Clone a filelike stream into n parallel streams

    This uses itertools.tee and buffer iterators, with the corresponding
    cautions about memory usage.  In general it should be more memory efficient
    than pulling everything into memory.


    Parameters
    ----------
    filelike : a file-like object
        An object which implements the :py:meth:`read(buffer_size)` method.
    n : int
        The number of filelike streams to produce.
    buffer_size : int
        The number of bytes to read at a time.

    """
    iters = itertools.tee(buffer_iterator(filelike, buffer_size), n)
    return [BufferIteratorIO(iter) for iter in iters]


class hashing_file(object):
    """ File-like wrapper which produces a hash as it is read

    """
    def __init__(self, filelike, hash):
        self.filelike = filelike
        self.hash = hash
        self.len = 0

    def read(self, nbytes):
        data = self.filelike.read(nbytes)
        self.hash.update(data)
        self.len += len(data)
        return data

    def close(self):
        return self.filelike.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.filelike.close()
