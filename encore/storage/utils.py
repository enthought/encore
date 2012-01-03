#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
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

from encore.events.api import ProgressManager
from .events import (StoreTransactionStartEvent, StoreTransactionEndEvent,
    StoreProgressStartEvent, StoreProgressStepEvent, StoreProgressEndEvent,
    StoreModificationEvent)


class StoreProgressManager(ProgressManager):
    """ ProgressManager subclass that generates :py:class:`..events.StoreProgressEvent`
    instances
    """
    StartEventType = StoreProgressStartEvent
    StepEventType = StoreProgressStepEvent
    EndEventType = StoreProgressEndEvent
    

class DummyTransactionContext(object):
    """ A dummy class that can be returned by stores which don't support transactions
    """
    def __new__(cls, store):
        if getattr(store, '_transaction', None) is None:
            obj = object.__new__()
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
                {'source': self.store}, sys.maxint)
        
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
    
    """
    
    def __init__(self, iterator):
        self.iterator = iterator
        self.buffer = b''
    
    def read(self, buffer_size=1048576):
        while len(self.buffer) < buffer_size:
            try:
                data = self.iterator.next()
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


def buffer_iterator(filelike, buffer_size=1048576, progress=None):
    """ Return an iterator of byte buffers
    
    The buffers of bytes default to the provided buffer_size.  This is a useful
    method when copying one data stream to another.
    
    """
    progress = progress if progress is not None else lambda *args, **kwargs: None
    bytes_iterated = 0
    while True:
        chunk = filelike.read(buffer_size)
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
    
    """
    iters = itertools.tee(buffer_iterator(filelike, buffer_size), n)
    return [BufferIteratorIO(iter) for iter in iters]
