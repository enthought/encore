#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

""" Utilities for key-value store handling

"""

import itertools
from encore.events.api import ProgressManager
from .events import (StoreTransactionStartEvent, StoreTransactionEndEvent,
    StoreProgressStartEvent, StoreProgressStepEvent, StoreProgressEndEvent)


class StoreProgressManager(ProgressManager):
    StartEventType = StoreProgressStartEvent
    StepEventType = StoreProgressStepEvent
    EndEventType = StoreProgressEndEvent
    

class DummyTransactionContext(object):
    """ A dummy class that can be returned by stores which don't support transactions
    """
    def __new__(cls, store):
        if getattr(store, '_transaction', None) is None:
            store._transaction = super(DummyTransactionContext).__new__(store)
        return store._transaction
    
    def __init__(self, store):
        self.store = store
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        return False

class SimpleTransactionContext(object):
    """ A simple class that adds support for simple transactions

    This works by deferring set/update/delete operations until the __exit__
    method of the context.  This is still not a perfect transaction system, as
    the transaction could fail during the __exit__ method, leaving the store in
    an incorrect state.  However if the store only ever writes new keys and
    never updates or deletes, then transactions will work as expected.

    This also handles emitting events appropriately when called (ie. start
    transaction and end transaction at the appropriate places, plus defers
    sets/deletes to the end if called correctly).
    
    To work with this API, the store needs to implement the following methods:
        
        _set()
            The same signature as set(), but actually does the work.
        
        _update_metadata()
            The same signature as update_metadata(), but actually does the work.
        
        _delete()
            The same signature as delete(), but actually does the work.

    """
    def __new__(cls, store):
        if getattr(store, '_transaction', None) is None:
            store._transaction = super(DummyTransactionContext).__new__(store)
        return store._transaction
    
    def __init__(self, store):
        self.store = store
        self._context_depth = 0
        self._events = []
    
    def __enter__(self):
        self._context_depth += 1
        if self._context_depth == 1:
            self.store.event_manager.emit(StoreTransactionStartEvent(
                source=self.store))    
            # grab Set & veto events for later emission
            self.store.event_manager.connect(StoreModificationEvent, self._handle_event,
                {'source': self.store}, 0)
        
    def _handle_event(self, event):
        self._events.append(event)
        self.event.mark_as_handled()
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._context_depth -= 1
        if self._context_depth <= 0:
            self.store.event_manager.emit(StoreTransactionEndEvent(
                source=self.store))
            self.store.event_manager.disconnect(self._handle_event)
            self.store._transaction = None
            for event in self._events:
                event._handled = False # Yikes!
                self.store.event_manager.emit(event)
        return False

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


def tee(filelike, n=2, buffer_size=1048576):
    """ Clone a filelike stream into n parallel streams
    
    This uses itertools.tee and buffer iterators, with the corresponding
    cautions about memory usage.  In general it should be more memory efficient
    than pulling everything into memory.
    
    """
    iters = itertools.tee(buffer_iterator(filelike, buffer_size), n)
    return [BufferIteratorIO(iter) for iter in iters]
