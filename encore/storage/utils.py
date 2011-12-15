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
from .events import (StoreProgressStartEvent, StoreProgressStepEvent,
    StoreProgressEndEvent)

class StoreProgressManager(ProgressManager):
    StartEventType = StoreProgressStartEvent
    StepEventType = StoreProgressStepEvent
    EndEventType = StoreProgressEndEvent
    

class DummyTransactionContext(object):
    """ A dummy class that can be returned by stores which don't support transactions

    """
    
    def __enter__(self):
        return
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
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
