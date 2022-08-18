#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
"""Module of useful routines for working with concurrency."""

from functools import wraps
from threading import RLock


def synchronized(func):
    """ Decorator that prevents simultaneous execution of a function

    This decorator that ensures that only one thread at a time can be executing
    the decorated function at the same time by using a dedicated anonymous
    lock.

    """
    lock = RLock()

    @wraps(func)
    def wrapper(*args, **kw):
        with lock:
            return func(*args, **kw)

    return wrapper
