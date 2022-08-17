#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#

from concurrent.futures import Executor, Future

class _WorkItem(object):
    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if not self.future.set_running_or_notify_cancel():
            return

        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as e:
            self.future.set_exception(e)
        else:
            self.future.set_result(result)


class SynchronousExecutor(Executor):
    """
    Simple Executor subclass that executes everything directly synchronously
    in the current thread.  The submit method of this executor blocks until
    the call is complete.  No cancellation of submitted tasks is possible.

    """

    def __init__(self):
        """ Initializes a new SynchronousExecutor instance."""
        self._shutdown = False

    def submit(self, fn, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = Future()
        w = _WorkItem(f, fn, args, kwargs)
        w.run()
        return f
    submit.__doc__ = Executor.submit.__doc__

    def shutdown(self, wait=True):
        self._shutdown = True
    shutdown.__doc__ = Executor.shutdown.__doc__
