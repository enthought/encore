#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#

import concurrent.futures
import Queue
import threading


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


class LazyExecutor(concurrent.futures.Executor):
    """
    concurrent.futures.Executor subclass that stores submitted work items in a
    queue, and only executes those work items when explicitly asked to do so.

    Methods 'execute_one' and 'execute_all' are provided to explicitly process
    work items.

    The submit and shutdown methods are thread-safe: calls to those methods may
    be made from any thread at any time.

    """
    def __init__(self):
        """ Initialize a new LazyExecutor instance."""
        self._work_queue = Queue.Queue()
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError(
                    'cannot schedule new futures after shutdown')
            f = concurrent.futures.Future()
            w = _WorkItem(f, fn, args, kwargs)
            self._work_queue.put(w)
            return f
    submit.__doc__ = concurrent.futures.Executor.submit.__doc__

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
    shutdown.__doc__ = concurrent.futures.Executor.shutdown.__doc__

    def execute_one(self):
        """
        Execute a single task from the work queue.

        Raise Queue.Empty exception if there are currently no tasks to process.

        """
        work_item = self._work_queue.get(block=False)
        work_item.run()

    def execute_all(self):
        """
        Execute all tasks in the work queue.

        Stop when the work queue is empty.

        """
        while True:
            try:
                work_item = self._work_queue.get(block=False)
            except Queue.Empty:
                break
            work_item.run()
