#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
import abc
import logging
import threading

logger = logging.getLogger(__name__)


class ABCWorkScheduler(object):
    """ An abstract class to implement various job scheduling and execution
    models using executors.

    .. warning::

        This is an experimental API and is subject to change.

    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, executor, name=None, callback=None):
        """ Initialize the Scheduler.

        Parameters
        ----------
        executor : concurrent.features.Executor
            The executor to use for the jobs.

        name : string
            The name of the Scheduler to be identified in the logs.

        callback : callable
            If a callable `callback` is provided, it will be called whenever an
            execution completes.  The callback must accept as its only argument
            the Future that encapsulates the job. (The Future objects used by
            the scheduler are otherwise private.) Exceptions raised within the
            callback will be logged and suppressed.  See the
            `concurrent.futures` documentation for more information about
            Future callbacks.

        Notes
        -----
        Any exception that occurs are logged. Exceptions in the user-supplied
        callback will mask exceptions in the future, which is expected when
        working with future callbacks.

        """
        #: Condition variable to control access to shared state.
        self._state_lock = threading.Condition(lock=threading.RLock())
        #: Currently executing future, or None.
        self._future = None
        #: True if we're in the process of shutting down.
        self._shutdown = False
        if name is None:
            name = type(self).__name__
        self.name = name
        self._callback = callback
        self._executor = executor

    ###########################################################################
    # Public methods.
    ###########################################################################

    def submit(self, operation, *args, **kwargs):
        """
        Schedule an operation.

        """
        with self._state_lock:
            if self._shutdown:
                raise RuntimeError(
                    "Cannot submit new operations after shutdown.")
            self._add_pending_operation(operation, args, kwargs)
            self._schedule_new()

    def wait(self):
        """
        Wait for all current and pending operations to complete.

        """
        with self._state_lock:
            while self._future is not None:
                self._state_lock.wait()

    def shutdown(self):
        """
        Clean up and wait for pending operations.

        The call is synchronous:  when the call returns, all pending
        operations have completed.

        """
        self._prevent_new_operations()
        self.wait()

    ###########################################################################
    # Private methods.
    ###########################################################################

    def _prevent_new_operations(self):
        """
        Prevent any new operations from being scheduled.

        Attempts to schedule any operation after this call will result
        in a RuntimeError.

        """
        with self._state_lock:
            self._shutdown = True

    def _operation_completion_callback(self, future):
        """
        Called on completion of an operation.

        """
        try:
            if self._callback is not None:
                self._callback(future)
            future.result()
        except Exception as e:
            exc_type = type(e)
            logger.exception(
                "{0} occurred in submitted {1} job.".format(
                    exc_type.__name__,
                    self.name,
                )
            )
            logger.error('Actual error:\n{}'.format(future.traceback()))

        with self._state_lock:
            self._future = None
            self._schedule_new()
            self._state_lock.notify_all()

    def _schedule_new(self):
        """ Schedule a new operation as dictated by the implemented scheduling
        model.

        """
        if self._future is not None:
            return

        pending = self._get_next_operation()
        if pending is not None:
            operation, args, kwargs = pending
            self._future = self._executor.submit(operation, *args, **kwargs)
            self._future.add_done_callback(
                self._operation_completion_callback)

    @abc.abstractmethod
    def _add_pending_operation(self, operation, args, kwargs):
        """ Add a new pending operation for scheduling.

        """

    @abc.abstractmethod
    def _get_next_operation(self):
        """ Return the next operation to schedule or None.

        """
