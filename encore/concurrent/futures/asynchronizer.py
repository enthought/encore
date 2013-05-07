#
# (C) Copyright 2013 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
import logging
import threading

from .enhanced_thread_pool_executor import EnhancedThreadPoolExecutor

logger = logging.getLogger(__name__)


class Asynchronizer(object):
    """
    Allows 'forgetful' submission of operations, giving a convenient
    pattern for some asynchronous use-cases.

    The Asynchronizer executes at most a single operation at a time.  Requests
    to `submit` a new operation while an operation is executed are stored for
    future execution, with each new submission overwriting the prior.  When a
    running operation completes, the most recent submission (if one exists) is
    then executed. Therefore, operations submitted between the previous and
    current execution are forgotten.  The last operation submitted is
    guaranteed to eventually be executed.

    """
    def __init__(self, executor=None, name=None):
        # Condition variable to control access to shared state.
        self._state_lock = threading.Condition(lock=threading.RLock())
        # Currently executing future, or None.
        self._future = None
        # Either a tuple (operation, args, kwargs) representing a pending
        # call, or None.
        self._pending_operation = None
        # True if we're in the process of shutting down.
        self._shutdown = False

        if name is None:
            name = type(self).__name__
        self.name = name
        # If an executor is supplied, it's the job of the caller to
        # shut down that executor when done with it.  Otherwise we
        # create our own, and make sure to shut it down when this
        # object is shut down.
        if executor is None:
            self._shared_executor = False
            executor = EnhancedThreadPoolExecutor(
                max_workers=1,
                name="{0}Executor".format(self.name),
            )
        else:
            self._shared_executor = True
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
            self._pending_operation = operation, args, kwargs
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
        if not self._shared_executor:
            self._executor.shutdown()

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
        # Log any exception that occurred.
        try:
            future.result()
        except Exception as e:
            exc_type = type(e)
            logger.exception(
                "{0} occurred in submitted {1} job.".format(
                    exc_type.__name__,
                    self.name,
                )
            )

        with self._state_lock:
            self._future = None
            self._schedule_new()
            self._state_lock.notify_all()

    def _schedule_new(self):
        """
        Helper method to move a pending operation to the executor
        when necessary.

        """
        if self._pending_operation is not None and self._future is None:
            operation, args, kwargs = self._pending_operation
            self._pending_operation = None
            self._future = self._executor.submit(
                operation, *args, **kwargs)
            self._future.add_done_callback(
                self._operation_completion_callback)
