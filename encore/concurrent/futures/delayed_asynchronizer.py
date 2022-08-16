#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
import logging
import threading

from .asynchronizer import Asynchronizer

logger = logging.getLogger(__name__)


class DelayedAsynchronizer(Asynchronizer):
    """A 'forgetful' scheduling of operations which enforces a delay
    between submitted operations.

    The Asynchronizer executes at most a single operation at a
    time. Requests to `submit` a new operation while an operation is
    executed are stored for future execution, with each new submission
    overwriting the prior.  When a running operation completes, a timer
    is started to schedule the most recent submission (if one exists) to
    be executed. Therefore, operations submitted between the previous
    and current execution are discarded.  The last operation submitted
    is guaranteed to eventually be executed unless the delayed
    asynchronizer is shut down before the timer completes.

    """

    def __init__(self, executor, interval, name=None, callback=None,
                 timer_factory=None):
        """Initialize the Asynchronizer.

        Parameters
        ----------
        executor : concurrent.features.Executor
            The executor to use for the jobs.

        interval : float
            Interval between operations in seconds.

        name : string, optional
            The name of the Scheduler to be identified in the logs.

        callback : callable, optional
            If a callable `callback` is provided, it will be called whenever an
            execution completes.  The callback must accept as its only argument
            the Future that encapsulates the job. (The Future objects used by
            the scheduler are otherwise private.) Exceptions raised within the
            callback will be logged and suppressed.  See the
            `concurrent.futures` documentation for more information about
            Future callbacks.

        timer_factory : callable, optional
            Returns a suitable timer configured with the interval.  The
            factory must accept two arguments: The interval in seconds
            and a callback to be executed on timeout.  The returned
            timer must have ``start()`` method to start the timer and a
            ``cancel()`` method to cancel the current timer at shutdown.

        Notes
        -----
        Any exception that occurs are logged. Exceptions in the user-supplied
        callback will mask exceptions in the future, which is expected when
        working with future callbacks.

        """
        super(DelayedAsynchronizer, self).__init__(
            executor, name=name, callback=callback)
        if timer_factory is None:
            timer_factory = threading.Timer
        self._timer_factory = timer_factory
        self._timer = None
        self._interval = interval

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
            self._timer = self._timer_factory(
                self._interval, self._timer_callback)
            self._timer.start()

    def _timer_callback(self):
        with self._state_lock:
            self._future = None
            self._schedule_new()
            self._state_lock.notify_all()

    def _prevent_new_operations(self):
        """
        Prevent any new operations from being scheduled.

        Attempts to schedule any operation after this call will result
        in a RuntimeError.

        """
        with self._state_lock:
            self._shutdown = True
            if self._timer is not None:
                self._timer.cancel()
