#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
import logging

from encore.concurrent.futures.abc_work_scheduler import ABCWorkScheduler

logger = logging.getLogger(__name__)


class Asynchronizer(ABCWorkScheduler):
    """ A 'forgetful' scheduling of operations.

    The Asynchronizer executes at most a single operation at a time. Requests
    to `submit` a new operation while an operation is executed are stored for
    future execution, with each new submission overwriting the prior.  When a
    running operation completes, the most recent submission (if one exists) is
    then executed. Therefore, operations submitted between the previous and
    current execution are discarded.  The last operation submitted is
    guaranteed to eventually be executed.

    .. warning::

        This is an experimental API and is subject to change.

    """

    def __init__(self, executor, name=None, callback=None):
        """ Initialize the Asynchronizer.

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
        super(Asynchronizer, self).__init__(executor, name, callback)
        #: Either a tuple (operation, args, kwargs) representing a pending
        #: call, or None.
        self._pending_operation = None

    ###########################################################################
    # Private methods.
    ###########################################################################

    def _get_next_operation(self):
        """ Return the next operation to schedule or None.

        """
        pending = self._pending_operation
        self._pending_operation = None
        return pending

    def _add_pending_operation(self, operation, args, kwargs):
        """ Add a new pending operation for scheduling.

        """
        self._pending_operation = operation, args, kwargs
