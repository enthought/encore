#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
from collections import deque

from encore.concurrent.futures.abc_work_scheduler import ABCWorkScheduler


class Serializer(ABCWorkScheduler):
    """ Execute all submitted jobs in order.

    All submitted operations are stored in a deque and are scheduled in
    sequence.

    .. warning::

        This is an experimental API and is subject to change.

    """
    def __init__(self, executor, name=None, callback=None):
        """ Initialize the Serializer.

        Parameters
        ----------
        executor : concurrent.features.Executor
            The executor to use for the jobs.

        name : string
            The name of the Serializer to be identified in the logs.

        callback : callable
            If a callable `callback` is provided, it will be called whenever an
            execution completes. The callback must accept as its only argument
            the Future that encapsulates the job. (The Future objects used by
            the scheduler are otherwise private.) Exceptions raised within the
            callback will be logged and suppressed.  See the
            `concurrent.futures` documentation for more information about
            Future callbacks.

        """
        super(Serializer, self).__init__(executor, name, callback)
        #: A deque to act as buffer for the pending operations
        self._pending_operations = deque()

    ###########################################################################
    # Private methods.
    ###########################################################################

    def _add_pending_operation(self, operation, args, kwargs):
        """ Add a new pending operation for scheduling.

        """
        self._pending_operations.append((operation, args, kwargs))

    def _get_next_operation(self):
        """ Get the next operation to schedule or return None.

        """
        if self._pending_operations:
            return self._pending_operations.popleft()
        else:
            return None
