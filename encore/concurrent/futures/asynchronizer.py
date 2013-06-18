#
# (C) Copyright 2012-13 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
import logging

from encore.concurrent.futures.abc_work_scheduler import ABCWorkScheduler

logger = logging.getLogger(__name__)


class Asynchronizer(ABCWorkScheduler):
    """
    Allows 'forgetful' submission of operations, giving a convenient
    pattern for some asynchronous use-cases.

    The last operation submitted is guaranteed to eventually be executed.

    """

    def __init__(self, executor, name=None, callback=None):
        super(Asynchronizer, self).__init__(executor, name, callback)
        # Either a tuple (operation, args, kwargs) representing a pending
        # call, or None.
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
        """ Add a new pending operation for sceduling.

        """
        self._pending_operation = operation, args, kwargs
