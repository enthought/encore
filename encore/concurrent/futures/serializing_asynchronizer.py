#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
from collections import OrderedDict

from encore.concurrent.futures.abc_work_scheduler import ABCWorkScheduler


class SerializingAsynchronizer(ABCWorkScheduler):
    """Provides Asynchronizer functionality for multiple operations.

    The SerializingAsynchronizer provides the same guarantees as the
    :class:`~encore.concurrent.futures.asynchronizer.Asynchronizer` for
    multiple different operations.  For any submitted callable, requests
    to submit a new operation while an operation of **the same**
    callable is underway, the new operation is stored overwriting the
    prior.  Different submitted callables are executed serially in the
    order in which they were originally submitted.

    For example if long-running callable ``C`` is submitted, then
    callable ``A``, then callable ``B``, then callable ``A`` again (all
    while ``C`` is running), then the order of execution will be ``C``,
    ``A``, ``B``.

    .. warning::

        This is an experimental API and is subject to change.

    """

    def __init__(self, executor, name=None, callback=None):
        super(SerializingAsynchronizer, self).__init__(
            executor, name, callback)
        # Ordered dictionary containing tuples (operation, args, kwargs)
        # representing pending operations.  The items are keyed by the
        # operation. Operations are executed in the order they were
        # entered in the dict.
        self._pending_operations = OrderedDict()

    ###########################################################################
    # Private methods.
    ###########################################################################

    def _get_next_operation(self):
        """ Return the next operation to schedule or None.

        """
        if len(self._pending_operations) == 0:
            return None
        _, pending = self._pending_operations.popitem(last=False)
        return pending

    def _add_pending_operation(self, operation, args, kwargs):
        """ Add a new pending operation for scheduling.

        """
        self._pending_operations[operation] = operation, args, kwargs
