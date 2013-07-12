import traceback

from concurrent.futures import _base


class Future(_base.Future):

    def __init__(self):
        super(Future, self).__init__()
        self._traceback = None

    def traceback(self):
        return self._traceback

    def set_exception(self, exception):
        """Sets the result of the future as being the given exception.

        Should only be used by Executor implementations and unit tests.
        """
        with self._condition:
            self._exception = exception
            self._traceback = traceback.format_exc()
            self._state = _base.FINISHED
            for waiter in self._waiters:
                waiter.add_exception(self)
            self._condition.notify_all()
        self._invoke_callbacks()
