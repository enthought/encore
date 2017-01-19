import traceback

from concurrent.futures import _base


class Future(_base.Future):

    def __init__(self):
        super(Future, self).__init__()
        self._traceback_formatted = None

    def traceback(self):
        """Return the formatted traceback of the error that occured in the
        Executor worker, or None if no error occurred.

        """
        return self._traceback_formatted

    def set_exception(self, exception):
        """Sets the result of the future as being the given exception.

        Should only be used by Executor implementations and unit tests.
        """
        with self._condition:
            self._exception = exception
            self._traceback_formatted = traceback.format_exc()
            self._state = _base.FINISHED
            for waiter in self._waiters:
                waiter.add_exception(self)
            self._condition.notify_all()
        self._invoke_callbacks()
