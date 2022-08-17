#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
import contextlib
import logging
import operator
import time
import unittest

from encore.concurrent.futures.asynchronizer import Asynchronizer
from encore.concurrent.futures.enhanced_thread_pool_executor import (
    EnhancedThreadPoolExecutor)


def _worker(data, value):
    time.sleep(0.25)
    data.append(value)
    return value


class TestHandler(logging.Handler):
    """
    Simple logging handler that just accumulates and stores records.

    """
    def __init__(self):
        logging.Handler.__init__(self)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@contextlib.contextmanager
def loghandler(logger_name):
    """
    Log errors from a call and yield the handler.

    """
    handler = TestHandler()
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    old_propagate_value = logger.propagate
    logger.propagate = False
    logger.addHandler(handler)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.propagate = old_propagate_value


class _TestException(Exception):
    pass


class TestAsynchronizer(unittest.TestCase):

    def setUp(self):
        self.executor = EnhancedThreadPoolExecutor(
            name='TestAsynchronizerExecutor',
            max_workers=1)
        self.asynchronizer = Asynchronizer(
            name='TestAsynchronizer',
            executor=self.executor,
        )

    def test_events_collapsed(self):
        numbers = []
        self.asynchronizer.submit(_worker, numbers, 1)
        self.asynchronizer.submit(_worker, numbers, 2)
        self.asynchronizer.submit(_worker, numbers, 3)
        self.asynchronizer.submit(_worker, numbers, 4)
        self.asynchronizer.submit(_worker, numbers, 5)
        self.asynchronizer.submit(_worker, numbers, 6)
        self.asynchronizer.submit(_worker, numbers, 7)
        self.asynchronizer.submit(_worker, numbers, 8)
        self.asynchronizer.submit(_worker, numbers, 9)
        self.asynchronizer.submit(_worker, numbers, 10)
        self.asynchronizer.wait()
        self.assertEqual(len(numbers), 2)
        self.assertEqual(numbers[0], 1)
        self.assertEqual(numbers[1], 10)

    def test_callback(self):
        # Make a callback that repeats the insertion into another queue.
        callback_numbers = []

        def _callback(future):
            value = future.result()
            callback_numbers.append(value)

        asynchronizer = Asynchronizer(
            name='TestCallbackAsynchronizer',
            executor=self.executor,
            callback=_callback
        )

        numbers = []
        asynchronizer.submit(_worker, numbers, 1)
        asynchronizer.submit(_worker, numbers, 2)
        asynchronizer.submit(_worker, numbers, 3)
        asynchronizer.submit(_worker, numbers, 4)
        asynchronizer.submit(_worker, numbers, 5)
        asynchronizer.submit(_worker, numbers, 6)
        asynchronizer.submit(_worker, numbers, 7)
        asynchronizer.submit(_worker, numbers, 8)
        asynchronizer.submit(_worker, numbers, 9)
        asynchronizer.submit(_worker, numbers, 10)
        asynchronizer.wait()
        self.assertEqual(len(numbers), 2)
        self.assertEqual(numbers[0], 1)
        self.assertEqual(numbers[1], 10)
        self.assertEqual(len(callback_numbers), 2)
        self.assertEqual(callback_numbers[0], 1)
        self.assertEqual(callback_numbers[1], 10)
        asynchronizer.shutdown()

    def test_asynchronizer_name(self):
        asynchronizer = Asynchronizer(executor=self.executor, name="Will")
        self.assertEqual(asynchronizer.name, "Will")
        self.assertEqual(
            asynchronizer._executor.name,
            'TestAsynchronizerExecutor')

    def test_submit_after_shutdown(self):
        self.asynchronizer.shutdown()
        with self.assertRaises(RuntimeError):
            self.asynchronizer.submit(lambda: None)

    def test_submit_bad_job(self):
        """
        Submission of a job that causes an exception should succeed,
        but we should get a logged exception as a result.

        """
        logger_name = 'encore.concurrent.futures.abc_work_scheduler'
        with loghandler(logger_name) as handler:
            self.asynchronizer.submit(operator.floordiv, 1, 0)
            self.asynchronizer.wait()

        # We log two messages for each failure. The actual traceback
        # from the worker, and the exception of where it occurred
        # (i.e. where the result was accessed)
        self.assertEqual(len(handler.records), 2)
        record = handler.records[0]
        self.assertIsNotNone(record.exc_info)
        exc_type, exc_value, exc_tb = record.exc_info
        self.assertIs(exc_type, ZeroDivisionError)

    def test_submit_bad_job_with_callback(self):
        """
        Submission of a job that causes an exception should succeed,
        even with a (good) callback, but we should get a logged
        exception as a result.

        """

        def _callback(future):
            future.result()

        asynchronizer = Asynchronizer(
            name='TestCallbackExceptionAsynchronizer',
            executor=self.executor,
            callback=_callback,
        )

        # Submit a bad job
        logger_name = 'encore.concurrent.futures.abc_work_scheduler'
        with loghandler(logger_name) as handler:
            asynchronizer.submit(operator.floordiv, 1, 0)
            asynchronizer.wait()

        # We log two messages for each failure. The actual traceback
        # from the worker, and the exception of where it occurred
        # (i.e. where the result was accessed)
        self.assertEqual(len(handler.records), 2)
        record = handler.records[0]
        self.assertIsNotNone(record.exc_info)
        exc_type, exc_value, exc_tb = record.exc_info
        self.assertIs(exc_type, ZeroDivisionError)

        asynchronizer.shutdown()

    def test_submit_job_with_raising_callback(self):
        """
        Submission of a job with a raising callback should detect
        the exception in the callback.

        """

        def _callback(future):
            raise _TestException('Failing callback')

        asynchronizer = Asynchronizer(
            name='TestCallbackExceptionAsynchronizer',
            executor=self.executor,
            callback=_callback,
        )

        # Submit a good job
        logger_name = 'encore.concurrent.futures.abc_work_scheduler'
        with loghandler(logger_name) as handler:
            asynchronizer.submit(operator.add, 1, 0)
            asynchronizer.wait()

        # We log two messages for each failure. The actual traceback
        # from the worker, and the exception of where it occurred
        # (i.e. where the result was accessed)
        self.assertEqual(len(handler.records), 2)
        record = handler.records[0]
        self.assertIsNotNone(record.exc_info)
        exc_type, exc_value, exc_tb = record.exc_info
        self.assertIs(exc_type, _TestException)

        # Submit a bad job
        with loghandler(logger_name) as handler:
            asynchronizer.submit(operator.floordiv, 1, 0)
            asynchronizer.wait()

        # We log two messages for each failure. The actual traceback
        # from the worker, and the exception of where it occurred
        # (i.e. where the result was accessed)
        self.assertEqual(len(handler.records), 2)
        record = handler.records[0]
        self.assertIsNotNone(record.exc_info)
        exc_type, exc_value, exc_tb = record.exc_info
        self.assertIs(exc_type, _TestException)

        asynchronizer.shutdown()

    def tearDown(self):
        self.asynchronizer.shutdown()
        del self.asynchronizer
        self.executor.shutdown()
        del self.executor


if __name__ == '__main__':
    unittest.main()
