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

from encore.concurrent.futures.serializer import Serializer
from encore.concurrent.futures.enhanced_thread_pool_executor import (
    EnhancedThreadPoolExecutor)


def _worker(data, value):
    time.sleep(0.1)
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


class TestSerializer(unittest.TestCase):

    def setUp(self):
        self.executor = EnhancedThreadPoolExecutor(
            name='TestSerializerExecutor',
            max_workers=1)
        self.serializer = Serializer(
            name='TestSerializer',
            executor=self.executor,
        )

    def test_events_serialized(self):
        numbers = []
        self.serializer.submit(_worker, numbers, 1)
        self.serializer.submit(_worker, numbers, 2)
        self.serializer.submit(_worker, numbers, 3)
        self.serializer.submit(_worker, numbers, 4)
        self.serializer.submit(_worker, numbers, 5)
        self.serializer.submit(_worker, numbers, 6)
        self.serializer.submit(_worker, numbers, 7)
        self.serializer.submit(_worker, numbers, 8)
        self.serializer.submit(_worker, numbers, 9)
        self.serializer.submit(_worker, numbers, 10)
        self.serializer.wait()
        self.assertEqual(len(numbers), 10)
        self.assertEqual(numbers, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    def test_callback(self):
        # Make a callback that repeats the insertion into another queue.
        callback_numbers = []

        def _callback(future):
            value = future.result()
            callback_numbers.append(value)

        serializer = Serializer(
            name='TestCallbackSerializer',
            executor=self.executor,
            callback=_callback
        )

        numbers = []
        serializer.submit(_worker, numbers, 1)
        serializer.submit(_worker, numbers, 2)
        serializer.submit(_worker, numbers, 3)
        serializer.submit(_worker, numbers, 4)
        serializer.submit(_worker, numbers, 5)
        serializer.submit(_worker, numbers, 6)
        serializer.submit(_worker, numbers, 7)
        serializer.submit(_worker, numbers, 8)
        serializer.submit(_worker, numbers, 9)
        serializer.submit(_worker, numbers, 10)
        serializer.wait()
        self.assertEqual(len(numbers), 10)
        self.assertEqual(numbers, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        self.assertEqual(len(callback_numbers), 10)
        self.assertEqual(callback_numbers, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        serializer.shutdown()

    def test_serializer_name(self):
        serializer = Serializer(executor=self.executor, name="Will")
        self.assertEqual(serializer.name, "Will")
        self.assertEqual(
            serializer._executor.name,
            'TestSerializerExecutor')

    def test_submit_after_shutdown(self):
        self.serializer.shutdown()
        with self.assertRaises(RuntimeError):
            self.serializer.submit(lambda: None)

    def test_submit_bad_job(self):
        """
        Submission of a job that causes an exception should succeed,
        but we should get a logged exception as a result.

        """
        logger_name = 'encore.concurrent.futures.abc_work_scheduler'
        with loghandler(logger_name) as handler:
            self.serializer.submit(operator.floordiv, 1, 0)
            self.serializer.wait()

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

        serializer = Serializer(
            name='TestCallbackExceptionSerializer',
            executor=self.executor,
            callback=_callback,
        )

        # Submit a bad job
        logger_name = 'encore.concurrent.futures.abc_work_scheduler'
        with loghandler(logger_name) as handler:
            serializer.submit(operator.floordiv, 1, 0)
            serializer.wait()

        # We log two messages for each failure. The actual traceback
        # from the worker, and the exception of where it occurred
        # (i.e. where the result was accessed)
        self.assertEqual(len(handler.records), 2)
        record = handler.records[0]
        self.assertIsNotNone(record.exc_info)
        exc_type, exc_value, exc_tb = record.exc_info
        self.assertIs(exc_type, ZeroDivisionError)

        serializer.shutdown()

    def test_submit_job_with_raising_callback(self):
        """
        Submission of a job with a raising callback should detect
        the exception in the callback.

        """

        def _callback(future):
            raise _TestException('Failing callback')

        serializer = Serializer(
            name='TestCallbackExceptionSerializer',
            executor=self.executor,
            callback=_callback,
        )

        # Submit a good job
        logger_name = 'encore.concurrent.futures.abc_work_scheduler'
        with loghandler(logger_name) as handler:
            serializer.submit(operator.add, 1, 0)
            serializer.wait()

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
            serializer.submit(operator.floordiv, 1, 0)
            serializer.wait()

        # We log two messages for each failure. The actual traceback
        # from the worker, and the exception of where it occurred
        # (i.e. where the result was accessed)
        self.assertEqual(len(handler.records), 2)
        record = handler.records[0]
        self.assertIsNotNone(record.exc_info)
        exc_type, exc_value, exc_tb = record.exc_info
        self.assertIs(exc_type, _TestException)

        serializer.shutdown()

    def tearDown(self):
        self.serializer.shutdown()
        del self.serializer
        self.executor.shutdown()
        del self.executor


if __name__ == '__main__':
    unittest.main()
