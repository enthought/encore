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

from encore.concurrent.futures.serializing_asynchronizer import (
    SerializingAsynchronizer)
from encore.concurrent.futures.enhanced_thread_pool_executor import (
    EnhancedThreadPoolExecutor)


class Worker(object):

    def __init__(self):
        self.data = []

    def __call__(self, value):
        time.sleep(0.25)
        self.data.append(value)
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
    old_level = logger.level
    logger.setLevel(logging.DEBUG)
    old_propagate_value = logger.propagate
    logger.propagate = False
    logger.addHandler(handler)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.propagate = old_propagate_value
        logger.setLevel(old_level)


class TestException(Exception):
    pass


class TestSerializingAsynchronizer(unittest.TestCase):

    def setUp(self):
        self.executor = EnhancedThreadPoolExecutor(
            name='TestSerializingAsynchronizerExecutor',
            max_workers=1,
        )
        self.asynchronizer = SerializingAsynchronizer(
            name='TestSerializingAsynchronizer',
            executor=self.executor,
        )

    def test_events_collapsed(self):
        worker1 = Worker()
        worker2 = Worker()
        self.asynchronizer.submit(worker1, 1)
        self.asynchronizer.submit(worker1, 2)
        self.asynchronizer.submit(worker1, 3)
        self.asynchronizer.submit(worker1, 4)
        self.asynchronizer.submit(worker1, 5)
        self.asynchronizer.submit(worker1, 6)
        self.asynchronizer.submit(worker1, 7)
        self.asynchronizer.submit(worker1, 8)
        self.asynchronizer.submit(worker1, 9)
        self.asynchronizer.submit(worker1, 10)

        self.asynchronizer.submit(worker2, 11)
        self.asynchronizer.submit(worker2, 12)
        self.asynchronizer.submit(worker2, 13)
        self.asynchronizer.submit(worker2, 14)
        self.asynchronizer.submit(worker2, 15)
        self.asynchronizer.submit(worker2, 16)
        self.asynchronizer.submit(worker2, 17)
        self.asynchronizer.submit(worker2, 18)
        self.asynchronizer.submit(worker2, 19)
        self.asynchronizer.submit(worker2, 20)

        self.asynchronizer.wait()
        self.assertEqual(len(worker1.data), 2)
        self.assertEqual(worker1.data[0], 1)
        self.assertEqual(worker1.data[1], 10)

        self.asynchronizer.wait()
        self.assertEqual(len(worker2.data), 1)
        self.assertEqual(worker2.data, [20])

    def test_callback(self):
        # Make a callback that repeats the insertion into another queue.
        callback_numbers = []

        def _callback(future):
            value = future.result()
            callback_numbers.append(value)

        asynchronizer = SerializingAsynchronizer(
            name='TestCallbackSerializingAsynchronizer',
            executor=self.executor,
            callback=_callback
        )

        worker1 = Worker()
        worker2 = Worker()

        asynchronizer.submit(worker1, 1)
        asynchronizer.submit(worker1, 2)
        asynchronizer.submit(worker1, 3)
        asynchronizer.submit(worker1, 4)
        asynchronizer.submit(worker1, 5)
        asynchronizer.submit(worker1, 6)
        asynchronizer.submit(worker1, 7)
        asynchronizer.submit(worker1, 8)
        asynchronizer.submit(worker1, 9)
        asynchronizer.submit(worker1, 10)

        asynchronizer.submit(worker2, 11)
        asynchronizer.submit(worker2, 12)
        asynchronizer.submit(worker2, 13)
        asynchronizer.submit(worker2, 14)
        asynchronizer.submit(worker2, 15)
        asynchronizer.submit(worker2, 16)
        asynchronizer.submit(worker2, 17)
        asynchronizer.submit(worker2, 18)
        asynchronizer.submit(worker2, 19)
        asynchronizer.submit(worker2, 20)
        asynchronizer.wait()

        self.assertEqual(len(worker1.data), 2)
        self.assertEqual(worker1.data[0], 1)
        self.assertEqual(worker1.data[1], 10)

        self.assertEqual(len(worker2.data), 1)
        self.assertEqual(worker2.data, [20])

        self.assertEqual(len(callback_numbers), 3)
        self.assertEqual(callback_numbers[0], 1)
        self.assertEqual(callback_numbers[1], 10)
        self.assertEqual(callback_numbers[2], 20)
        asynchronizer.shutdown()

    def test_asynchronizer_name(self):
        asynchronizer = SerializingAsynchronizer(
            executor=self.executor, name="Will")
        self.assertEqual(asynchronizer.name, "Will")
        self.assertEqual(
            asynchronizer._executor.name,
            'TestSerializingAsynchronizerExecutor')

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

        asynchronizer = SerializingAsynchronizer(
            name='TestCallbackExceptionSerializingAsynchronizer',
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
            raise TestException('Failing callback')

        asynchronizer = SerializingAsynchronizer(
            name='TestCallbackExceptionSerializingAsynchronizer',
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
        self.assertIs(exc_type, TestException)

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
        self.assertIs(exc_type, TestException)

        asynchronizer.shutdown()

    def tearDown(self):
        self.asynchronizer.shutdown()
        del self.asynchronizer
        self.executor.shutdown()
        del self.executor


if __name__ == '__main__':
    unittest.main()
