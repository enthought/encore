#
# (C) Copyright 2013 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
import contextlib
import logging
import operator
import Queue
import time
import unittest

from ..asynchronizer import Asynchronizer
from ..enhanced_thread_pool_executor import EnhancedThreadPoolExecutor


def _worker(queue, value):
    time.sleep(0.25)
    queue.put(value)
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
    old_propagate_value = logger.propagate
    logger.propagate = False
    logger.addHandler(handler)
    yield handler
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
        queue = Queue.Queue()
        self.asynchronizer.submit(_worker, queue, 1)
        self.asynchronizer.submit(_worker, queue, 2)
        self.asynchronizer.submit(_worker, queue, 3)
        self.asynchronizer.submit(_worker, queue, 4)
        self.asynchronizer.submit(_worker, queue, 5)
        self.asynchronizer.submit(_worker, queue, 6)
        self.asynchronizer.submit(_worker, queue, 7)
        self.asynchronizer.submit(_worker, queue, 8)
        self.asynchronizer.submit(_worker, queue, 9)
        self.asynchronizer.submit(_worker, queue, 10)
        self.asynchronizer.wait()
        self.assertEqual(queue.qsize(), 2)
        self.assertEqual(queue.get(), 1)
        self.assertEqual(queue.get(), 10)

    def test_callback(self):
        # Make a callback that repeats the insertion into another queue.
        callback_queue = Queue.Queue()

        def _callback(future):
            value = future.result()
            callback_queue.put(value)

        asynchronizer = Asynchronizer(
            name='TestCallbackAsynchronizer',
            executor=self.executor,
            callback=_callback,
        )

        queue = Queue.Queue()
        asynchronizer.submit(_worker, queue, 1)
        asynchronizer.submit(_worker, queue, 2)
        asynchronizer.submit(_worker, queue, 3)
        asynchronizer.submit(_worker, queue, 4)
        asynchronizer.submit(_worker, queue, 5)
        asynchronizer.submit(_worker, queue, 6)
        asynchronizer.submit(_worker, queue, 7)
        asynchronizer.submit(_worker, queue, 8)
        asynchronizer.submit(_worker, queue, 9)
        asynchronizer.submit(_worker, queue, 10)
        asynchronizer.wait()

        # Test that the queues contain the same contents.
        self.assertEqual(queue.qsize(), 2)
        self.assertEqual(queue.get(), 1)
        self.assertEqual(queue.get(), 10)
        self.assertEqual(callback_queue.qsize(), 2)
        self.assertEqual(callback_queue.get(), 1)
        self.assertEqual(callback_queue.get(), 10)

        asynchronizer.shutdown()

    def test_private_executor_shutdown(self):
        asynchronizer = Asynchronizer()
        asynchronizer.submit(lambda: None)
        self.assertTrue(
            all(t.isAlive() for t in asynchronizer._executor._threads))
        asynchronizer.shutdown()
        self.assertFalse(
            all(t.isAlive() for t in asynchronizer._executor._threads))

    def test_public_executor_shutdown(self):
        self.asynchronizer.submit(lambda: None)
        self.assertTrue(
            all(t.isAlive() for t in self.asynchronizer._executor._threads))
        self.asynchronizer.shutdown()
        self.assertTrue(
            all(t.isAlive() for t in self.asynchronizer._executor._threads))

    def test_asynchronizer_name(self):
        asynchronizer = Asynchronizer(name="Will")
        self.assertEqual(asynchronizer.name, "Will")
        self.assertEqual(asynchronizer._executor.name, "WillExecutor")

    def test_submit_after_shutdown(self):
        self.asynchronizer.shutdown()
        with self.assertRaises(RuntimeError):
            self.asynchronizer.submit(lambda: None)

    def test_submit_after_shutdown_private_executor(self):
        asynchronizer = Asynchronizer()
        asynchronizer.shutdown()
        with self.assertRaises(RuntimeError):
            asynchronizer.submit(lambda: None)

    def test_submit_bad_job(self):
        """
        Submission of a job that causes an exception should succeed,
        but we should get a logged exception as a result.

        """
        with loghandler('encore.concurrent.futures.asynchronizer') as handler:
            self.asynchronizer.submit(operator.div, 1, 0)
            self.asynchronizer.wait()

        self.assertEqual(len(handler.records), 1)
        record = handler.records[0]
        self.assertIsNotNone(record.exc_info)
        exc_type, exc_value, exc_tb = record.exc_info
        self.assertIs(exc_type, ZeroDivisionError)

    def test_submit_bad_job_with_callback(self):
        """
        Submission of a job with a raising callback should detect the
        exception in the callback.

        """

        def _callback(future):
            future.result()

        asynchronizer = Asynchronizer(
            name='TestCallbackExceptionAsynchronizer',
            executor=self.executor,
            callback=_callback,
        )

        # Submit a bad job
        with loghandler('encore.concurrent.futures.asynchronizer') as handler:
            asynchronizer.submit(operator.div, 1, 0)
            asynchronizer.wait()

        self.assertEqual(len(handler.records), 1)
        record = handler.records[0]
        self.assertIsNotNone(record.exc_info)
        exc_type, exc_value, exc_tb = record.exc_info
        self.assertIs(exc_type, ZeroDivisionError)

        asynchronizer.shutdown()

    def test_submit_job_with_raising_callback(self):
        """
        Submission of a job with a raising callback should detect the exception
        in the callback.

        """

        def _callback(future):
            raise _TestException('Failing callback')

        asynchronizer = Asynchronizer(
            name='TestCallbackExceptionAsynchronizer',
            executor=self.executor,
            callback=_callback,
        )

        # Submit a good job
        with loghandler('encore.concurrent.futures.asynchronizer') as handler:
            asynchronizer.submit(operator.add, 1, 0)
            asynchronizer.wait()

        self.assertEqual(len(handler.records), 1)
        record = handler.records[0]
        self.assertIsNotNone(record.exc_info)
        exc_type, exc_value, exc_tb = record.exc_info
        self.assertIs(exc_type, _TestException)

        # Submit a bad job
        with loghandler('encore.concurrent.futures.asynchronizer') as handler:
            asynchronizer.submit(operator.div, 1, 0)
            asynchronizer.wait()

        self.assertEqual(len(handler.records), 1)
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
