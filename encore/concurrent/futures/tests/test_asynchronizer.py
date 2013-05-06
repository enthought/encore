#
# (C) Copyright 2013 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
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


class TestHandler(logging.Handler):
    """
    Simple logging handler that just accumulates and stores records.

    """
    def __init__(self):
        logging.Handler.__init__(self)
        self.records = []

    def emit(self, record):
        self.records.append(record)


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
        # Submission of a job that causes an exception should succeed,
        # but we should get a logged exception as a result.
        handler = TestHandler()

        logger = logging.getLogger('encore.concurrent.futures.asynchronizer')
        old_propagate_value = logger.propagate

        logger.propagate = False
        logger.addHandler(handler)
        try:
            self.asynchronizer.submit(operator.div, 1, 0)
            self.asynchronizer.wait()
        finally:
            logger.removeHandler(handler)
            logger.propagate = old_propagate_value

        self.assertEqual(len(handler.records), 1)
        record = handler.records[0]
        self.assertIsNotNone(record.exc_info)
        exc_type, exc_value, exc_tb = record.exc_info
        self.assertIs(exc_type, ZeroDivisionError)

    def tearDown(self):
        self.asynchronizer.shutdown()
        del self.asynchronizer
        self.executor.shutdown()
        del self.executor
