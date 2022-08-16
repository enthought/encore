# Portions of this code are taken from the Python source distribution, which is
# subject to the PSF license. See http://docs.python.org/2/license.html.

import collections
import threading
import time
import unittest
import weakref

from concurrent import futures
from concurrent.futures._base import (
    PENDING, RUNNING, CANCELLED, CANCELLED_AND_NOTIFIED, FINISHED, Future)

from encore.concurrent.futures.enhanced_thread_pool_executor import \
        EnhancedThreadPoolExecutor
from encore.testing.asserts import assert_python_ok


def create_future(state=PENDING, exception=None, result=None):
    f = Future()
    f._state = state
    f._exception = exception
    f._result = result
    return f

PENDING_FUTURE = create_future(state=PENDING)
RUNNING_FUTURE = create_future(state=RUNNING)
CANCELLED_FUTURE = create_future(state=CANCELLED)
CANCELLED_AND_NOTIFIED_FUTURE = create_future(state=CANCELLED_AND_NOTIFIED)
EXCEPTION_FUTURE = create_future(state=FINISHED, exception=OSError())
SUCCESSFUL_FUTURE = create_future(state=FINISHED, result=42)


def mul(x, y):
    return x * y


def sleep_and_raise(t):
    time.sleep(t)
    raise Exception('this is an exception')


class MyObject(object):
    def my_method(self):
        pass


class ExecutorMixin(object):
    worker_count = 5

    def setUp(self):
        self.t1 = time.time()
        try:
            self.executor = self.executor_type(max_workers=self.worker_count)
        except NotImplementedError as e:
            self.skipTest(str(e))
        self._prime_executor()

    def tearDown(self):
        self.executor.shutdown(wait=True)
        dt = time.time() - self.t1
        self.assertLess(dt, 60, "synchronization issue: test lasted too long")

    def _prime_executor(self):
        # Make sure that the executor is ready to do work before running the
        # tests. This should reduce the probability of timeouts in the tests.
        futures_ = [self.executor.submit(time.sleep, 0.1)
                   for _ in range(self.worker_count)]

        for f in futures_:
            f.result()


class EnhancedThreadPoolMixin(ExecutorMixin):
    executor_type = EnhancedThreadPoolExecutor


class CustomFuture(Future):
    pass


class CustomThreadPoolExecutor(EnhancedThreadPoolExecutor):
    _future_factory = CustomFuture


class EnhancedThreadPoolShutdownTest(EnhancedThreadPoolMixin, unittest.TestCase):
    def _prime_executor(self):
        pass

    def test_threads_terminate(self):
        self.executor.submit(mul, 21, 2)
        self.executor.submit(mul, 6, 7)
        self.executor.submit(mul, 3, 14)
        self.assertEqual(len(self.executor._threads), 3)
        self.executor.shutdown()
        for t in self.executor._threads:
            t.join()

    def test_context_manager_shutdown(self):
        with EnhancedThreadPoolExecutor(max_workers=5) as e:
            executor = e
            self.assertEqual(list(e.map(abs, range(-5, 5))),
                             [5, 4, 3, 2, 1, 0, 1, 2, 3, 4])

        for t in executor._threads:
            t.join()

    def test_del_shutdown(self):
        executor = EnhancedThreadPoolExecutor(max_workers=5)
        executor.map(abs, range(-5, 5))
        threads = executor._threads
        del executor

        for t in threads:
            t.join()

    def test_run_after_shutdown(self):
        self.executor.shutdown()
        self.assertRaises(RuntimeError,
                          self.executor.submit,
                          pow, 2, 5)

    def test_interpreter_shutdown(self):
        # Test the atexit hook for shutdown of worker threads and processes
        rc, out, err = assert_python_ok('-c', """\
if 1:
    from encore.concurrent.futures.enhanced_thread_pool_executor import EnhancedThreadPoolExecutor
    from time import sleep
    import sys
    def sleep_and_print(t, msg):
        sleep(t)
        print(msg)
        sys.stdout.flush()
    t = {executor_type}(5)
    t.submit(sleep_and_print, 1.0, "apple")
            """.format(executor_type=self.executor_type.__name__))
        # Errors in atexit hooks don't change the process exit code, check
        # stderr manually.
        self.assertFalse(err)
        self.assertEqual(out.strip(), b"apple")

    def test_hang_issue12364(self):
        fs = [self.executor.submit(time.sleep, 0.1) for _ in range(50)]
        self.executor.shutdown()
        for f in fs:
            f.result()


class EnhancedThreadPoolWaitTests(EnhancedThreadPoolMixin, unittest.TestCase):

    def test_pending_calls_race(self):
        # Issue #14406: multi-threaded race condition when waiting on all
        # futures.
        event = threading.Event()

        def future_func():
            event.wait()

        fs = {self.executor.submit(future_func) for i in range(100)}
        event.set()
        futures.wait(fs, return_when=futures.ALL_COMPLETED)

    def test_first_completed(self):
        future1 = self.executor.submit(mul, 21, 2)
        future2 = self.executor.submit(time.sleep, 1.5)

        done, not_done = futures.wait(
                [CANCELLED_FUTURE, future1, future2],
                 return_when=futures.FIRST_COMPLETED)

        self.assertEqual(set([future1]), done)
        self.assertEqual(set([CANCELLED_FUTURE, future2]), not_done)

    def test_first_completed_some_already_completed(self):
        future1 = self.executor.submit(time.sleep, 1.5)

        finished, pending = futures.wait(
                 [CANCELLED_AND_NOTIFIED_FUTURE, SUCCESSFUL_FUTURE, future1],
                 return_when=futures.FIRST_COMPLETED)

        self.assertEqual(
                set([CANCELLED_AND_NOTIFIED_FUTURE, SUCCESSFUL_FUTURE]),
                finished)
        self.assertEqual(set([future1]), pending)

    def test_first_exception(self):
        future1 = self.executor.submit(mul, 2, 21)
        future2 = self.executor.submit(sleep_and_raise, 1.5)
        future3 = self.executor.submit(time.sleep, 3)

        finished, pending = futures.wait(
                [future1, future2, future3],
                return_when=futures.FIRST_EXCEPTION)

        self.assertEqual(set([future1, future2]), finished)
        self.assertEqual(set([future3]), pending)

    def test_first_exception_some_already_complete(self):
        future1 = self.executor.submit(divmod, 21, 0)
        future2 = self.executor.submit(time.sleep, 1.5)

        finished, pending = futures.wait(
                [SUCCESSFUL_FUTURE,
                 CANCELLED_FUTURE,
                 CANCELLED_AND_NOTIFIED_FUTURE,
                 future1, future2],
                return_when=futures.FIRST_EXCEPTION)

        self.assertEqual(set([SUCCESSFUL_FUTURE,
                              CANCELLED_AND_NOTIFIED_FUTURE,
                              future1]), finished)
        self.assertEqual(set([CANCELLED_FUTURE, future2]), pending)

    def test_first_exception_one_already_failed(self):
        future1 = self.executor.submit(time.sleep, 2)

        finished, pending = futures.wait(
                 [EXCEPTION_FUTURE, future1],
                 return_when=futures.FIRST_EXCEPTION)

        self.assertEqual(set([EXCEPTION_FUTURE]), finished)
        self.assertEqual(set([future1]), pending)

    def test_all_completed(self):
        future1 = self.executor.submit(divmod, 2, 0)
        future2 = self.executor.submit(mul, 2, 21)

        finished, pending = futures.wait(
                [SUCCESSFUL_FUTURE,
                 CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 future1,
                 future2],
                return_when=futures.ALL_COMPLETED)

        self.assertEqual(set([SUCCESSFUL_FUTURE,
                              CANCELLED_AND_NOTIFIED_FUTURE,
                              EXCEPTION_FUTURE,
                              future1,
                              future2]), finished)
        self.assertEqual(set(), pending)

    def test_timeout(self):
        future1 = self.executor.submit(mul, 6, 7)
        future2 = self.executor.submit(time.sleep, 6)

        finished, pending = futures.wait(
                [CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 SUCCESSFUL_FUTURE,
                 future1, future2],
                timeout=5,
                return_when=futures.ALL_COMPLETED)

        self.assertEqual(set([CANCELLED_AND_NOTIFIED_FUTURE,
                              EXCEPTION_FUTURE,
                              SUCCESSFUL_FUTURE,
                              future1]), finished)
        self.assertEqual(set([future2]), pending)


class EnhancedThreadPoolAsCompletedTests(EnhancedThreadPoolMixin, unittest.TestCase):

    def test_no_timeout(self):
        future1 = self.executor.submit(mul, 2, 21)
        future2 = self.executor.submit(mul, 7, 6)

        completed = set(futures.as_completed(
                [CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 SUCCESSFUL_FUTURE,
                 future1, future2]))
        self.assertEqual(set(
                [CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 SUCCESSFUL_FUTURE,
                 future1, future2]),
                completed)

    def test_zero_timeout(self):
        future1 = self.executor.submit(time.sleep, 2)
        completed_futures = set()
        try:
            for future in futures.as_completed(
                    [CANCELLED_AND_NOTIFIED_FUTURE,
                     EXCEPTION_FUTURE,
                     SUCCESSFUL_FUTURE,
                     future1],
                    timeout=0):
                completed_futures.add(future)
        except futures.TimeoutError:
            pass

        self.assertEqual(set([CANCELLED_AND_NOTIFIED_FUTURE,
                              EXCEPTION_FUTURE,
                              SUCCESSFUL_FUTURE]),
                         completed_futures)


class EnhancedThreadPoolExecutorTest(EnhancedThreadPoolMixin, unittest.TestCase):

    def test_map_submits_without_iteration(self):
        """Tests verifying issue 11777."""
        self.finished = set()

        def record_finished(n):
            self.finished.add(n)

        self.executor.map(record_finished, range(10))
        self.executor.shutdown(wait=True)
        self.assertEqual(self.finished, set(range(10)))

    def test_submit(self):
        future = self.executor.submit(pow, 2, 8)
        self.assertEqual(256, future.result())

    def test_submit_keyword(self):
        future = self.executor.submit(mul, 2, y=8)
        self.assertEqual(16, future.result())

    def test_map(self):
        self.assertEqual(
                list(self.executor.map(pow, range(10), range(10))),
                list(map(pow, range(10), range(10))))

    def test_map_exception(self):
        i = self.executor.map(divmod, [1, 1, 1, 1], [2, 3, 0, 5])
        self.assertEqual(next(i), (0, 1))
        self.assertEqual(next(i), (0, 1))
        with self.assertRaises(ZeroDivisionError):
            next(i)

    def test_map_timeout(self):
        results = []
        try:
            for i in self.executor.map(time.sleep,
                                       [0, 0, 6],
                                       timeout=5):
                results.append(i)
        except futures.TimeoutError:
            pass
        else:
            self.fail('expected TimeoutError')

        self.assertEqual([None, None], results)

    def test_shutdown_race_issue12456(self):
        # Issue #12456: race condition at shutdown where trying to post a
        # sentinel in the call queue blocks (the queue is full while processes
        # have exited).
        self.executor.map(str, [2] * (self.worker_count + 1))
        self.executor.shutdown()

    def test_no_stale_references(self):
        # Issue #16284: check that the executors don't unnecessarily hang onto
        # references.
        my_object = MyObject()
        my_object_collected = threading.Event()
        my_object_callback = weakref.ref(
            my_object, lambda obj: my_object_collected.set())
        # Deliberately discarding the future.
        self.executor.submit(my_object.my_method)
        del my_object

        collected = my_object_collected.wait(timeout=5.0)
        self.assertTrue(collected,
                        "Stale reference not collected within timeout.")

    def test_name(self):
        # Unnamed executor.
        executor = EnhancedThreadPoolExecutor(max_workers=5)
        with executor as e:
            # Start some threads.
            for _ in range(10):
                e.submit(pow, 2, 2)
            expected_prefix = "{0}Worker-".format(type(e).__name__)
            for t in e._threads:
                self.assertTrue(t.name.startswith(expected_prefix))
            # Workers should all have different names.
            thread_names = [t.name for t in e._threads]
            self.assertEqual(
                len(set(thread_names)),
                len(thread_names),
                msg="Threads don't have unique names: {0}.".format(
                    thread_names))

        # Executor with explicit name.
        executor = EnhancedThreadPoolExecutor(max_workers=5,
                                              name="DeadParrotExecutioner")
        with executor as e:
            self.assertEqual(e.name, "DeadParrotExecutioner")
            # Start some threads.
            for _ in range(10):
                e.submit(pow, 2, 2)
            expected_prefix = "DeadParrotExecutionerWorker-"
            for t in e._threads:
                self.assertTrue(t.name.startswith(expected_prefix))
            # Workers should all have different names.
            thread_names = [t.name for t in e._threads]
            self.assertEqual(
                len(set(thread_names)),
                len(thread_names),
                msg="Threads don't have unique names: {0}.".format(
                    thread_names))

    def test_retrieve_real_traceback(self):
        future = self.executor.submit(divmod, 1, 0)
        self.assertRaises(ZeroDivisionError, future.result)
        traceback = future.traceback()
        # We don't want to see a traceback of retrieving the result, but
        # of where it occurred in the worker.
        # This is concurrent\futures\_base.py", line 357
        invalid = 'return self.__get_result()'
        self.assertTrue(invalid not in traceback)
        self.assertTrue('ZeroDivisionError' in traceback)


class EnhancedThreadPoolExecutorInitUninit(unittest.TestCase):

    def test_initialize(self):
        num_workers = 5
        self.artifacts = []

        def initialize():
            self.artifacts.append(None)

        executor = EnhancedThreadPoolExecutor(num_workers, initializer=initialize)

        _start_all_threads(executor, num_workers)

        self.assertEqual([None] * num_workers, self.artifacts)
        executor.shutdown()

    def test_uninitialize(self):
        num_workers = 5
        self.artifacts = []

        def uninitialize():
            self.artifacts.append(None)

        executor = EnhancedThreadPoolExecutor(num_workers, uninitializer=uninitialize)

        _start_all_threads(executor, num_workers)

        executor.shutdown(wait=True)
        self.assertEqual([None] * num_workers, self.artifacts)


class EnhancedThreadPoolWaitAtExit(unittest.TestCase):
    """ Test whether the `wait_at_exit` argument is honored. """

    def _execute(self, wait=False):
        code = """if 1: # For indentation
            import os, sys, time
            from encore.concurrent.futures.enhanced_thread_pool_executor \
                    import EnhancedThreadPoolExecutor

            def job():
                time.sleep(1)
                sys.stdout.write('FINISHED')
                sys.stdout.flush()

            executor = EnhancedThreadPoolExecutor(max_workers=4,
                            wait_at_exit=int(os.environ['SHOULD_WAIT']))
            executor.submit(job)
        """
        return assert_python_ok('-c', code, SHOULD_WAIT=str(int(wait)))

    def test_no_wait_at_exit(self):
        rc, out, err = self._execute(False)
        self.assertEqual(out, b'')

    def test_wait_at_exit(self):
        rc, out, err = self._execute(True)
        self.assertEqual(out, b'FINISHED')


class TestCustomFuture(unittest.TestCase):

    def test_custom_future(self):
        num_workers = 5
        executor = CustomThreadPoolExecutor(num_workers)
        futures_ = _start_all_threads(executor, num_workers)
        self.assertTrue(
            all(isinstance(future_, CustomFuture) for future_ in futures_)
        )


def _start_all_threads(executor, num_workers):

    counter = collections.Counter(count=0)
    lock = threading.Lock()
    futures_ = []
    for i in range(num_workers):
        future = executor.submit(
            _wait_for_counter, counter, lock, num_workers)
        futures_.append(future)
    futures.wait(futures_)
    return futures_


def _wait_for_counter(counter, lock, max_count):
    with lock:
        counter['count'] += 1
    while counter['count'] < max_count:
        time.sleep(0.05)


if __name__ == "__main__":
    unittest.main()
