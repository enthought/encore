import unittest

from encore.concurrent.futures.synchronous import SynchronousExecutor


def mul(x, y):
    return x * y


class TestSynchronousExecutor(unittest.TestCase):
    def setUp(self):
        self.executor = SynchronousExecutor()

    def tearDown(self):
        self.executor.shutdown(wait=True)

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

    def test_run_after_shutdown(self):
        self.executor.shutdown()
        with self.assertRaises(RuntimeError):
            self.executor.submit(pow, 2, 5)


if __name__ == '__main__':
    unittest.main()
