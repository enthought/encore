#
# (C) Copyright 2014 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
import unittest

from encore.concurrent.futures.decorators import dispatch


class TestDispatcher(object):

    def __init__(self):
        self.calls = []

    def submit(self, func, *args, **kw):
        # Drop 'self' from args
        self.calls.append((args[1:], kw))
        func(*args, **kw)

    def __call__(self, func, *args, **kw):
        # Drop 'self' from args
        self.calls.append((args[1:], kw))
        func(*args, **kw)


TEST_DISPATCHER = TestDispatcher()


class TestClass(object):

    def __init__(self):
        self.dispatcher = TestDispatcher()
        self.calls = []

    @dispatch(dispatcher=TEST_DISPATCHER)
    def dispatcher_wrapped(self, *args, **kw):
        self.calls.append((args, kw))

    @dispatch(dispatcher="dispatcher")
    def dispatcher_string_wrapped(self, *args, **kw):
        self.calls.append((args, kw))

    @dispatch(call=TEST_DISPATCHER)
    def call_wrapped(self, *args, **kw):
        self.calls.append((args, kw))

    @dispatch(call="dispatcher")
    def call_string_wrapped(self, *args, **kw):
        self.calls.append((args, kw))


class TestDispatch(unittest.TestCase):

    def setUp(self):
        self.obj = TestClass()
        self.calls = [
            ((1, 2), {"c": 3, "d": 4}),
            ((5, 6), {"e": 7, "f": 8})
        ]
        TEST_DISPATCHER.calls = []

    def _make_calls(self, bound_method):
        for args, kw in self.calls:
            bound_method(*args, **kw)

    def test_dispatcher(self):
        self._make_calls(self.obj.dispatcher_wrapped)
        self.assertEqual(self.obj.calls, TEST_DISPATCHER.calls)
        self.assertEqual(self.obj.calls, self.calls)

    def test_dispatcher_string(self):
        self._make_calls(self.obj.dispatcher_string_wrapped)
        self.assertEqual(self.calls, self.obj.dispatcher.calls)
        self.assertEqual(self.obj.calls, self.calls)

    def test_call(self):
        self._make_calls(self.obj.call_wrapped)
        self.assertEqual(self.calls, TEST_DISPATCHER.calls)
        self.assertEqual(self.obj.calls, self.calls)

    def test_call_string(self):
        self._make_calls(self.obj.call_string_wrapped)
        self.assertEqual(self.calls, self.obj.dispatcher.calls)
        self.assertEqual(self.obj.calls, self.calls)

    def test_wrong_args(self):
        self.assertRaises(ValueError, dispatch)
        self.assertRaises(ValueError, dispatch, 'foo', 'bar')


if __name__ == '__main__':
    unittest.main()
