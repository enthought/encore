#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

# Standard library imports.
import unittest
import unittest.mock as mock
import weakref
import threading

# Local imports.
from encore.events.event_manager import EventManager, BaseEvent
from encore.events.api import (get_event_manager, set_event_manager,
                               BaseEventManager)
import encore.events.package_globals as package_globals


class TestEventManager(unittest.TestCase):
    def setUp(self):
        self.evt_mgr = EventManager()

    def test_register(self):
        """ Test if event is successfully registered.
        """
        self.evt_mgr.register(BaseEvent)
        self.assertTrue(BaseEvent in self.evt_mgr.get_event())

    def test_emit(self):
        """ Test if events are succesfully emitted.
        """
        callback = mock.Mock()
        self.evt_mgr.connect(BaseEvent, callback)

        evt1 = BaseEvent()
        self.evt_mgr.emit(evt1)
        self.assertEqual(callback.call_count, 1)
        self.assertEqual(callback.call_args, ((evt1,), {}))

        callback2 = mock.Mock()
        self.evt_mgr.connect(BaseEvent, callback2)
        evt2 = BaseEvent()
        self.evt_mgr.emit(evt2)
        self.assertEqual(callback.call_count, 2)
        self.assertEqual(callback.call_args, ((evt2,), {}))
        self.assertEqual(callback2.call_count, 1)
        self.assertEqual(callback2.call_args, ((evt2,), {}))

        # Exceptions in listeners should still propagate events.
        def callback3(evt):
            raise RuntimeError('i\'m just like this')

        callback3 = mock.Mock(wraps=callback3)
        callback4 = mock.Mock()
        self.evt_mgr.connect(BaseEvent, callback3)
        self.evt_mgr.connect(BaseEvent, callback4)

        evt3 = BaseEvent()
        self.evt_mgr.emit(evt3)
        self.assertEqual(callback.call_count, 3)
        self.assertEqual(callback2.call_count, 2)
        self.assertEqual(callback3.call_count, 1)
        self.assertEqual(callback4.call_count, 1)

    def test_connect(self):
        """ Test if adding connections works.
        """
        callback = mock.Mock()
        self.evt_mgr.connect(BaseEvent, callback)
        self.assertEqual(list(self.evt_mgr.get_listeners(BaseEvent)),
                         [callback])

        callback2 = mock.Mock()
        self.evt_mgr.connect(BaseEvent, callback2)
        self.assertEqual(list(self.evt_mgr.get_listeners(BaseEvent)),
                         [callback, callback2])

    def test_listeners(self):
        """ Test if correct listeners are returned.
        """
        self.assertEqual(list(self.evt_mgr.get_listeners(BaseEvent)), [])

        class MyEvt(BaseEvent):
            def __init__(self, name=1):
                super(MyEvt, self).__init__()
                self.name = name
            def callback_bound(self, evt):
                pass
            def callback_unbound(self):
                pass
        callback = mock.Mock()
        obj = MyEvt()
        self.evt_mgr.connect(BaseEvent, callback)
        self.evt_mgr.connect(MyEvt, MyEvt.callback_unbound)
        self.evt_mgr.connect(MyEvt, obj.callback_bound)

        self.assertEqual(list(self.evt_mgr.get_listeners(MyEvt)),
                         [callback, MyEvt.callback_unbound, obj.callback_bound])

        callback2 = mock.Mock()
        self.evt_mgr.connect(BaseEvent, callback2, filter={'name':0})

        # get listeners with filtering
        self.assertEqual(list(self.evt_mgr.get_listeners(MyEvt(0))),
                         [callback, MyEvt.callback_unbound, obj.callback_bound,
                          callback2])

        self.assertEqual(list(self.evt_mgr.get_listeners(MyEvt(1))),
                         [callback, MyEvt.callback_unbound, obj.callback_bound])

    def test_disconnect(self):
        """ Test if disconnecting listeners works.
        """
        callback = mock.Mock()
        self.evt_mgr.connect(BaseEvent, callback)

        evt1 = BaseEvent()
        self.evt_mgr.emit(evt1)
        self.assertEqual(callback.call_count, 1)
        self.assertEqual(callback.call_args, ((evt1,), {}))

        self.evt_mgr.disconnect(BaseEvent, callback)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(callback.call_count, 1)
        self.assertEqual(callback.call_args, ((evt1,), {}))

    def test_disable(self):
        """ Test if temporarily disabling an event works.
        """
        class MyEvt(BaseEvent):
            def __init__(self):
                super(MyEvt, self).__init__()

        callback = mock.Mock()
        self.evt_mgr.connect(BaseEvent, callback)

        callback2 = mock.Mock()
        self.evt_mgr.connect(MyEvt, callback2)

        evt1 = BaseEvent()
        self.evt_mgr.emit(evt1)
        self.assertEqual(callback.call_count, 1)
        self.assertEqual(callback.call_args, ((evt1,), {}))

        # Disabling BaseEvent.
        self.evt_mgr.disable(BaseEvent)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(callback.call_count, 1)

        # Disabling BaseEvent should also disable MyEvt.
        self.evt_mgr.emit(MyEvt())
        self.assertEqual(callback.call_count, 1)
        self.assertEqual(callback2.call_count, 0)

        # Reenabling BaseEvent should fire notifications.
        self.evt_mgr.enable(BaseEvent)
        self.evt_mgr.emit(MyEvt())
        self.assertEqual(callback.call_count, 2)
        self.assertEqual(callback2.call_count, 1)

        # Disabling MyEvt should not disable BaseEvent but only MyEvt.
        self.evt_mgr.disable(MyEvt)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(callback.call_count, 3)
        self.evt_mgr.emit(MyEvt())
        self.assertEqual(callback.call_count, 3)
        self.assertEqual(callback2.call_count, 1)

        # Reenabling MyEvent should notify callback2.
        self.evt_mgr.enable(MyEvt)
        self.evt_mgr.emit(MyEvt())
        self.assertEqual(callback.call_count, 4)
        self.assertEqual(callback2.call_count, 2)

        # Test for disable before any method is registered.
        class MyEvt2(BaseEvent):
            pass

        self.evt_mgr.disable(MyEvt2)
        callback = mock.Mock()
        self.evt_mgr.connect(MyEvt2, callback)
        self.evt_mgr.emit(MyEvt2())
        self.assertFalse(callback.called)

    def test_mark_as_handled(self):
        """ Test if mark_as_handled() works.
        """
        class MyEvent(BaseEvent):
            def __init__(self, veto=False):
                super(MyEvent, self).__init__()
                self.veto = veto

        def callback(evt):
            if evt.veto:
                evt.mark_as_handled()
        callback = mock.Mock(wraps=callback)
        self.evt_mgr.connect(MyEvent, callback, priority=2)
        callback2 = mock.Mock()
        self.evt_mgr.connect(MyEvent, callback2, priority=1)

        evt1 = MyEvent()
        self.evt_mgr.emit(evt1)
        self.assertEqual(callback.call_count, 1)
        self.assertEqual(callback2.call_count, 1)

        evt2 = MyEvent(veto=True)
        self.evt_mgr.emit(evt2)
        self.assertEqual(callback.call_count, 2)
        self.assertEqual(callback.call_args, ((evt2,), {}))
        self.assertEqual(callback2.call_count, 1)
        self.assertEqual(callback2.call_args, ((evt1,), {}))

    def test_filtering(self):
        """ Test if event filtering on arguments works.
        """
        depth = 5
        class A(object):
            count = depth
            def __init__(self):
                A.count -= 1
                if A.count:
                    self.a = A()
                else:
                    self.a = 0

        class MyEvent(BaseEvent):
            def __init__(self, prop1="f0", prop2=True, prop3=None):
                super(MyEvent, self).__init__()
                self.prop1 = prop1
                self.prop2 = prop2
                self.prop3 = prop3

        callbacks = [mock.Mock() for i in range(8)]
        self.evt_mgr.connect(MyEvent, callbacks[0])
        self.evt_mgr.connect(MyEvent, callbacks[1], filter={'prop1':'f2'})
        self.evt_mgr.connect(MyEvent, callbacks[2], filter={'prop2':False})
        self.evt_mgr.connect(MyEvent, callbacks[3], filter={'prop3':BaseEvent})
        self.evt_mgr.connect(MyEvent, callbacks[4], filter={'prop1':'f2', 'prop2':False})
        self.evt_mgr.connect(MyEvent, callbacks[5], filter={'prop1.real':0})
        self.evt_mgr.connect(MyEvent, callbacks[6], filter={'prop1.a.a.a.a.a':0})
        self.evt_mgr.connect(MyEvent, callbacks[7], filter={'prop1.a.a.a.a':0})

        def check_count(evt, *counts):
            self.evt_mgr.emit(evt)
            for callback, count in zip(callbacks, counts):
                self.assertEqual(callback.call_count, count)

        # Notify only 0,1
        check_count(MyEvent(prop1='f2'), 1, 1, 0, 0, 0, 0, 0, 0)

        # Notify only 0, 1, 2, 4
        check_count(MyEvent(prop1='f2', prop2=False), 2, 2, 1, 0, 1, 0, 0, 0)

        # Notify only 0, 3
        check_count(MyEvent(prop3=BaseEvent), 3, 2, 1, 1, 1, 0, 0, 0)

        # Notify only 0; (extended filter fail on AttributeError for 5)
        check_count(MyEvent(prop1=1), 4, 2, 1, 1, 1, 0, 0, 0)

        # Notify only 0 and 5 (extended attribute filter)
        check_count(MyEvent(prop1=1j), 5, 2, 1, 1, 1, 1, 0, 0)

        # Notify only 0 and 5 (extended attribute filter)
        check_count(MyEvent(prop1=A()), 6, 2, 1, 1, 1, 1, 1, 0)

    def test_exception(self):
        """ Test if exception in handler causes subsequent notifications.
        """
        class MyEvt(BaseEvent):
            def __init__(self, err=False):
                super(MyEvt, self).__init__()
                self.err = err
        def callback(evt):
            if evt.err:
                raise Exception('you did it')
        callback = mock.Mock(wraps=callback)
        self.evt_mgr.connect(MyEvt, callback)
        callback2 = mock.Mock()
        self.evt_mgr.connect(MyEvt, callback2)

        self.evt_mgr.emit(MyEvt(err=False))
        self.assertEqual(callback.call_count, 1)
        self.assertEqual(callback2.call_count, 1)

        self.evt_mgr.emit(MyEvt(err=True))
        self.assertEqual(callback.call_count, 2)
        self.assertEqual(callback2.call_count, 2)

    def test_priority(self):
        """ Test if setting priority of handlers works.
        """
        class Callback(object):
            calls = []
            def __init__(self, name):
                self.name = name
            def __call__(self, evt):
                self.calls.append(self.name)

        callback = mock.Mock(wraps=Callback(name=1))
        self.evt_mgr.connect(BaseEvent, callback, priority=1)

        callback2 = mock.Mock(wraps=Callback(name=2))
        self.evt_mgr.connect(BaseEvent, callback2, priority=2)

        callback3 = mock.Mock(wraps=Callback(name=3))
        self.evt_mgr.connect(BaseEvent, callback3, priority=0)

        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(callback.call_count, 1)
        self.assertEqual(callback2.call_count, 1)
        self.assertEqual(callback3.call_count, 1)

        self.assertEqual(Callback.calls, [2, 1, 3])

    def test_subclass(self):
        """ Test if subclass event notifies superclass listeners.

        Cases to test:
            1. subclass event should notify superclass listeners
                even when the subclass event is not registered/connected
                even when the superclass event is added before/after subclass
            2. superclass event should not notify subclass listeners
        """
        class MyEvt(BaseEvent):
            pass

        class MyEvt2(MyEvt):
            pass

        callback = mock.Mock()
        callback2 = mock.Mock()
        self.evt_mgr.connect(MyEvt, callback)
        self.evt_mgr.connect(MyEvt2, callback2)

        # No callback called on BaseEvent
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(callback.call_count, 0)
        self.assertEqual(callback2.call_count, 0)

        # Only callback called on MyEvt
        self.evt_mgr.emit(MyEvt())
        self.assertEqual(callback.call_count, 1)
        self.assertEqual(callback2.call_count, 0)

        # Both callbacks called on MyEvt2
        self.evt_mgr.emit(MyEvt2())
        self.assertEqual(callback.call_count, 2)
        self.assertEqual(callback2.call_count, 1)

        # Add a new subclass event
        class MyEvt3(MyEvt2):
            pass

        # Subclass event not registered
        # Both callbacks called on MyEvt3
        self.evt_mgr.emit(MyEvt3())
        self.assertEqual(callback.call_count, 3)
        self.assertEqual(callback2.call_count, 2)

    def test_event_hierarchy(self):
        """ Test whether the correct hierarchy of event classes is returned.
        """
        class MyEvt(BaseEvent):
            pass
        class MyEvt2(MyEvt):
            pass
        class MyEvt3(MyEvt):
            pass
        class MyEvt4(MyEvt2, MyEvt3):
            pass

        self.assertEqual(self.evt_mgr.get_event_hierarchy(BaseEvent),
                         (BaseEvent,))
        self.assertEqual(self.evt_mgr.get_event_hierarchy(MyEvt),
                         (MyEvt, BaseEvent))
        self.assertEqual(self.evt_mgr.get_event_hierarchy(MyEvt2),
                         (MyEvt2, MyEvt, BaseEvent))
        self.assertEqual(self.evt_mgr.get_event_hierarchy(MyEvt3),
                         (MyEvt3, MyEvt, BaseEvent))
        self.assertEqual(self.evt_mgr.get_event_hierarchy(MyEvt4),
                         (MyEvt4, MyEvt2, MyEvt3, MyEvt, BaseEvent))

    def test_prepost_emit(self):
        """ Test whether pre/post methods of event are called correctly on emit.
        """
        call_seq = []
        class MyEvt(BaseEvent):
            def pre_emit(self):
                call_seq.append(0)
            def post_emit(self):
                call_seq.append(2)
        def callback(evt):
            call_seq.append(1)

        evt = MyEvt()
        self.evt_mgr.connect(BaseEvent, callback)
        self.evt_mgr.emit(evt)
        self.assertEqual(call_seq, list(range(3)))

    def test_reentrant_disconnect_emit(self):
        """ Test listener is called even if it is disconnected before notify.
        """
        data = []
        def callback(evt):
            data.append(0)
            self.evt_mgr.disconnect(BaseEvent, callback2)
            self.evt_mgr.disconnect(BaseEvent, callback3)
            data.append(1)

        def callback2(evt):
            data.append(2)

        def callback3(evt):
            data.append(3)

        self.evt_mgr.connect(BaseEvent, callback)
        self.evt_mgr.connect(BaseEvent, callback2)
        self.evt_mgr.connect(BaseEvent, callback3)

        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(data, [0, 1, 2, 3])

    def test_lambda_connect(self):
        """ Test if lambda functions w/o references are not garbage collected.
        """
        data = []
        self.evt_mgr.connect(BaseEvent, lambda evt: data.append(1))
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(data, [1])

    def test_method_weakref(self):
        """ Test if methods do not prevent garbage collection of objects.
        """
        data = []
        class MyHeavyObject(object):
            def callback(self, evt):
                data.append(1)

        obj = MyHeavyObject()
        obj_wr = weakref.ref(obj)
        self.evt_mgr.connect(BaseEvent, obj.callback)
        del obj
        # Now there should be no references to obj.
        self.assertEqual(obj_wr(), None)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(data, [])

    def test_method_call(self):
        """ Test if instance methods are called.
        """
        data = []

        class MyHeavyObject(BaseEvent):

            def callback(self, evt):
                data.append(1)

            def callback_unbound(self):
                data.append(2)

        obj = MyHeavyObject()
        obj_wr = weakref.ref(obj)
        self.evt_mgr.connect(BaseEvent, obj.callback)
        self.evt_mgr.connect(BaseEvent, MyHeavyObject.callback_unbound)
        self.assertTrue(obj_wr() is not None)
        self.evt_mgr.emit(obj)
        self.assertEqual(data, [1, 2])

    def test_method_collect(self):
        """ Test if object garbage collection disconnects listener method.
        """
        data = []
        class MyHeavyObject(object):
            def callback(self, evt):
                data.append(1)
        obj = MyHeavyObject()
        obj_wr = weakref.ref(obj)
        self.evt_mgr.connect(BaseEvent, obj.callback)
        del obj
        # Now there should be no references to obj.
        self.assertEqual(obj_wr(), None)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(data, [])
        self.assertEqual(len(list(self.evt_mgr.get_listeners(BaseEvent))), 0)

    def test_method_disconnect(self):
        """ Test if method disconnect works.
        """
        data = []
        class MyHeavyObject(object):
            def callback(self, evt):
                data.append(1)
        obj = MyHeavyObject()
        obj_wr = weakref.ref(obj)
        self.evt_mgr.connect(BaseEvent, obj.callback)
        self.evt_mgr.disconnect(BaseEvent, obj.callback)
        del obj
        # Now there should be no references to obj.
        self.assertEqual(obj_wr(), None)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(data, [])

    def test_method_disconnect2(self):
        """ Test if method disconnect on unconnected method fails.
        """
        data = []
        class MyHeavyObject(object):
            def callback(self, evt):
                data.append(1)
            def callback2(self, evt):
                data.append(2)
        obj = MyHeavyObject()
        self.evt_mgr.connect(BaseEvent, obj.callback)
        with self.assertRaises(Exception):
            self.evt_mgr.disconnect(BaseEvent, obj.callback2)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(data, [1])

    def test_no_block(self):
        """ Test if non-blocking emit works.
        """
        data = []
        lock = threading.Lock()
        lock.acquire()
        def callback(evt):
            # callback will wait until lock is released.
            with lock:
                data.append('callback')
                data.append(threading.current_thread().name)
        self.evt_mgr.connect(BaseEvent, callback)

        t = self.evt_mgr.emit(BaseEvent(), block=False)
        # The next statement will be executed before callback returns.
        data.append('main')
        # Unblock the callback to proceed.
        lock.release()
        # Wait until the event handling finishes.
        t.join()
        data.append('main2')

        self.assertEqual(len(data), 4)
        self.assertEqual(data[0], 'main')
        self.assertEqual(data[1], 'callback')
        self.assertEqual(data[3], 'main2')

    def test_reentrant_emit(self):
        """ Test if reentrant emit works. """
        data = []
        class MyEvt(BaseEvent): pass
        class MyEvt2(BaseEvent): pass
        def callback(evt):
            typ = type(evt)
            data.append(typ)
            if typ == MyEvt:
                self.evt_mgr.emit(MyEvt2())
            data.append(typ)

        self.evt_mgr.connect(MyEvt, callback)
        self.evt_mgr.connect(MyEvt2, callback)

        self.evt_mgr.emit(MyEvt())
        self.assertEqual(data, [MyEvt, MyEvt2, MyEvt2, MyEvt])

    def test_reconnect(self):
        """ Test reconnecting already connected listener. """
        calls = []
        def callback1(evt):
            calls.append(1)
        def callback2(evt):
            calls.append(2)

        # Test if reconnect disconnects previous.
        self.evt_mgr.connect(BaseEvent, callback1)
        self.evt_mgr.connect(BaseEvent, callback1)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(calls, [1])
        calls[:] = []

        # Test if sequence is changed.
        self.evt_mgr.connect(BaseEvent, callback2)
        self.evt_mgr.connect(BaseEvent, callback1)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(calls, [2, 1])
        calls[:] = []

    def test_global_event_manager(self):
        """ Test if getting/setting global event manager works. """
        evt_mgr = get_event_manager()
        self.assertIsInstance(evt_mgr, BaseEventManager)

        # Reset the global event_manager
        package_globals._event_manager = None

        set_event_manager(self.evt_mgr)
        self.assertEqual(self.evt_mgr, get_event_manager())

        self.assertRaises(ValueError, lambda: set_event_manager(evt_mgr))

class TracingTests(unittest.TestCase):
    def setUp(self):
        self.evt_mgr = EventManager()
        self.traces = []
        self.tracedict = {}
        self.veto_condition = None

    def trace_func(self, name, method, args):
        """ Trace function for event manager. """
        save = (name, method, args)
        self.traces.append(save)
        self.tracedict.setdefault(name, []).append(save)

    def trace_func_veto(self, name, method, args):
        """ Trace function to veto actions. """
        self.trace_func(name, method, args)
        if self.veto_condition is None or self.veto_condition(name, method, args):
            return True

    def test_set_trace(self):
        """ Test whether setting trace method works. """
        self.evt_mgr.set_trace(self.trace_func)
        self.evt_mgr.emit(BaseEvent())
        self.assertTrue(len(self.traces), 1)
        self.evt_mgr.set_trace(None)
        self.evt_mgr.emit(BaseEvent())
        self.assertTrue(len(self.traces), 1)

    def test_trace_emit(self):
        """ Test whether trace works for all actions. """
        self.evt_mgr.set_trace(self.trace_func)
        self.evt_mgr.emit(BaseEvent())
        self.assertTrue(len(self.traces), 1)
        self.assertEqual(len(self.tracedict['emit']), 1)

        callback1 = mock.Mock()
        self.evt_mgr.connect(BaseEvent, callback1)
        self.assertTrue(len(self.traces), 2)
        self.assertEqual(len(self.tracedict['connect']), 1)

        self.evt_mgr.emit(BaseEvent())
        self.assertTrue(len(self.traces), 4)
        self.assertEqual(len(self.tracedict['emit']), 2)
        self.assertEqual(len(self.tracedict['listen']), 1)
        self.assertEqual(callback1.call_count, 1)

        self.evt_mgr.disconnect(BaseEvent, callback1)
        self.assertTrue(len(self.traces), 5)
        self.assertEqual(len(self.tracedict['disconnect']), 1)
        self.assertEqual(callback1.call_count, 1)

    def test_trace_veto(self):
        """ Test whether vetoing of actions works. """
        callback1 = mock.Mock()
        callback2 = mock.Mock()

        self.evt_mgr.set_trace(self.trace_func_veto)
        self.evt_mgr.connect(BaseEvent, callback1)
        self.evt_mgr.emit(BaseEvent())
        self.assertTrue(len(self.traces), 2)
        self.assertEqual(len(self.tracedict['connect']), 1)
        self.assertEqual(len(self.tracedict['emit']), 1)
        self.assertEqual(len(self.tracedict.get('listen', [])), 0)
        self.assertFalse(callback1.called)

        # Disable calling of callback1
        self.veto_condition = lambda name, method, args: name == 'listen' and method == callback1
        self.evt_mgr.connect(BaseEvent, callback1)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(len(self.tracedict['connect']), 2)
        self.assertEqual(len(self.tracedict['emit']), 2)
        self.assertEqual(len(self.tracedict['listen']), 1)
        self.assertFalse(callback1.called)

        # Ensure callback2 is still called.
        self.evt_mgr.connect(BaseEvent, callback2)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(len(self.tracedict['connect']), 3)
        self.assertEqual(len(self.tracedict['emit']), 3)
        self.assertEqual(len(self.tracedict['listen']), 3)
        self.assertFalse(callback1.called)
        self.assertTrue(callback2.called)

        # Disable tracing.
        self.evt_mgr.set_trace(None)
        self.evt_mgr.emit(BaseEvent())
        self.assertEqual(len(self.tracedict['connect']), 3)
        self.assertEqual(len(self.tracedict['emit']), 3)
        self.assertEqual(len(self.tracedict['listen']), 3)
        self.assertTrue(callback1.called)
        self.assertEqual(callback2.call_count, 2)


if __name__ == '__main__':
    unittest.main()
