#
#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#
""" This module defines an event registry, notification and filtering class.

The main class of the module is the `EventManager`.
"""

# Standard library imports.
import logging
import itertools
import bisect
import heapq
import threading
from types import MethodType
import weakref
import traceback

# Logging.
logger = logging.getLogger(__name__)

# Local imports
from .abstract_event_manager import BaseEvent, BaseEventManager


###############################################################################
# Logging Trace Function.
###############################################################################
class LoggingTracer(object):
    """ A tracer object for event manager to log events.
    
    Usage
    -----
    event_manager.set_trace(LoggingTracer())

    """
    def __init__(self, logger=logger, level=logging.INFO):
        self._level = level
        self._logger = logger

    def __call__(self, name, func, *args, **kwds):
        self._logger.log(self._level,
                         'event_log: %s, function: %s,\nargs=%s,\nkwds=%s',
                         name, func, args, kwds)

###############################################################################
# Notifier classes for callables: lightweight weakref substitutes.
###############################################################################
class CallableNotifier(object):
    """ Notifier for general callables, whose strong reference is stored.
    """
    __slots__ = ['func']
    def __init__(self, func, notify=None, args=()):
        self.func = func

    def __call__(self):
        """ Return the original listener callable.
        """
        return self.func

class MethodNotifier(object):
    """ Notifiers for methods, for which weak refs of object is stored.
    """
    __slots__ = ['func', 'cls', 'obj', '_notify', '_args']
    def __init__(self, meth, notify=None, args=()):
        self.func = meth.im_func
        self.cls = meth.im_class
        obj = meth.im_self
        if obj is None:
            # Unbound Method.
            self.obj = None
        else:
            # Bound method.
            self.obj = weakref.ref(meth.im_self, self.notify)
        if notify:
            self._notify = notify
            self._args = args

    def notify(self, ref):
        """ Notify the garbage collection listeners.
        """
        self._notify(*self._args)

    def __call__(self):
        """ Return the original listener method, or None if it no longer exists.
        """
        obj = self.obj
        if obj is None:
            # Unbound method.
            objc = None
        else:
            objc = obj()
            if objc is None:
                # Bound method whose object has been garbage collected.
                return
        return MethodType(self.func, objc, self.cls)

###############################################################################
# `EventInfo` Private Class.
###############################################################################
class EventInfo(object):
    """ A class which manages handling of a single event.
    """
    def __init__(self, cls):
        """ Constructor.

        Parameters:
        -----------
        cls : class
            Class of the event.
        """
        self.cls = cls
        self._priority_list = [] # sorted priority list
        self._priority_info = {}
        self._listener_filters = {}
        self._filter_keys = set() # to precompute filters on event emit
        self._disable = False

        self._priority_list_lock = threading.Lock()


    def connect(self, func, filter=None, priority=0, count=0):
        """ Add a listener for the event.


        Parameters
        ----------
        cls : class
            The class of events for which the listener is registered.

        func : callable
            A callable to be called when the event is emitted.  The function
            should expect one argument which is the event instance which was
            emitted.

        filter : dict
            Filters to match for before calling the listener. The listener is
            called only when the event matches all of the filter .
            
            Filter specification:
                - key: string which is extended name of an attribute of the
                    event instance. For example string 'source.name' will be
                    the attribute `event.source.name`
                - value: the value of the specified attribute.

        priority : int
            The priority of the listener. Higher priority listeners are called
            before lower priority listeners.

        count : int
            A unique integer to break a tie in priority. This is generally
            an incremental number assigned by EventManager in order of
            registration.

        Note
        ----
       
        Reconnecting an already connected listener will disconnect the
        old listener. This may have rammifications in changing the filters
        and the priority.

        The filtering is added so that future optimizations can be done
        on specific events with large number of handlers. For example there
        should be a fast way to filter key events to specific listeners rather
        than iterating through all listeners.

        """
        id = self.get_id(func)
        if id in self._priority_info:
            # Ensure a function is connected only once.
            # Reconnecting will update its sequence and filters.
            self._disconnect(id)
        with self._priority_list_lock:
            sub = self._get_notifier(func, self._listener_deleted)
            if filter:
                self._listener_filters[id] = filter
                for key in filter:
                    self._filter_keys.add(key)
            key = (-priority, count, sub)
            bisect.insort_left(self._priority_list, key)
            self._priority_info[id] = key

    def disconnect(self, func):
        """ Disconnects a listener from being notified about the event'

        Parameters
        ----------
        func : callable
            The callable which was registered.

        """
        self._disconnect(self.get_id(func))

    def _listener_deleted(self, id):
        if id in self._priority_info:
            self._disconnect(id)

    def _disconnect(self, id):
        with self._priority_list_lock:
            key = self._priority_info[id]
            idx = bisect.bisect_left(self._priority_list, key)
            del self._priority_info[id]
            del self._priority_list[idx]
            if id in self._listener_filters:
                del self._listener_filters[id]

    def get_id(self, func):
        """ Get an id as unique key for the function. """
        if type(func) is MethodType:
            obj = func.im_self
            if obj is None:
                # Unbound method
                return weakref.ref(func.im_func),weakref.ref(func.im_class)
            else:
                # Bound method.
                return weakref.ref(func.im_func),weakref.ref(func.im_self)
        else:
            return func

    def _get_notifier(self, func, notify=None):
        """ Notify is callable to be called when the bound func's object
        is garbage collected.
        """
        if type(func) is MethodType:
            if notify is None:
                args = ()
            else:
                args = (self.get_id(func),)
            return MethodNotifier(func, notify, args)
        else:
            return CallableNotifier(func, notify)

    def get_listeners(self, event):
        """ Return listeners which will be called on specified event.

        If ``event`` is None, all listeners are returned.
        If ``event`` is an event, only listeners which will be called for the
        event are returned (satisfying any filters on the listeners).
        """
        with self._priority_list_lock:
            if event is None or not self._listener_filters:
                return self._priority_list[:]
            ret = []
            l_filter = self._listener_filters
            for linfo in self._priority_list:
                listener = linfo[-1]
                id = self.get_id(listener())
                if id in l_filter:
                    for key, value in l_filter[id].iteritems():
                        attr = event
                        try:
                            # Get extended attributes of the event.
                            for part in key.split('.'):
                                attr = getattr(attr, part)
                        except AttributeError as e:
                            logger.info('Error filtering listener: %s; %s',
                                        linfo, e)
                            break
                        if attr != value:
                            break
                    else:
                        ret.append(linfo)
                else:
                    ret.append(linfo)
            return ret

    def disable(self):
        """ Disable the event from generating notifications.
        """
        self._disable = True

    def enable(self):
        """ Enable the event again to generate notifications.
        """
        self._disable = False

    def is_enabled(self):
        """ Check if the event is enabled.
        """
        return not self._disable


###############################################################################
# `EventManager` Class.
###############################################################################

class EventManager(BaseEventManager):
    """ A single registry point for all application events.

    """
    # store the length of the BaseEvent's __mro__
    bmro_clip = -len(BaseEvent.__mro__)+1
    def __init__(self):
        self.event_map = {}
        self.count = itertools.count()
        self._trace_func = None

    ###########################################################################
    # `EventManager` Interface
    ###########################################################################
    def register(self, cls):
        """ Register an event with the event manager.

        Calling this is not generally nececssary. An event is automatically
        registered the first time a listener connects to it.

        Parameters:
        -----------
        cls : str
            The ``class`` of the event.
        """
        if cls in self.event_map:
            raise ValueError('Event {0} already registered'.format(cls))
        else:
            self.event_map[cls] = EventInfo(cls)

    def connect(self, cls, func, filter=None, priority=0):
        """ Add a listener for the event.

        Parameters
        ----------
        cls : class
            The class of events for which the listener is registered.

        func : callable
            A callable to be called when the event is emitted.  The function
            should expect one argument which is the event instance which was
            emitted.

        filter : dict
            Filters to match for before calling the listener. The listener is
            called only when the event matches all of the filter .
            
            Filter specification:
                - key: string which is name of an attribute of the event instance.
                - value: the value of the specified attribute.

        priority : int
            The priority of the listener. Higher priority listeners are called
            before lower priority listeners.

        Note
        ----
       
        Reconnecting an already connected listener will disconnect the
        old listener. This may have rammifications in changing the filters
        and the priority.

        The filtering is added so that future optimizations can be done
        on specific events with large number of handlers. For example there
        should be a fast way to filter key events to specific listeners rather
        than iterating through all listeners.

        """
        if self._trace_func is not None:
            if self._trace_func('connect', self.connect,
                                (cls, func, filter, priority)):
                return
        if cls not in self.event_map:
            self.register(cls)
        self.event_map[cls].connect(func, filter, priority, next(self.count))

    def disconnect(self, cls, func):
        """ Disconnects a listener from being notified about the event'

        Parameters
        ----------
        cls : class
            The class of events for which the listener is registered.
        func : callable
            The callable which was registered for that class.

        Raises
        ------
        KeyError :
            if `func` is not already connected.
            
        """
        if self._trace_func is not None:
            if self._trace_func('disconnect', self.disconnect,
                                (cls, func)):
                return
        self.event_map[cls].disconnect(func)

    def emit(self, event, block=True):
        """ Notifies all listeners about the event with the specified arguments.

        Parameters
        ----------
        event : instance of :py:class:`BaseEvent`
            The :py:class:`BaseEvent` instance to emit.
        block : bool
            Whether to block the call until the event handling is finished.
            If block is False, the event will be emitted in a separate thread
            and the thread will be returned, so you can later query its status
            or do ``wait()`` on the thread.

        Note
        ----
        
        Listeners of superclasses of the event are also called.
        Eg. a :py:class:`BaseEvent` listener will also be notified about any
        derived class events.

        """
        if not block:
            t = threading.Thread(target=self.emit, args=(event, True),
                                 name='Event emit: {0}'.format(event))
            t.start()
            return t

        trace_func = self._trace_func
        if trace_func is not None:
            if trace_func('emit', self.emit, (event,)):
                return

        cls = type(event)
        if not self.is_enabled(cls):
            return

        listeners = self.get_listeners(event, cls)

        event.pre_emit()

        for listener in listeners:
            try:
                if trace_func is not None:
                    if trace_func('listen', listener, (event,)):
                        continue
                listener(event)
            except Exception as e:
                logger.warn('Exception {0} occurred in listener: {1} for '
                    'event: {2}:\n{3}'.format(e, listener, event,
                                              traceback.format_exc()))
            if event._handled:
                # Only enable when debugging -- very slow even if it doesn't get emitted because 
                # it must still do the string formatting.
                #logger.debug('Event: {0} handled by listener: {1}'.format(
                #                                        event, listener))
                break

        event.post_emit()

    def get_event(self, cls=None):
        """ Returns an ``EventInfo`` instance for the event.

        Parameters
        ----------
        cls : class
            The class of the event we want the ``EventInfo`` for.        
            If ``cls`` is ``None``, then all known event types are returned.
        
        Returns
        -------
        event_info :
            An ``EventInfo`` instance for the class, or a dictionary mapping
            classes to ``EventInfo`` instances.
        
        """
        if cls is None:
            return self.event_map
        else:
            return self.event_map[cls]

    def get_listeners(self, event, cls=None):
        """ Return listeners which will be called on specified event.
        
        Parameters
        ----------
        
        event : event instance or class
            The event we want to get the listeners for.
            If ``event`` is an instance of BaseEvent(), the listeners which will
            be called for the event are returned (satisfying any filters on the
            listeners).
            
            If ``event`` is BaseEvent subclass, all listeners for specified event
            class are returned (but no filtering is performed).
        
        cls : BaseEvent subclass
            ``cls`` argument is generally not needed, it is for internal use.
            If ``cls`` is specified as a subclass of ``BaseEvent``, then only
            listeners for the specified event class and superclasses are
            returned.
        
        """
        evt_map = self.event_map
        if cls is None:
            if isinstance(event, BaseEvent):
                cls = type(event)
            else:
                cls = event
                event = None
        classes = self.get_event_hierarchy(cls)
        listeners = heapq.merge(*[evt_map[cls].get_listeners(event)
                                    for cls in classes if cls in evt_map])
        listeners = (l[-1]() for l in listeners)
        return listeners

    def disable(self, cls):
        """ Disable the event from generating notifications.

        Parameters
        ----------
        cls : class
            The class of events which we want to disable.

        """
        if cls not in self.event_map:
            self.register(cls)
        self.event_map[cls].disable()

    def enable(self, cls):
        """ Enable the event again to generate notifications.

        Parameters
        ----------
        cls : class
            The class of events which we want to enable.

        """
        if cls not in self.event_map:
            self.register(cls)
        self.event_map[cls].enable()

    def is_enabled(self, cls):
        """ Check if the event is enabled.

        Parameters
        ----------
        cls : class
            The class of events which we want check the status of.

        """
        for cls in self.get_event_hierarchy(cls):
            if cls in self.event_map and not self.event_map[cls].is_enabled():
                return False
        return True

    def get_event_hierarchy(self, cls):
        """ The the sequence of event classes which are notified for given cls.
        
        """
        return cls.__mro__[:self.bmro_clip]

    def set_trace(self, func):
        """ Set a trace method for various actions performed.

        func is a callable which takes three arguments:
            name, method and args

        name - str
            An identifiable name of the method being called
            Either of 'connect', 'disconnect', 'emit' or 'listen'
            The subsequent arguments depend on this value.
        method - callable
            The method which would be called as a consequence of the action
            If name=='listen', this is the listener which would be called.
        args - tuple
            The arguments specific to each method name are specified below:
                connect - cls, func, filter, priority
                disconnect - cls, func
                emit - event
                listen - event

        If the trace function returns a True-like value: the corresponding
        action is not performed. For listen action, the corresponding listener
        is not called, but subsequent ones are called (depending on return
        value for their trace function calls).

        Note: Calling set_trace with None as the callable argument removes
        existing trace function set. Also, only a single trace method can be
        active at a time, calling set_trace removes the existing trace function.

        """
        self._trace_func = func
