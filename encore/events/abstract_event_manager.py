#
#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#
""" This module defines event manager class API.

The main class of the module is the :py:class:`BaseEventManager`.
Event managers are expected to implement the interface as specified by
:py:class:`BaseEventManager`. A concrete implementation is present in the
:py:mod:`~encore.events.event_manager`
module.

"""

# Standard library imports.
from abc import ABCMeta, abstractmethod


###############################################################################
# `BaseEvent` Class.
###############################################################################
class BaseEvent(object):
    """ Base class for all events.
    
    Parameters
    ----------
    source : object
        The object which generated the Event.
    kwargs : dict
        Additional Event attributes which will be added to the Event object.
    
    """

    def __init__(self, source=None, **kwargs):
        # The source of the event.
        kwargs['source'] = [] if source is None else source
        self.__dict__.update(**kwargs)
        
        # Whether the event has been handled by a listener.
        self._handled = False
    
    def mark_as_handled(self):
        """ Mark the event as handled so subsequent listeners are not notified.
        """
        self._handled = True

    def pre_emit(self):
        """ Called before emitting an event.

        Can be used any event specific functionality, validation etc.
        """
        pass

    def post_emit(self):
        """ Called after emitting an event.

        Can be used any event specific functionality, post event validation etc.
        """
        pass

###############################################################################
# `BaseEventManager` Class.
###############################################################################
class BaseEventManager(object):
    """ This abstract class defines the API for Event Managers.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
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
                - key: string which is extended (`.` separated) name of an
                       attribute of the event instance.
                - value: the value of the specified attribute.

            If the attribute does not exist then the filter is considered failed
            and the listener is not called.

        priority : int
            The priority of the listener. Higher priority listeners are called
            before lower priority listeners.

        Note
        ----
       
        The filtering is added so that future optimizations can be done
        on specific events with large number of handlers. For example there
        should be a fast way to filter key events to specific listeners rather
        than iterating through all listeners.
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def disable(self, cls):
        """ Disable the event from generating notifications.

        Parameters
        ----------
        cls : class
            The class of events which we want to disable.

        """
        raise NotImplementedError

    @abstractmethod
    def enable(self, cls):
        """ Enable the event again to generate notifications.

        Parameters
        ----------
        cls : class
            The class of events which we want to enable.

        """
        raise NotImplementedError

    @abstractmethod
    def is_enabled(self, cls):
        """ Check if the event is enabled.

        Parameters
        ----------
        cls : class
            The class of events which we want check the status of.
        """
        raise NotImplementedError

###############################################################################
