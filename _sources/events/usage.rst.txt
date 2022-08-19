Usage
=====

.. currentmodule:: encore.events.api

The :py:mod:`encore.events` package provides a fairly straightforward event
dispatcher.

Event Classes
-------------

Basic filtering is based upon the class of the event, so most users
will want to define their own set of event classes, but a number of standard
:py:mod:`BaseEvent` subclasses are provided by the module.  An event class can
be as simple as::

    from encore.events.api import BaseEvent
    
    class SaveEvent(BaseEvent):
        """ Event generated when a file is saved
        
        Attributes
        ----------
        directory : path
            The directory that the file that was saved in.
        filename : string
            The name of the file that was saved.
        
        """

The :py:class:`~BaseEvent`'s :py:meth:`~BaseEvent.__init__` method takes any
additional keyword arguments it is supplied, and adds them as attributes on the
object.  Every event has a source object which is the object which generated the
event.  You can create an instance of an event like so::

    event = SaveEvent(source=obj, directory='foo/bar', filename='baz.txt')

Because filtering of events respects the class heirarchy of events, you will
frequently want to define some abstract base classes to assist with filtering::

    from encore.events.api import BaseEvent
    
    class FileEvent(BaseEvent):
        """ Event generated when a file is operated upon
        
        Attributes
        ----------
        directory : path
            The directory that the file that was saved in.
        filename : string
            The name of the file that was saved.
        
        """
    
    class OpenEvent(FileEvent):
        pass
    
    class SaveEvent(FileEvent):
        pass
    
    class DeleteEvent(FileEvent):
        pass

In the above example, you will probably never generate an instance of
:py:class:`FileEvent`, but you may set up listeners for such events.

Event Managers
--------------

To emit events, you will then need to ensure that your application has a
(usually unique) event manager to handle the dispatch of events.  Creating an
event manager is straightforward::

    from encore.events.api import EventManager
    
    event_manager = EventManager()

More typically you will have some sort of global application state object that
is responsible for managing things like event managers, and then you might use
it as follows::

    import os
    from uuid import uuid4
    from encore.events.api import EventManager, ProgressManager
    from .app_events import SaveEvent

    class App(object):
        def __init__(self):
            self.event_manager = EventManager()

    class File(object):
        def __init__(self, app, directory, filename, data=''):
            self.app = app
            self.directory = directory
            self.filename = filename
            self.data = data
    
        def save(self):
            event_manager = self.app.event_manager
            path = os.path.join(self.directory, self.filename)
            op_id = uuid4()
            try:
                with open(path, 'wb') as fp:
                    steps = range(0, len(data), 2**20)
                    with ProgressManager(event_manager, self, op_id,
                            'Saving "%s"' % path, len(steps)) as progress:
                        for i, pos in enumerate(step):
                            fp.write(self.data[i:i+2**20])
                            progress('Saving "%s" (%d of %d bytes)' % (path, pos, len(data)),
                                step=i+1)
            else:
                event_manager.emit(SaveEvent(source=self, directory=self.directory,
                    filename=self.filename))

Notice the use of the standard :py:class:`ProgressManager` subclasses to generate
progress update events while writing out the data.

Listeners
---------

A listener is simply a function which expects to be given an event instance and
does something with it.  For example, we could write a listener which listens
for :py:class:`SaveEvents` and logs them to a logger::

    import logging
    import os
    
    logger = logging.getLogger(__name__)
    
    def save_logger(event):
        path = os.path.join(event.directory, event.filename)
        logger.info("Saved file '%s'" % path)

Once you have a listener, it can be connected to listen for particular classes
of events via the event manager::

    event_manager.connect(SaveEvent, save_logger)

Once the listener is connected, the :py:func:`save_logger` function will be
called every time that a :py:class:`SaveEvent` is emitted.  A listener can be
explicitly disconnected by calling the :py:meth:`disconnect` method of the event
manager::

    event_manager.disconnect(SaveEvent, save_logger)

A listener which is a bound method will be disconnected automatically if the
underlying instance has been garbage-collected, so in many instances you will
not need to worry about explicitly disconnecting listeners.

In the above example, you would be more likely to want to log all
:py:class:`FileEvents` rather than save events.  This could be achieved by
something like::

    def file_event_logger(event):
        path = os.path.join(event.directory, event.filename)
        logger.info("%s: file '%s'" % (event.__class__.__name__, path))

    event_manager.connect(FileEvent, file_event_logger)

This will call the :py:func:`file_event_logger` function every time that a
subclass of :py:class:`FileEvent` is emitted.

Listener Priority
-----------------

It is possible to have multiple listeners on a particular class, and you may
want some listeners to run before other listeners.  In particular, a listener
may mark an event as "handled" in which case processing stops and all lower
priority listeners do not get to see the event.

For instance, in the above example, we might want to have both the
:py:func:`save_logger` and :py:func:`file_event_logger` active.  In that case we
don't want to have save events logged twice, so we can do the following::

    def save_logger(event):
        path = os.path.join(event.directory, event.filename)
        logger.info("Saved file '%s'" % path)
        event.mark_as_handled()
    
    event_manager.connect(SaveEvent, save_logger, priority=100)
    event_manager.connect(FileEvent, file_event_logger, piority=50)

By setting the priority of :py:func:`save_logger` higher than that of
:py:func:`file_event_logger`, it will get called first, and when it calls the
event's :py:meth:`~.BaseEvent.mark_as_handled` method then it will prevent any
lower-priority events from firing.

In the default event manager implementation, listeners of the same priority are
called in the order in which they were connected.

Filtering
---------

On occassion a listener may only care about events from certain sources or
matching certain attributes.  The event manager allows a filter to be specified
when connecting a listener, so that the listener will only be called when the
filter is matched.

A filter is simply specified as a dictionary of event attribute, value pairs::

    class Project(object):
        def __init__(self, app, directory):
            self.app = app
            self.directory = directory
            self._needs_compile = False
            self._connect_listener()

        def directory_listener(self, event):
            self._needs_complie = True
        
        def _connect_listener(self):
            self.app.event_manager.connect(SaveEvent, self.directory_listener,
                filter={'directory': self.directory})

In this example, a :py:class:`Project` instance will have its
:py:meth:`directory_watcher` method called whenever a file is saved in the
directory specified by its :py:attr:`directory` attribute.

Example: Progress Bar
---------------------

As an example which ties together the concepts which have been shown so far,
we will write some code which displays progress indications to standard out
that look something like the following::

    Saving "foo/bar/baz.txt":
    [*************************************

We start with a class which is responsible for listening for the start of a
progress event.  For simplicty we will assume that there will only be one
progress sequence happening at any given time, so we will have the class instance
hook up a listener for :py:class:`ProgressStartEvents`::

    class ProgressDisplay(object):
        def __init__(self, event_manager):
            self.event_manager = event_manager
            self.event_manager.connect(ProgressStartEvent, self.start_listener)
        
When a :py:class:`ProgressStartEvent` occurs, then we will print out the initial
text, and set up a listener for the :py:class:`ProgressStepEvent` and
:py:class:`ProgressEndEvent` event types::

        def start_listener(self, event):
            # display initial text
            sys.stdout.write(event.message)
            sys.stdout.write(':\n[')
            sys.stdout.flush()
            
            # create a ProgressWriter instance
            writer = ProgressWriter(self, event.operation_id, event.steps)
            self.writers[event.operation_id] = writer
            
            # connect listeners
            self.event_manager.connect(ProgressStepEvent, writer.step_listener,
                filter={'operation_id': event.operation_id})
            self.event_manager.connect(ProgressEndEvent, writer.end_listener,
                filter={'operation_id': event.operation_id})

The writer class handles listening for step and end events.  The end event
listener simply removes the writer object from the display, which will cause it
to eventually be garbage-collected and the listeners disconnected automatically::

    class ProgressWriter(object):
        def __init__(self, display, operation_id, steps):
            self.display = display
            self.operation_id = operation_id
            self.steps = steps
            self._count = 0
            self._max = 75
        
        def step_listener(self, event):
            stars = int(round(float(event.step)/self.steps*self._max))
            if stars > self._count:
                sys.stdout.write('*'*(stars-self._count))
                sys.stdout.flush()
                self._count = stars
        
        def end_listener(self, event):
            if event.exit_state == 'normal':
                sys.stdout.write(']\n')
                sys.stdout.flush()
            else:
                sys.stdout.write('\n')
                sys.stdout.write(event.exit_state.upper())
                sys.stdout.write(': ')
                sys.stdout.write(event.message)
                sys.stdout.write('\n')
                sys.stdout.flush()
            del self.display[self.operation_id]

Advanced Features
-----------------

Disabling Events
~~~~~~~~~~~~~~~~

The event manager has methods that allow code to temporarily disable events
of a certain class.  These are accessed via the :py:meth:`~.EventManager.disable`,
:py:meth:`~.EventManager.enable`, and :py:meth:`~.EventManager.is_enabled` methods.
Disabling an event class will also disable any of its subclasses, so::

    event_manager.disable(BaseEvent)

will disable all events.

Enabled/disabled state is kept track of on a per-class basis, so after::

    event_manager.disable(SaveEvent)
    event_manager.disable(FileEvent)
    event_manager.enable(FileEvent)

the :py:class:`SaveEvent` events will still be disabled.

Pre- and Post-Emit Callbacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The event classes also have two hooks :py:meth:`~.BaseEvent.pre_emit` and
:py:meth:`~.BaseEvent.post_emit` which get called immediately before and
immediately after dispatch to listeners.  This potentially allows Event code to
perform actions based upon interactions with listeners, such as having a
:py:meth:`~.BaseEvent.post_emit` method which does something sensible if an
event is not handled.  These hooks may also be of use for instrumenting and
debugging code.

Threading
~~~~~~~~~

By default events are processed on the thread that they were emitted on, and
the :py:meth:`~.EventManager.connect`, :py:meth:`~.EventManager.disconnect`
and :py:meth:`~.EventManager.emit` methods should be thread-safe.  Processing
an event blocks that thread from further work until all listeners have been
called.

The :py:meth:`~.EventManager.emit` method has an optional argument ``block``
which if ``False`` will cause the emit method to create a worker thread to
perform the listener dispatch, and will return that thread from the function
call.

