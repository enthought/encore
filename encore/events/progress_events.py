#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#
""" Events and helpers for managing progress indicators
"""

from .abstract_event_manager import BaseEvent

class ProgressEvent(BaseEvent):
    """ Abstract base class for all progress events
    
    This class is provided so that listeners can easily listen for any type
    ProgressEvent.

    Attributes
    ----------    
    operation_id :
        A unique identifier for the operation being performed.
    
    message : string
        A human-readable describing the operation being performed.
    
    """

class ProgressStartEvent(ProgressEvent):
    """ Event emitted at the start of an operation
    
    Attributes
    ----------    
    operation_id :
        A unique identifier for the operation being performed.
    
    message : string
        A human-readable describing the operation being performed.
    
    steps : int
        The number of steps in the operation.  If unknown or variable, use -1.
        
    """

class ProgressStepEvent(ProgressEvent):
    """ Event emitted periodically during an operation
    
    Attributes
    ----------    
    operation_id :
        A unique identifier for the operation being performed.
    
    message : string
        A human-readable describing the state of the operation being performed.
    
    step : int
        The count of the step.  If unknown, use -1.
        
    """

class ProgressEndEvent(ProgressEvent):
    """ Event emitted at the end of an operation
    
    Attributes
    ----------
    operation_id :
        A unique identifier for the operation that is finished.
    
    message : string
        A human-readable describing the state of the operation that ended.
    
    exit_state : string
        A constant describing the end state of the operation.  One of ``normal``,
        ``warning``, ``error`` or ``exception``.
        
    """


class ProgressManager(object):
    """ Utility class for managing progress events
    
    This class provides a context manager that will probably be sufficient in
    most use cases.  The standard method of invoking it will be something like::
    
        with ProgressManager(event_manager, source, id, "Performing operation", steps) as progress:
            for step in range(steps):
                ... do work ...
                progress(step)
    
    This pattern guarantees that the appropriate Start and Stop events are
    always emitted, even if there is an exception.
    
    If finer-grained control is needed, the class also provides start(), step()
    and stop() methods that can be invoked in when required.  In particular,
    this pattern may be useful for more fine-grained exception reporting::
        
        progress = ProgressManager(event_manager, source, id, "Performing operation", steps)
        progress.start()
        try:
            for step in range(steps):
                ... do work ...
                progress(step)
        except ... as exc:
            progress.end(message='Failure mode 1', end_state='warning')
        except ... as exc:
            progress.end(message='Failure mode 2', end_state='error')
        except Exception as exc:
            progress.end(message=str(exc), end_state='exception')
        else:
            progress.end(message='Success', end_state='normal')
    
    Attributes
    ----------
    StartEventType : ProgressStartEvent subclass
        The actual event class to use when emitting a start event.  The default
        is :py:class:`ProgressStartEvent`, but subclasses may choose to override.
    StepEventType : ProgressStepEvent subclass
        The actual event class to use when emitting a step event.  The default
        is :py:class:`ProgressStepEvent`, but subclasses may choose to override.
    EndEventType : ProgressEndEvent subclass
        The actual event class to use when emitting an end event.  The default
        is :py:class:`ProgressEndEvent`, but subclasses may choose to override.
    
    """
    
    StartEventType = ProgressStartEvent
    StepEventType = ProgressStepEvent
    EndEventType = ProgressEndEvent
    
    def __init__(self, event_manager=None, source=None, operation_id=None,
            message='Performing operation', steps=-1, **kwargs):
        """ Create a progress manager instance
        
        Arguments
        ---------
        event_manager : EventManager instance
            The event manager to use when emitting events.
        
        source : any
            The object that is the source of the events.
        
        operation_id : any
            The unique identifier for the operation.
        
        message : string
            The default message to use for events which are emitted.
        
        steps : int
            The number of steps.  If this is not known, use -1.
        
        """
        if event_manager is None:
            from .package_globals import get_event_manager
            event_manager = get_event_manager()
        self.event_manager = event_manager
        
        if source is None:
            source = self
        self.source = source
        
        if operation_id is None:
            from uuid import uuid4
            operation_id = uuid4()
        self.operation_id = operation_id
        
        self.message = message
        self.steps = steps
        self.kwargs = kwargs
        
        self._step_count = 0
        self._running = False
    
    def start(self, **extra_kwargs):
        """ Emit a start event
        
        By default creates an instance of :py:attr:`StartEventType` with the
        appropriate attributes.
        
        Parameters
        ----------
        extra_kwargs : dict
            Additional arguments to be passed through to the event's constructor.
            
        """
        self._running = True
        
        kwargs = self.kwargs.copy()
        kwargs.update(**extra_kwargs)
        
        self.event_manager.emit(self.StartEventType(
            source=self.source,
            operation_id=self.operation_id,
            message=self.message,
            steps=self.steps,
            **kwargs))
    
    def step(self, message=None, step=None, **extra_kwargs):
        """ Emit a step event
        
        By default creates an instance of :py:attr:`StepEventType` with the
        appropriate attributes.
        
        Parameters
        ----------
        message : str
            The message to be passed to the event's constructor.  By default will
            use ``self.message``.
        step : int
            The step number.  By default keeps an internal step count, incremented
            each time this method is called.
        extra_kwargs : dict
            Additional arguments to be passed through to the event's constructor.
            
        """
        if not self._running:
            raise Exception("ProgressManager.step() called before start()")

        message = self.message if message is None else message
        step = self._step_count if step is None else step
        kwargs = self.kwargs.copy()
        kwargs.update(**extra_kwargs)

        self.event_manager.emit(self.StepEventType(
            source=self.source,
            operation_id=self.operation_id,
            message=message,
            step=step,
            **kwargs))

        self._step_count += 1

    def end(self, message=None, exit_state='normal', **extra_kwargs):
        """ Emit a step event
        
        By default creates an instance of :py:attr:`StepEventType` with the
        appropriate attributes.
        
        Parameters
        ----------
        message : str
            The message to be passed to the event's constructor.  By default will
            use ``self.message``.
        exit_state : one of ``normal``, ``warning``, ``error`` or ``exception``
            The exit_state of the event.
        extra_kwargs : dict
            Additional arguments to be passed through to the event's constructor.
            
        """
        if not self._running:
            raise Exception("ProgressManager.end() called before start()")
            
        message = self.message if message is None else message
        kwargs = self.kwargs.copy()
        kwargs.update(**extra_kwargs)

        self.event_manager.emit(self.EndEventType(
            source=self.source,
            operation_id=self.operation_id,
            message=message,
            exit_state=exit_state,
            **kwargs))
        self._running = False

    def __call__(self, message=None, step=None, **extra_kwargs):
        if not self._running:
            self.start()
        self.step(message, step, **extra_kwargs)
    
    def __enter__(self):
        if not self._running:
            self.start()
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self._running:
            if exc_value is not None:
                message = str(exc_value)
                exit_state = 'exception'
                self.end(message, exit_state)
            else:
                self.end()
