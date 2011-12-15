from encore.events.api import (BaseEvent, ProgressEvent, ProgressStartEvent,
    ProgressStepEvent, ProgressEndEvent)


class StoreEvent(BaseEvent):
    pass

class StoreKeyEvent(StoreEvent):
    """ An abstract base class for evenst related to a particular key in the
    store.  This should provide the key and metadata (if available) of the
    modified key.
    
    Attributes
    ----------
    
    key : string
        The key which is involved in the event.
    
    metadata : dict
        The metadata of the key which is involved in the event.
        
    """

class StoreProgressEvent(ProgressEvent, StoreKeyEvent):
    pass

class StoreProgressStartEvent(ProgressStartEvent, StoreProgressEvent):
    """
    
    Attributes
    ----------
    
    operation_id :
        A unique identifier for the operation being performed.
    
    message : string
        A human-readable describing the operation being performed.
    
    steps : int
        The number of steps in the operation.  If unknown or variable, use -1.
        
    """

class StoreProgressStepEvent(ProgressStepEvent, StoreProgressEvent):
    """
    
    Attributes
    ----------
    
    operation_id :
        A unique identifier for the operation being performed.
    
    message : string
        A human-readable describing the state of the operation being performed.
    
    step : int
        The count of the step.  If unknown, use -1.
        
    """

class StoreProgressEndEvent(ProgressEndEvent, StoreProgressEvent):
    """
    
    Attributes
    ----------
    
    operation_id :
        A unique identifier for the operation that is finished.
    
    message : string
        A human-readable describing the state of the operation that ended.
    
    exit_state : string
        A constant describing the end state of the operation.  One of 'normal',
        'warning', 'error' or 'exception'.
        
    """
