.. currentmodule:: encore.storage.events

Events
------

The Storage API generates events using the Encore Event API.  This module defines
the event classes that are required aby the abstract API.

Event Inheritance Diagram
=========================

The following diagram shows the inheritance heirarchy of the various Event
subclasses defined in this module.  When listening for events, you may want to
listen on appropriate superclasses.

.. inheritance-diagram::
    encore.events.api.BaseEvent
    encore.events.progress_events.ProgressEvent
    encore.events.progress_events.ProgressStartEvent
    encore.events.progress_events.ProgressStepEvent
    encore.events.progress_events.ProgressEndEvent
    StoreEvent
    StoreTransactionEvent
    StoreTransactionStartEvent
    StoreTransactionEndEvent
    StoreKeyEvent
    StoreModificationEvent
    StoreSetEvent
    StoreUpdateEvent
    StoreDeleteEvent
    StoreProgressEvent
    StoreProgressStartEvent
    StoreProgressStepEvent
    StoreProgressEndEvent
    :parts: 1

.. automodule:: encore.storage.events
    :members: StoreEvent, StoreTransactionEvent,  StoreTransactionStartEvent,
        StoreTransactionEndEvent, StoreKeyEvent,
        StoreModificationEvent,  StoreSetEvent,  StoreUpdateEvent,
        StoreDeleteEvent, StoreProgressEvent, StoreProgressStartEvent,
        StoreProgressStepEvent, StoreProgressEndEvent

