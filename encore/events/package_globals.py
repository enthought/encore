#
# Canopy product code
#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is confidential and NOT open source.  Do not distribute.
#
""" Module to hold global state, such as a global event manager.

"""


from .event_manager import EventManager

_event_manager = None

def get_event_manager():
    """ Get the global event manager. """
    global _event_manager
    if _event_manager is None:
        _event_manager = EventManager()
    return _event_manager

def set_event_manager(event_manager):
    """ Set the global event manager.

    Raises
    ------
    ValueError - If an event manager has already been set. This is to prevent
        the loss of registered listeners which may be being used by others.

    """
    global _event_manager
    if _event_manager is not None:
        raise ValueError('Event manager has already been set.')
    _event_manager = event_manager



