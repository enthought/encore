#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

# standard library imports
import sys
import time

# Local imports
from encore.events.api import ProgressStartEvent, ProgressStepEvent, ProgressEndEvent
from .utils import write_static


class ProgressWriter(object):
    """ Display an animated progress bar """

    def __init__(self, display, operation_id, steps):
        self.display = display
        self.operation_id = operation_id
        self.steps = steps
        self._max = 70
        self.last_step = 0
        self.last_time = 0
        self._format = '%5d [%'+str(-self._max)+'s]'

    def step_listener(self, event):
        if time.time() - self.last_time >= 0.05:
            stars = int(round(float(event.step)/self.steps*self._max))
            write_static(self._format % (event.step, '*'*(stars)))
            self.last_time = time.time()
        self.last_step = event.step

    def end_listener(self, event):
        if event.exit_state == 'normal':
            write_static(self._format % (self.last_step, '*'*self._max))
            sys.stdout.write('\nDone.\n')
            sys.stdout.flush()
        else:
            sys.stdout.write('\n%s: %s\n' % (event.exit_state.upper(), event.message))
            sys.stdout.flush()
        del self.display.writers[self.operation_id]


class SpinWriter(object):
    """ Display an animated progress spinner """

    def __init__(self, display, operation_id):
        self.display = display
        self.operation_id = operation_id
        self._count = 0
        self._max = 70
        self.last_time = 0
        self._format = '%s %'+str(-self._max)+'s'

    def step_listener(self, event):
        if time.time() - self.last_time >= 0.05:
            write_static(self._format % ('\|/-'[self._count % 4], event.message[:self._max]))
            self._count += 1
            self.last_time = time.time()

    def end_listener(self, event):
        if event.exit_state == 'normal':
            write_static(self._format % (' ', 'Done.'))
            sys.stdout.write('\n')
            sys.stdout.flush()
        else:
            sys.stdout.write('\n%s: %s\n' % (event.exit_state.upper(), event.message))
            sys.stdout.flush()
        del self.display.writers[self.operation_id]


class ProgressDisplay(object):
    """ Manage the display of progress indicators """

    progress_writer = ProgressWriter
    spin_writer = SpinWriter

    def __init__(self, event_manager=None):
        if event_manager is None:
            from encore.events.api import get_event_manager
            event_manager = get_event_manager()
        self.event_manager = event_manager
        self.writers = {}
        self._format = '%s:\n'
        self.event_manager.connect(ProgressStartEvent, self.start_listener)


    def start_listener(self, event):
        # display initial text
        sys.stdout.write(self._format % event.message)
        sys.stdout.flush()

        # create a ProgressWriter instance
        if event.steps > 0:
            writer = self.progress_writer(self, event.operation_id, event.steps)
        else:
            writer = self.spin_writer(self, event.operation_id)
        self.writers[event.operation_id] = writer

        # connect listeners
        self.event_manager.connect(ProgressStepEvent, writer.step_listener,
            filter={'operation_id': event.operation_id})
        self.event_manager.connect(ProgressEndEvent, writer.end_listener,
            filter={'operation_id': event.operation_id})
