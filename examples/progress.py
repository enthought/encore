import sys
import time
import random
from uuid import uuid4
from encore.events.api import EventManager, ProgressManager, ProgressStartEvent, \
    ProgressStepEvent, ProgressEndEvent

def write_static(s):
    sys.stdout.write('\b%s%s' % (s, '\b'*len(s)))
    sys.stdout.flush()

class ProgressDisplay(object):
    """ Manage the display of progress indicators """
    
    def __init__(self, event_manager):
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
            writer = ProgressWriter(self, event.operation_id, event.steps)
        else:
            writer = SpinWriter(self, event.operation_id)
        self.writers[event.operation_id] = writer
        
        # connect listeners
        self.event_manager.connect(ProgressStepEvent, writer.step_listener,
            filter={'operation_id': event.operation_id})
        self.event_manager.connect(ProgressEndEvent, writer.end_listener,
            filter={'operation_id': event.operation_id})


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

class CosmicRayError(Exception):
    pass

def test():
    e = EventManager()
    display = ProgressDisplay(e)
    
    # create some dummy progress bars
    for j in range(10):
        operation_id = uuid4()
        steps = random.randint(0,600)
        known = random.randint(0,4)
        fail_point = random.randint(0, 2400)
        
        # create a progress manager
        progress = ProgressManager(e, source=None, operation_id=operation_id,
            steps=steps if known else -1,
            message="Doing something %d" % j)
        try:
            with progress:
                for i in range(steps):
                    time.sleep(random.uniform(0, 0.01))
                    if i > fail_point:
                        raise CosmicRayError('Cosmic ray hit a memory location')
                    progress(step=i+1, message="Working...")
        except CosmicRayError:
            # skip our artificial exceptions and move on with the next iteration
            pass

if __name__ == '__main__':
    test()