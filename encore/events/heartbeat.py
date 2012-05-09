#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#

import sys
import time
import threading

from .abstract_event_manager import BaseEvent
from .package_globals import get_event_manager

if sys.platform == 'win32':
    accurate_time = time.clock
else:
    accurate_time = time.time

class HeartbeatEvent(BaseEvent):
    """ Event which is emitted periodically
    """
    pass

class Heartbeat(object):
    """ Service which emits an event periodically
    
    Note that unless the event manager uses threaded dispatch, event listeners
    which take longer than the interval to perform will result in a slower
    heartbeat.  The heartbeat runs on its own thread, and as a result, any
    listeners will also run on that thread.
    
    The heartbeat is only intended to be approximately accurate, and should not
    be used for applications which require precise timing.
    
    """
    
    def __init__(self, interval=1/50., event_manager=None):
        self.state = 'waiting'
        self.interval = interval
        self.frame_count = 0
        self.event_manager = (event_manager if event_manager is not None
            else get_event_manager())
    
    def run(self):
        self.state = 'running'
        while self.state in ['running', 'paused']:
            if self.state == 'running':
                t = accurate_time()
                self.event_manager.emit(HeartbeatEvent(source=self, time=t,
                    frame=self.frame_count, interval=self.interval))
                self.frame_count += 1
                # try to ensure regular heartbeat, but always sleep for at least 1ms
                wait = max(t+self.interval-accurate_time(), 0.001)
            else:
                wait = self.interval
            time.sleep(wait)
        self.state = 'stopped'

    def serve(self):
        thread = threading.Thread(target=self.run)
        thread.daemon = True
        thread.start()
    

