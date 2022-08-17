#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#

import time
import random
from encore.events.api import ProgressManager
from encore.terminal.api import ProgressDisplay


class CosmicRayError(Exception):
    """ An artifical exception type to play with """
    pass


class ProgressApplication(object):
    """ A simple application that demonstrates ProgressManagers and ProgressDisplays """

    def __init__(self):
        self.display = ProgressDisplay()

    def run(self):
        # create some dummy progress bars
        for j in range(10):
            steps = random.randint(0,600)
            known = random.randint(0,4)
            fail_point = random.randint(0, 2400)

            # create a progress manager
            progress = ProgressManager(source=self,
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

def main():
    app = ProgressApplication()
    app.run()


if __name__ == '__main__':
    main()
