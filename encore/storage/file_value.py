#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

import os
import time

from .abstract_store import Value

class FileValue(Value):
    
    def __init__(self, path, metadata=None):
        self._path = path
        self._data_stream = None
        self._metadata = metadata
        self._stat()
            
    @property
    def data(self):
        if self._data_stream is None:
            self._data_stream = file(self._path, 'rb')
        return self._data_stream

    @property
    def metadata(self):
        return self._metadata.copy()
        
    def _stat(self):
        stat = os.stat(self._path)
        self.size = stat.st_size
        self.created = None
        #self.created = created if created is not None else time.time()
        self.modified = stat.st_mtime
        #self.permissions = permissions if permissions is not None else set(['exists', 'get', 'set', 'delete'])
            
            