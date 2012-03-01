#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

from cStringIO import StringIO
import time

from .abstract_store import Value

class StringValue(Value):
    
    def __init__(self, data='', metadata=None, created=None, modified=None):
        if not isinstance(data, basestring):
            raise ValueError(data)
        self._data = data
        self._data_stream = None
        self._metadata = metadata if metadata is not None else {}
        self.size = len(self._data)
        self.created = created if created is not None else time.time()
        self.modified = modified if modified is not None else time.time()
    
    @property
    def data(self):
        if self._data_stream is None:
            self._data_stream = StringIO(self._data)
        return self._data_stream

    @property
    def metadata(self):
        return self._metadata.copy()
        
