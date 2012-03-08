#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

import urllib2
import rfc822

from .abstract_store import Value

class URLValue(Value):
    
    def __init__(self, url, metadata=None, opener=None):
        self._url = url
        self._metadata = metadata if metadata is not None else {}
        self._opener = opener if opener is not None else urllib2.urlopen
        self._data_stream = None
        self._size = None
        self._created = None
        self._modified = None
    
    @property
    def data(self):
        if self._data_stream is None:
            self.open()
        return self._data_stream

    @property
    def metadata(self):
        return self._metadata.copy()
        
    @property
    def size(self):
        if self._data_stream is not None:
            self.open()
        return self._size
        
    @property
    def modified(self):
        if self._data_stream is not None:
            self.open()
        return self._modified
        
    @property
    def created(self):
        if self._data_stream is not None:
            self.open()
        return self._created
    
    def open(self):
        self._data_stream = self._opener.open(self._url)
        headers = self._data_stream.info()

        size = headers.get('Content-Length', None)
        if size is not None:
            size = int(size)
        self._size = size

        modified = headers.get('Last-Modified', None)
        if modified is not None:
            modified = rfc822.mktime_tz(rfc822.parsedate_tz(modified))
        self._modified = modified
        
        return self._data_stream
