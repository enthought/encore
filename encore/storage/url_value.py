#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

from email.utils import parsedate_tz, mktime_tz

import urllib


from .abstract_store import Value, AuthorizationError
from .utils import (
    BufferIteratorIO, buffer_iterator, add_context_manager_support
)


class URLValue(Value):

    def __init__(self, url, metadata=None, opener=None):
        self._url = url
        self._metadata = metadata if metadata is not None else {}
        self._opener = opener if opener is not None else urllib.request.urlopen
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
    def permissions(self):
        raise AuthorizationError("key not owned by user")

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

    def range(self, start=None, end=None):
        # need to build a reqquest with a range header
        start_string = str(start) if start is not None else ''
        end_string = str(end) if end is not None else ''
        request = urllib.request.Request(self._url, headers={
            'Range': 'bytes={0}-{1}'.format(start_string, end_string),
        })

        stream = self._opener.open(request)
        add_context_manager_support(stream)

        if stream.getcode() == 206:
            # it worked!
            return stream
        else:
            if start is not None:
                stream.read(start)
            else:
                start = 0
            if end is not None:
                max_bytes = end-start
                return BufferIteratorIO(buffer_iterator(stream,
                                                        max_bytes=max_bytes))
            else:
                return stream

    def open(self):
        self._data_stream = self._opener.open(self._url)

        headers = self._data_stream.info()
        size = headers.get('Content-Length', None)
        if size is not None:
            size = int(size)
        self._size = size

        modified = headers.get('Last-Modified', None)
        if modified is not None:
            modified = mktime_tz(parsedate_tz(modified))
        self._modified = modified

        self._data_stream = add_context_manager_support(self._data_stream)

        return self._data_stream
