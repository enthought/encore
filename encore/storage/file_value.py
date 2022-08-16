#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

import os

from .abstract_store import Value, AuthorizationError
from .utils import BufferIteratorIO, buffer_iterator

class FileValue(Value):

    def __init__(self, path, metadata=None):
        self._path = path
        self._data_stream = None
        self._metadata = metadata
        self._stat()

    @property
    def data(self):
        if self._data_stream is None:
            self._data_stream = open(self._path, 'rb')
        return self._data_stream

    @property
    def metadata(self):
        return self._metadata.copy()

    @property
    def permissions(self):
        raise AuthorizationError("key not owned by user")

    def range(self, start=None, end=None):
        if start is None:
            start = 0
        if self._data_stream is None:
            self._data_stream = open(self._path, 'rb')
        self._data_stream.seek(start)
        if end is not None:
            max_bytes = end-start
            return BufferIteratorIO(
                buffer_iterator(self._data_stream, max_bytes=max_bytes)
            )
        else:
            return self._data_stream

    def _stat(self):
        stat = os.stat(self._path)
        self.size = stat.st_size
        self.created = None
        self.modified = stat.st_mtime
