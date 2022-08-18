#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

from io import BytesIO
import time

from .abstract_store import Value, AuthorizationError
from .utils import add_context_manager_support

class StringValue(Value):

    def __init__(self, data=b'', metadata=None, created=None, modified=None):
        self._data = data
        self._data_stream = None
        self._metadata = metadata if metadata is not None else {}
        self.size = len(self._data)
        self.created = created if created is not None else time.time()
        self.modified = modified if modified is not None else time.time()

    @property
    def data(self):
        if self._data_stream is None:
            self._data_stream = BytesIO(self._data)
            add_context_manager_support(self._data_stream)
        return self._data_stream

    @property
    def metadata(self):
        return self._metadata.copy()

    @property
    def permissions(self):
        raise AuthorizationError("key not owned by user")

    def range(self, start=None, end=None):
        data_range = BytesIO(self._data[slice(start, end)])
        add_context_manager_support(data_range)
        return data_range
