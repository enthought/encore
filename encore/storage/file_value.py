#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

import os
import stat

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

    @property
    def permissions(self):
        tags = {
            self._owner: {
                'read': stat.S_IRUSR,
                'write': stat.S_IWUSR,
                'execute': stat.S_IXUSR,
            },
            'group': {
                'read': stat.S_IRGRP,
                'write': stat.S_IWGRP,
                'execute': stat.S_IXGRP,
            },
            'other': {
                'read': stat.S_IROTH,
                'write': stat.S_IWOTH,
                'execute': stat.S_IXOTH,
            }
        }
        return {'owned': {self._owner},
            'read': {tag for tag, masks in tags.items() if masks['read'] &
                self._mode},
            'write': {tag for tag, masks in tags.items() if masks['write'] &
                self._mode},
            'execute': {tag for tag, masks in tags.items() if masks['execute'] &
                self._mode},
        }
        
    def _stat(self):
        stat = os.stat(self._path)
        self.size = stat.st_size
        self.created = None
        self.modified = stat.st_mtime
        self._mode = stat.st_mode
        self._owner = str(stat.st_uid)
            
            