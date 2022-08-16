#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#
"""
File System Store
=================

This file defines a filesystem store.  This stores data in a specified directory
in a filesystem.  Data files are stored in files with name key+'.data' and
metadata files with name key+'.metadata'.

"""

# System library imports.
import glob
import io
import json
import os

# ETS library imports.
from .abstract_store import AbstractStore
from .file_value import FileValue
from .events import StoreSetEvent, StoreUpdateEvent, StoreDeleteEvent
from .utils import DummyTransactionContext, buffer_iterator, StoreProgressManager

class FileSystemStoreError(Exception):
    pass


def init_shared_store(path, magic_fname='.FSStore'):
    """Create the magic file for the shared store.  Useful to initialize
    the store for the first time.

    Parameters
    ----------
    path :
        The directory that will be used for the file store.
    magic_fname :
        The name of the magic file in that directory,
    """
    magic_path = os.path.join(path, magic_fname)
    with open(magic_path, 'wb') as magic_fp:
        magic_fp.write(b'__version__ = 0\n')


################################################################################
# SharedFSStore class.
################################################################################
class FileSystemStore(AbstractStore):
    """
    A store that uses a Shared file system to store the data/metadata.
    """
    def __init__(self, path, magic_fname='.FSStore'):
        """Initializes the store given a path to a store.

        Parameters
        ----------
        path : str:
            A path to the root of the file system store.
        magic_fname :
            The name of the magic file in that directory,

        """
        super(FileSystemStore, self).__init__()
        self._root = path
        self._magic_fname = magic_fname

        if not os.path.exists(path):
            raise FileSystemStoreError('Unable to find path %s'%path)
        # The path should have a .FSStore file.
        if not (os.path.exists(os.path.join(path, self._magic_fname))):
            raise FileSystemStoreError('Path %s is not a valid store'%path)

    def connect(self, credentials=None):
        """ Connect to the key-value store.

        Parameters
        ----------
        credentials :
            These are not used by default.

        """
        self._connected = True

    def disconnect(self):
        """ Disconnect from the key-value store

        This store does not authenticate, and has no external resources, so this
        does nothing

        """
        self._connected = False

    def is_connected(self):
        """ Whether or not the store is currently connected

        Returns
        -------
        connected : bool
            Whether or not the store is currently connected.

        """
        return self._connected

    def info(self):
        """ Get information about the key-value store

        Returns
        -------

        metadata : dict
            A dictionary of metadata giving information about the key-value store.
        """
        return {'type': 'FileSystemStore', 'version': 0}

    ##########################################################################
    # Basic Create/Read/Update/Delete Methods
    ##########################################################################
    def get(self, key):
        """ Retrieve a stream of data and metdata from a given key in the key-value store.

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.

        Returns
        -------
        data : file-like
            A readable file-like object that provides stream of data from the
            key-value store
        metadata : dictionary
            A dictionary of metadata for the key.

        Raises
        ------
        KeyError :
            If the key is not found in the store, a KeyError is raised.

        """
        data_path = self._get_data_path(key)
        metadata = self.get_metadata(key)
        return FileValue(data_path, metadata)

    def set(self, key, value, buffer_size=1048576):
        """ Store a stream of data into a given key in the key-value store.

        This may be left unimplemented by subclasses that represent a read-only
        key-value store.

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        value : tuple of file-like, dict
            A pair of objects, the first being a readable file-like object that
            provides stream of data from the key-value store.  The second is a
            dictionary of metadata for the key.
        buffer_size : int
            An optional indicator of the number of bytes to read at a time.
            Implementations are free to ignore this hint or use a different
            default if they need to.  The default is 1048576 bytes (1 MiB).

        Events
        ------
        StoreProgressStartEvent :
            For buffering implementations, this event should be emitted prior to
            writing any data to the underlying store.
        StoreProgressStepEvent :
            For buffering implementations, this event should be emitted
            periodically as data is written to the underlying store.
        StoreProgressEndEvent :
            For buffering implementations, this event should be emitted after
            finishing writing to the underlying store
        StoreSetEvent :
            On successful completion of a transaction, a StoreSetEvent should be
            emitted with the key & metadata

        """
        update = self.exists(key)
        metadata_path = self._get_metadata_path(key)
        data_path = self._get_data_path(key)
        if isinstance(value, tuple):
            data_stream, metadata = value
            steps = -1
        else:
            data_stream = value.data
            metadata = value.metadata
            steps = value.size

        json_string = json.dumps(metadata, ensure_ascii=False)
        with io.open(metadata_path, 'w', encoding='utf-8') as fh:
            fh.write(json_string)

        with open(data_path, 'wb') as fp:
            bytes_written = 0
            progress = StoreProgressManager(source=self, steps=steps,
                    message="Setting key '%s'" % key, key=key,
                    metadata=metadata)
            with progress:
                with data_stream:
                    for buffer in buffer_iterator(data_stream, buffer_size):
                        fp.write(buffer)
                        fp.flush()
                        bytes_written += len(buffer)
                        progress("Setting key '%s' (%d bytes written)"
                            % (key, bytes_written))

        if update:
            self.event_manager.emit(StoreUpdateEvent(self, key=key, metadata=metadata))
        else:
            self.event_manager.emit(StoreSetEvent(self, key=key, metadata=metadata))

    def delete(self, key):
        """ Delete a key from the repsository.

        This may be left unimplemented by subclasses that represent a read-only
        key-value store.

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.

        Events
        ------
        StoreDeleteEvent :
            On successful completion of a transaction, a StoreDeleteEvent should
            be emitted with the key.

        """
        metadata = self.get_metadata(key)
        metadata_path = self._get_metadata_path(key)
        data_path = self._get_data_path(key)
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
        if os.path.exists(data_path):
            os.remove(data_path)
        self.event_manager.emit(StoreDeleteEvent(self, key=key, metadata=metadata))

    def get_data(self, key):
        """ Retrieve a stream from a given key in the key-value store.

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.

        Returns
        -------
        data : file-like
            A readable file-like object the that provides stream of data from the
            key-value store.

        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.

        """
        data_path = self._get_data_path(key)
        if not self.exists(key):
            raise KeyError('Key %s does not exist in store!'%key)
        else:
            return open(data_path, 'rb')

    def get_metadata(self, key, select=None):
        """ Retrieve the metadata for a given key in the key-value store.

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        select : iterable of strings or None
            Which metadata keys to populate in the result.  If unspecified, then
            return the entire metadata dictionary.

        Returns
        -------
        metadata : dict
            A dictionary of metadata associated with the key.  The dictionary
            has keys as specified by the select argument.  If a key specified in
            select is not present in the metadata, then it will not be present
            in the returned value.

        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.

        """
        metadata_path = self._get_metadata_path(key)
        if not os.path.exists(metadata_path):
            raise KeyError('Key %s does not exist in store!'%metadata_path)
        else:
            md = self._get_metadata(metadata_path)
            if select is None:
                return md
            else:
                return dict((k, md[k]) for k in select if k in md)

    def set_data(self, key, data, buffer_size=1048576):
        """ Replace the data for a given key in the key-value store.

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        data : file-like
            A readable file-like object the that provides stream of data from the
            key-value store.
        buffer_size : int
            An optional indicator of the number of bytes to read at a time.
            Implementations are free to ignore this hint or use a different
            default if they need to.  The default is 1048576 bytes (1 MiB).

        Events
        ------
        StoreProgressStartEvent :
            For buffering implementations, this event should be emitted prior to
            writing any data to the underlying store.
        StoreProgressStepEvent :
            For buffering implementations, this event should be emitted
            periodically as data is written to the underlying store.
        StoreProgressEndEvent :
            For buffering implementations, this event should be emitted after
            finishing writing to the underlying store.
        StoreSetEvent :
            On successful completion of a transaction, a StoreSetEvent should be
            emitted with the key & metadata

        """
        # FIXME: Add support for events and buffering.
        metadata_path = self._get_metadata_path(key)
        if not os.path.exists(metadata_path):
            metadata = {}
        else:
            metadata = self._get_metadata(metadata_path)
        self.set(key, (data, metadata), buffer_size)


    def set_metadata(self, key, metadata):
        """ Set new metadata for a given key in the key-value store.

        This replaces the existing metadata set for the key with a new set of
        metadata.

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        metadata : dict
            A dictionary of metadata to associate with the key.  The dictionary
            keys should be strings which are valid Python identifiers.

        Events
        ------
        StoreSetEvent :
            On successful completion of a transaction, a StoreSetEvent should be
            emitted with the key & metadata

        """
        metadata_path = self._get_metadata_path(key)
        metadata_str = json.dumps(metadata).encode('utf-8')
        with open(metadata_path, 'wb') as fh:
            fh.write(metadata_str)
        self._touch(key)

    def update_metadata(self, key, metadata):
        """ Update the metadata for a given key in the key-value store.

        This performs a dictionary update on the existing metadata with the
        provided metadata keys and values

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        metadata : dict
            A dictionary of metadata to associate with the key.  The dictionary
            keys should be strings which are valid Python identifiers.

        Events
        ------
        StoreSetEvent :
            On successful completion of a transaction, a StoreSetEvent should be
            emitted with the key & metadata

        """
        update = self.exists(key)
        metadata_path = self._get_metadata_path(key)
        new_metadata = self._get_metadata(metadata_path)
        new_metadata.update(metadata)
        json_string = json.dumps(metadata, ensure_ascii=False)
        with io.open(metadata_path, 'w', encoding='utf-8') as fh:
            fh.write(json_string)
        if update:
            self.event_manager.emit(StoreUpdateEvent(self, key=key, metadata=metadata))
        else:
            self.event_manager.emit(StoreSetEvent(self, key=key, metadata=metadata))

    def exists(self, key):
        """ Test whether or not a key exists in the key-value store

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.

        Returns
        -------
        exists : bool
            Whether or not the key exists in the key-value store.

        """
        metadata_path = self._get_metadata_path(key)
        if os.path.exists(metadata_path):
            return True
        else:
            return False

    def transaction(self, notes):
        """ Provide a transaction context manager

        This class does not support transactions, so it returns a dummy object.

        """
        return DummyTransactionContext(self)


    def query(self, select=None, **kwargs):
        """ Query for keys and metadata matching metadata provided as keyword arguments

        This provides a very simple querying interface that returns precise
        matches with the metadata.  If no arguments are supplied, the query
        will return the complete set of metadata for the key-value store.

        Parameters
        ----------
        select : iterable of strings or None
            An optional list of metadata keys to return.  If this is not None,
            then the metadata dictionaries will only have values for the specified
            keys populated.
        kwargs :
            Arguments where the keywords are metadata keys, and values are
            possible values for that metadata item.

        Returns
        -------
        result : iterable
            An iterable of (key, metadata) tuples where metadata matches
            all the specified values for the specified metadata keywords.
            If a key specified in select is not present in the metadata of a
            particular key, then it will not be present in the returned value.

        """
        all_metadata = glob.glob(os.path.join(self._root, '*.metadata'))
        items = [(os.path.splitext(os.path.basename(x))[0], x) for x in all_metadata]
        if select is not None:
            for key, path in items:
                metadata = self._get_metadata(path)
                if all(metadata.get(arg) == value for arg, value in kwargs.items()):
                    yield key, dict((metadata_key, metadata[metadata_key])
                        for metadata_key in select if metadata_key in metadata)
        else:
            for key, path in items:
                metadata = self._get_metadata(path)
                if all(metadata.get(arg) == value for arg, value in kwargs.items()):
                    yield key, metadata.copy()

    def query_keys(self, **kwargs):
        """ Query for keys matching metadata provided as keyword arguments

        This provides a very simple querying interface that returns precise
        matches with the metadata.  If no arguments are supplied, the query
        will return the complete set of keys for the key-value store.

        This is equivalent to ``self.query(**kwargs).keys()``, but potentially
        more efficiently implemented.

        Parameters
        ----------
        kwargs :
            Arguments where the keywords are metadata keys, and values are
            possible values for that metadata item.

        Returns
        -------
        result : iterable
            An iterable of key-value store keys whose metadata matches all the
            specified values for the specified metadata keywords.

        """
        all_metadata = glob.glob(os.path.join(self._root, '*.metadata'))
        if kwargs:
            items = [(os.path.splitext(os.path.basename(x))[0], x) for x in all_metadata]
            for key, path in items:
                metadata = self._get_metadata(path)
                if all(metadata.get(arg) == value for arg, value in kwargs.items()):
                    yield key
        else:
            for x in all_metadata:
                yield os.path.splitext(os.path.basename(x))[0]

    ##########################################################################
    # Private methods
    ##########################################################################
    def _get_metadata_path(self, key):
        path = os.path.join(self._root, key + '.metadata')
        return os.path.normpath(path)

    def _get_data_path(self, key):
        path = os.path.join(self._root, key + '.data')
        return os.path.normpath(path)

    def _get_metadata(self, path):
        with open(path, 'rb') as fh:
            content = fh.read()
        md = json.loads(content.decode('utf-8'))
        return md

    def _touch(self, key):
        path = self._get_data_path(key)
        if os.path.exists(path):
            os.utime(path, None)
        else:
            open(path, 'a').close()
