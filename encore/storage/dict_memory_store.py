#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

"""
Memory Store
------------

This is a simple implementation of the key-value store API that lives entirely
in memory.  Data and metadata are stored in dictionaries.  This is not optimized
in any way to reduce memory usage.

This class is provided in part as a sample implementation of the API.

"""

import cStringIO
from itertools import izip
from uuid import uuid4

from .abstract_store import AbstractStore
from .utils import buffer_iterator, DummyTransactionContext, StoreProgressManager
from .events import StoreUpdateEvent, StoreSetEvent, StoreDeleteEvent


class DictMemoryStore(AbstractStore):
    """ Dictionary-based in-memory Store
    
    This is a simple implementation of the key-value store API that lives entirely
    in memory.  Data and metadata are stored in dictionaries.  This
    is not optimized in any way to reduce memory usage.

    The file-like objects returned by data methods are cStringIO objects.
    
    Parameters
    ----------
    event_manager :
        An object which implements the :py:class:`~.abstract_event_manager.BaseEventManager` API.

    """
    
    def __init__(self, event_manager):
        self._data = {}
        self._metadata = {}
        self.event_manager = event_manager
        self._connected = False
    
    def connect(self, credentials=None):
        """ Connect to the key-value store
        
        Parameters
        ----------
        credentials : None
            This store does not authenticate, and has no external resources,
            so credentials are ignored.

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
        return {
            'readonly': False
        }
    
    
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
        return self.get_data(key), self._metadata[key].copy()
    
    
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
            finishing writing to the underlying store.
        StoreSetEvent :
            On successful completion of a transaction, a StoreSetEvent should be
            emitted with the key & metadata
     
        """
        data, metadata = value
        self._metadata[key] = metadata.copy()
        self.set_data(key, data, buffer_size)

    def delete(self, key):
        """ Delete a key from the repsository.
        
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
        del self._data[key]
        metadata = self._metadata.pop(key)
        self.event_manager.emit(StoreDeleteEvent(self, key=key, metadata=metadata))
    
    
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
        return key in self._data and key in self._metadata


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

        """
        return cStringIO.StringIO(self._data[key])


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
            has keys as specified by the metadata_keys argument.
        
        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store,
            and if any metadata key is requested which is not present in the
            metadata.

        """
        metadata = self._metadata[key]
        if select is not None:
            return dict((metadata_key, metadata[metadata_key])
                for metadata_key in select if metadata_key in metadata)
        return metadata.copy()
    
    
    def set_data(self, key, data, buffer_size=1048576):
        """ Replace the data for a given key in the key-value store.
        
        If the key does not already exist, it tacitly creates an empty metadata
        object.
        
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
        update = key in self._data
        metadata = self._metadata.get(key, None)
        with StoreProgressManager(self.event_manager, self, uuid4(),
                "Setting data into '%s'" % (key,), -1,
                key=key, metadata=metadata) as progress:
            chunks = list(buffer_iterator(data, buffer_size, progress))
            self._data[key] = b''.join(chunks)
        if metadata is None:
            self._metadata[key] = {}
        if update:
            self.event_manager.emit(StoreUpdateEvent(self, key=key, metadata=metadata))
        else:
            self.event_manager.emit(StoreSetEvent(self, key=key, metadata=metadata))
    
    
    def set_metadata(self, key, metadata):
        """ Set new metadata for a given key in the key-value store.
        
        This replaces the existing metadata set for the key with a new set of
        metadata.  If the key does not already exist, it tacitly creates an
        empty data object.
        
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
        update = key in self._metadata
        self._metadata[key] = metadata.copy()
        if not key in self._data:
            self._data[key] = b''
        if update:
            self.event_manager.emit(StoreUpdateEvent(self, key=key, metadata=metadata))
        else:
            self.event_manager.emit(StoreSetEvent(self, key=key, metadata=metadata))


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
        self._metadata[key].update(metadata)
        self.event_manager.emit(StoreUpdateEvent(self, key=key, metadata=self._metadata[key]))
   
      
    def transaction(self, notes):
        """ Provide a transaction context manager
        
        This class does not support transactions, so it returns a dummy object.
        
        Parameters
        ----------
        notes : string
            Some information about the transaction, which is ignored by this
            implementation.

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
            An iterable of keys, metadata tuples where metadata matches
            all the specified values for the specified metadata keywords.
        
        """
        if select is not None:
            for key, metadata in self._metadata.items():
                if all(metadata.get(arg) == value for arg, value in kwargs.items()):
                    yield key, dict((metadata_key, metadata[metadata_key])
                        for metadata_key in select if metadata_key in metadata)
        else:
            for key, metadata in self._metadata.items():
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
        for key, metadata in self._metadata.items():
            if all(metadata.get(arg) == value for arg, value in kwargs.items()):
                yield key

        
    def to_file(self, key, path, buffer_size=1048576):
        """ Efficiently store the data associated with a key into a file.
        
        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        path : string
            A file system path to store the data to.
        buffer_size : int
            This is ignored.
        
        """
        with open(path, 'wb') as fp:
            fp.write(self._data[key])
    
    
    def from_file(self, key, path, buffer_size=1048576):
        """ Efficiently read data from a file into a key in the key-value store.
        
        This makes no attempt to set metadata.
                
        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        path : string
            A file system path to read the data from.
        buffer_size : int
            This is ignored.
        
        """
        with open(path, 'rb') as fp:
            self._data[key] = fp.read()

    def to_bytes(self, key, buffer_size=1048576):
        """ Efficiently store the data associated with a key into a bytes object.
        
        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        buffer_size : int
            This is ignored.
        
        """
        return self._data[key]
        
    def from_bytes(self, key, data, buffer_size=1048576):
        """ Efficiently read data from a bytes object into a key in the key-value store.
        
        This makes no attempt to set metadata.
                
        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        data : bytes
            The data as a bytes object.
        buffer_size : int
            This is ignored.
        
        """
        self._data[key] = data

