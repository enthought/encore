#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

"""
Key-Value Store API
===================

This module specifies the key-value store API for the various package management and
installation systems that are in use at Enthought and our clients.

The key-value store API exposes an interface on top of whatever
backend implementation is used by subclasses.  This permits code which requires
access to a key-value store to use it in a uniform way without having to care about
where and how the data is stored.  The API is also agnostic about what is being
stored, and so while the key use case is for egg repositories, potentially any
data values can be stored in the key-value store.

"""

from abc import ABCMeta, abstractmethod
from itertools import izip
import fnmatch
from cStringIO import StringIO

from .utils import StoreProgressManager, buffer_iterator

class Filelike(object):
    """ Abstract base class for file-like objects used by Key-Value stores
    
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def read(self, size=-1):
        """ Read at most size bytes, returning as a string
        
        If size is negative or omitted, then read until EOF.
        
        """
        raise NotImplementedError
    
    @abstractmethod
    def close(self):
        """ Close the file-like object, releasing any resources
        """
        raise NotImplementedError

Filelike.register(file)
Filelike.register(type(StringIO('')))


class AbstractReadOnlyStore(object):
    """ Abstract base class for read-only Key-Value Store API
    
    This class implements some of the API so that it can be used with super()
    where appropriate.
    
    Attributes
    ----------
    event_manager :
        Every store is assumed to have an event_manager attribute which
        implements the :py:class:`~.abstract_event_manager.BaseEventManager` API.
        
    """
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def __init__(self, event_manager):
        self.event_manager = event_manager

        self._connected = False
    
    @abstractmethod
    def connect(self, credentials=None):
        """ Connect to the key-value store, optionally with authentication
        
        This method creates or connects to any long-lived resources that the
        store requires.
        
        Parameters
        ----------
        credentials :
            An object that can supply appropriate credentials to to authenticate
            the use of any required resources.  The exact form of the credentials
            is implementation-specific, but may be as simple as a
            ``(username, password)`` tuple.
            
        """
        self._connected = True
    
    
    @abstractmethod
    def disconnect(self):
        """ Disconnect from the key-value store
        
        This method disposes or disconnects to any long-lived resources that the
        store requires.
        """
        self._connected = False
    
    
    @abstractmethod
    def is_connected(self):
        """ Whether or not the store is currently connected
        
        Returns
        -------
        connected : bool
            Whether or not the store is currently connected.

        """
        return self._connected


    @abstractmethod
    def info(self):
        """ Get information about the key-value store
        
        Returns
        -------
        metadata : dict
            A dictionary of metadata giving information about the key-value store.
        """
        return {
            'readonly': True
        }

        
    ##########################################################################
    # Basic Create/Read/Update/Delete Methods
    ##########################################################################
    
    @abstractmethod
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
        return (self.get_data(key), self.get_metadata(key))


    @abstractmethod
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
        raise NotImplementedError


    @abstractmethod
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
        raise NotImplementedError

                        
    @abstractmethod
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
        try:
            self.get_metadata(key)
        except KeyError:
            return False
        else:
            return True

        
    ##########################################################################
    # Multiple-key Methods
    ##########################################################################
   
    def multiget(self, keys):
        """ Retrieve the data and metadata for a collection of keys.
        
        Parameters
        ----------
        keys : iterable of strings
            The keys for the resources in the key-value store.  Each key is a
            unique identifier for a resource within the key-value store.
        
        Returns
        -------
        result : iterator of (file-like, dict) tuples
            An iterator of (data, metadata) pairs.
        
        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.
        
        """
        for key in keys:
            yield self.get(key)
    
   
    def multiget_data(self, keys):
        """ Retrieve the data for a collection of keys.
        
        Parameters
        ----------
        keys : iterable of strings
            The keys for the resources in the key-value store.  Each key is a
            unique identifier for a resource within the key-value store.
        
        Returns
        -------
        result : iterator of file-like
            An iterator of file-like data objects corresponding to the keys.
        
        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.
        
        """
        for key in keys:
            yield self.get_data(key)
    

    def multiget_metadata(self, keys, select=None):
        """ Retrieve the metadata for a collection of keys in the key-value store.
        
        Parameters
        ----------
        keys : iterable of strings
            The keys for the resources in the key-value store.  Each key is a
            unique identifier for a resource within the key-value store.
        select : iterable of strings or None
            Which metadata keys to populate in the results.  If unspecified, then
            return the entire metadata dictionary.

        Returns
        -------
        metadatas : iterator of dicts
            An iterator of dictionaries of metadata associated with the key.
            The dictionaries have keys as specified by the select argument.  If
            a key specified in select is not present in the metadata, then it
            will not be present in the returned value.
        
        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.
        
        """
        for key in keys:
            yield self.get_metadata(key, select)
        
        
    ##########################################################################
    # Querying Methods
    ##########################################################################
        
    @abstractmethod
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
        raise NotImplementedError

        
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
        return (key for key, value in self.query(**kwargs))


    def glob(self, pattern):
        """ Return keys which match glob-style patterns
        
        Parameters
        ----------
        pattern : string
            Glob-style pattern to match keys with.

        Returns
        -------
        result : iterable
            A iterable of keys which match the glob pattern.
        
        """
        for key in self.query_keys():
            if fnmatch.fnmatchcase(key, pattern):
                yield key
        

    ##########################################################################
    # Utility Methods
    ##########################################################################

    def to_file(self, key, path, buffer_size=1048576):
        """ Efficiently store the data associated with a key into a file.
        
        This method can be optionally overriden by subclasses to proved a more
        efficient way of copy the data from the underlying data store to a path
        in the filesystem.  The default implementation uses the get() method
        together with chunked reads from the returned data stream to the disk.
        
        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        path : string
            A file system path to store the data to.
        buffer_size : int
            An optional indicator of the number of bytes to read at a time.
            Implementations are free to ignore this hint or use a different
            default if they need to.  The default is 1048576 bytes (1 MiB).
        
        Events
        ------
        StoreProgressStartEvent :
            For buffering implementations, this event should be emitted prior to
            writing any data to disk.
        StoreProgressStepEvent :
            For buffering implementations, this event should be emitted
            periodically as data is written to disk.
        StoreProgressEndEvent :
            For buffering implementations, this event should be emitted after
            finishing writing to disk.
        
        """
        with open(path, 'wb') as fp:
            data, metadata = self.get(key)
            bytes_written = 0
            with StoreProgressManager(self.event_manager, self, None,
                    "Saving key '%s' to file '%s'" % (key, path), -1,
                    key=key, metadata=metadata) as progress:
                for buffer in buffer_iterator(data, buffer_size):
                    fp.write(buffer)
                    bytes_written += len(buffer) 
                    progress("Saving key '%s' to file '%s' (%d bytes written)"
                        % (key, path, bytes_written))


    def to_bytes(self, key, buffer_size=1048576):
        """ Efficiently store the data associated with a key into a bytes object.
        
        This method can be optionally overriden by subclasses to proved a more
        efficient way of copy the data from the underlying data store to a bytes
        object.  The default implementation uses the get() method
        together with chunked reads from the returned data stream and join.
        
        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        buffer_size : int
            An optional indicator of the number of bytes to read at a time.
            Implementations are free to ignore this hint or use a different
            default if they need to.  The default is 1048576 bytes (1 MiB).

        Returns
        -------
        bytes :
            The contents of the file-like object as bytes.
        
        Events
        ------
        StoreProgressStartEvent :
            For buffering implementations, this event should be emitted prior to
            extracting the data.
        StoreProgressStepEvent :
            For buffering implementations, this event should be emitted
            periodically as data is extracted.
        StoreProgressEndEvent :
            For buffering implementations, this event should be emitted after
            extracting the data.
        
        """
        return b''.join(buffer_iterator(self.get_data(key), buffer_size))


class AbstractStore(AbstractReadOnlyStore):
    """ Abstract base class for Key-Value Store API
    
    This class implements some of the API so that it can be used with super()
    where appropriate.
    
    Attributes
    ----------
    event_manager :
        Every store is assumed to have an event_manager attribute which
        implements the :py:class:`~.abstract_event_manager.BaseEventManager` API.
        
    """
    __metaclass__ = ABCMeta


    @abstractmethod
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
        
    ##########################################################################
    # Basic Create/Read/Update/Delete Methods
    ##########################################################################

    @abstractmethod
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
        with self.transaction('Setting key "%s"' % key):
            self.set_metadata(key, metadata)
            self.set_data(key, data)


    @abstractmethod
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
        raise NotImplementedError
            

    @abstractmethod
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
        raise NotImplementedError
        

    @abstractmethod
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
        raise NotImplementedError
        

    @abstractmethod
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
        raise NotImplementedError

        
    ##########################################################################
    # Multiple-key Methods
    ##########################################################################
            
    def multiset(self, keys, values, buffer_size=1048576):
        """ Set the data and metadata for a collection of keys.
        
        Where supported by an implementation, this should perform the whole
        collection of sets as a single transaction.
        
        Like zip() if keys and values have different lengths, then any excess
        values in the longer list should be silently ignored.
        
        Parameters
        ----------
        keys : iterable of strings
            The keys for the resources in the key-value store.  Each key is a
            unique identifier for a resource within the key-value store.
        values : iterable of (file-like, dict) tuples
            An iterator that provides the (data, metadata) pairs for the
            corresponding keys.
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
            emitted with the key & metadata for each key that was set.

        """
        with self.transaction('Setting '+', '.join('"%s"' % key for key in keys)):
            for key, value in izip(keys, values):
                self.set(key, value, buffer_size)
    
   
    def multiset_data(self, keys, datas, buffer_size=1048576):
        """ Set the data for a collection of keys.
        
        Where supported by an implementation, this should perform the whole
        collection of sets as a single transaction.
                
        Like zip() if keys and datas have different lengths, then any excess
        values in the longer list should be silently ignored.

        Parameters
        ----------
        keys : iterable of strings
            The keys for the resources in the key-value store.  Each key is a
            unique identifier for a resource within the key-value store.
        datas : iterable of file-like objects
            An iterator that provides the data file-like objects for the
            corresponding keys.
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
            emitted with the key & metadata for each key that was set.

        """
        with self.transaction('Setting data for '+', '.join('"%s"' % key for key in keys)):
            for key, data in izip(keys, datas):
                self.set_data(key, data, buffer_size)
    
   
    def multiset_metadata(self, keys, metadatas):
        """ Set the metadata for a collection of keys.
        
        Where supported by an implementation, this should perform the whole
        collection of sets as a single transaction.
                
        Like zip() if keys and metadatas have different lengths, then any excess
        values in the longer list should be silently ignored.

        Parameters
        ----------
        keys : iterable of strings
            The keys for the resources in the key-value store.  Each key is a
            unique identifier for a resource within the key-value store.
        metadatas : iterable of dicts
            An iterator that provides the metadata dictionaries for the
            corresponding keys.

        Events
        ------
        StoreSetEvent :
            On successful completion of a transaction, a StoreSetEvent should be
            emitted with the key & metadata for each key that was set.

        """
        with self.transaction('Setting metadata for '+', '.join('"%s"' % key for key in keys)):
            for key, metadata in izip(keys, metadatas):
                self.set_metadata(key, metadata)
   
   
    def multiupdate_metadata(self, keys, metadatas):
        """ Update the metadata for a collection of keys.
        
        Where supported by an implementation, this should perform the whole
        collection of sets as a single transaction.
                
        Like zip() if keys and metadatas have different lengths, then any excess
        values in the longer list should be silently ignored.

        Parameters
        ----------
        keys : iterable of strings
            The keys for the resources in the key-value store.  Each key is a
            unique identifier for a resource within the key-value store.
        metadatas : iterable of dicts
            An iterator that provides the metadata dictionaries for the
            corresponding keys.
        
        Events
        ------
        StoreSetEvent :
            On successful completion of a transaction, a StoreSetEvent should be
            emitted with the key & metadata for each key that was set.

        """
        with self.transaction('Updating metadata for '+', '.join('"%s"' % key for key in keys)):
            for key, metadata in izip(keys, metadatas):
                self.update_metadata(key, metadata)
    
   
    ##########################################################################
    # Transaction Methods
    ##########################################################################
        
    @abstractmethod
    def transaction(self, notes):
        """ Provide a transaction context manager
        
        Implementations which have no native notion of transactions may choose
        not to implement this.
        
        This method provides a context manager which creates a data store
        transaction in its __enter__() method, and commits it in its __exit__()
        method if no errors occur.  Intended usage is::
        
            with repo.transaction("Writing data..."):
                # everything written in this block is part of the transaction
                ...
            
        If the block exits without error, the transaction commits, otherwise
        the transaction should roll back the state of the underlying data store
        to the start of the transaction.
        
        Parameters
        ----------
        notes : string
            Some information about the transaction, which may or may not be used
            by the implementation.
        
        Returns
        -------        
        transaction : context manager
            A context manager for the transaction.
        
        Events
        ------        
        StoreTransactionStartEvent :
            This event should be emitted on entry into the transaction.
        StoreProgressStartEvent :
            For buffering implementations, this event should be emitted prior to
            writing any data to the underlying store.
        StoreProgressStepEvent :
            For buffering implementations, this event should be emitted
            periodically as data is written to the underlying store.
        StoreProgressEndEvent :
            For buffering implementations, this event should be emitted after
            finishing writing to the underlying store.
        StoreTransactionEndEvent :
            This event should be emitted on successful conclusion of the
            transaction, before any Set or Delete events are emitted.
        StoreSetEvent :
            On successful completion of a transaction, a StoreSetEvent should be
            emitted with the key & metadata for each key that was set during the
            transaction.
        StoreDeleteEvent :
            On successful completion of a transaction, a StoreDeleteEvent should
            be emitted with the key for all deleted keys.

        """
        raise NotImplementedError
        

    ##########################################################################
    # Utility Methods
    ##########################################################################                        
        
    def from_file(self, key, path, buffer_size=1048576):
        """ Efficiently read data from a file into a key in the key-value store.
        
        This method can be optionally overriden by subclasses to proved a more
        efficient way of copy the data from a path in the filesystem to the
        underlying data store.   The default implementation uses the set() method
        together with chunked reads from the disk which are fed into the data
        stream.
        
        This makes no attempt to set metadata.
        
        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        path : string
            A file system path to read the data from.
        buffer_size : int
            An optional indicator of the number of bytes to read at a time.
            Implementations are free to ignore this hint or use a different
            default if they need to.  The default is 1048576 bytes (1 MiB).
        
        """
        with open(path, 'rb') as fp:
            self.set_data(key, fp, buffer_size=buffer_size)


    def from_bytes(self, key, data, buffer_size=1048576):
        """ Efficiently store a bytes object as the data associated with a key.
        
        This method can be optionally overriden by subclasses to proved a more
        efficient way of copy the data from a bytes object to the underlying
        data store.  The default implementation uses the set() method
        together with a cStringIO.
        
        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        data : bytes
            The data as a bytes object.
        buffer_size : int
            An optional indicator of the number of bytes to read at a time.
            Implementations are free to ignore this hint or use a different
            default if they need to.  The default is 1048576 bytes (1 MiB).
                
        """
        self.set_data(key, StringIO(data), buffer_size)
