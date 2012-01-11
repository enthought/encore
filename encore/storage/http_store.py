#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

"""
HTTP Stores
============

This module contains several different stores that communicate with a remote
HTTP server which provides the actual data storage.

The simplest of these is the :py:class:`~HTTPStore` which can be run
against a static HTTP server which provides a json file with all metadata and
then serves data from URLS from another path.  It polls the metadata periodically
for updates.

"""

import threading
import json
import httplib
import urllib2
import urllib
import time

from .abstract_store import AbstractStore
from .utils import StoreProgressManager, buffer_iterator, DummyTransactionContext
from .events import StoreUpdateEvent, StoreSetEvent, StoreDeleteEvent


def basic_auth_factory(**kwargs):
    """ A factory that creates a :py:class:`~.HTTPBasicAuthHandler` instance
    
    The arguments are passed directly through to the :py:meth:`add_password`
    method of the handler.
    
    """
    auth_handler = urllib2.HTTPBasicAuthHandler()
    auth_handler.add_password(**kwargs)


class StoreRequest(urllib2.Request):
    def get_method(self):
        return self._method

class HTTPStore(AbstractStore):
    """ A read-only key-value store that is a front end for data served via URLs
    
    All data is assumed to be served from some root url.  In addition
    the store requires knowledge of two paths: a data prefix URL which is a 
    partial URL to which the keys will be appended when requesting data, and a
    query URL which is a single URL which provides all metadata as a json
    encoded file.
    
    For example, an HTTP server may store data at URLs of the form::
    
        http://www.example.com/data/<key>
    
    and may store the metadata at::
    
        http://www.example.com/index.json
    
    These would have a root url of "http://www.example.com/", a data path
    of "data/" and a query path of "index.json".
    
    All queries are performed using urllib2.urlopen, so this store can be
    implemented by an HTTP, FTP or file server which serves static files.  When
    connecting, if appropriate credentials are supplied then HTTP authentication
    will be used when connecting the remote server
   
    .. warning::
    
        Since we use urllib2 without any further modifications, HTTPS requests
        do not validate the server's certificate.
    
    Because of the limited nature of the interface, this store implementation
    is read only, and handles updates via periodic polling of the query prefix
    URL.  This guarantees that the viewed data is always consistent, it just may
    not be current. Most of the work of querying is done on the client side
    using the cached metadata.
     
    Parameters
    ----------
    event_manager :
        An event_manager which implements the :py:class:`~.abstract_event_manager.BaseEventManager`
        API.
    root_url : str
        The base url that data is served from.
    data_path : str
        The URL prefix that the data is served from.
    query_path : str
        The URL that the metadata is served from.
    poll : float
        The polling frequency for the polling thread.  Polls every 5 min by default.
    
        
    """
    def __init__(self, event_manager, root_url, data_path, query_path, poll=300):
        self.event_manager = event_manager
        self.root_url = root_url
        self.data_path = data_path
        self.query_path = query_path
        self.poll = poll
        
        self._opener = None
        self._index = None
        self._index_lock = threading.Lock()
        self._index_thread = None

                
    def connect(self, credentials=None, proxy_handler=None, auth_handler_factory=None):
        """ Connect to the key-value store, optionally with authentication
        
        This method creates appropriate urllib2 openers for the store.
        
        Parameters
        ----------
        credentials : dict
            A dictionary which has at least keys 'username' and 'password'
            and optional keys 'uri' and 'realm'.  The 'uri' will default to
            the root url of the store, and 'realm' will default to
            'encore.storage'.
        proxy_handler : urllib2.ProxyHandler
            An optional urllib2.ProxyHandler instance.  If none is provided
            then urllib2 will create a proxy handler from the user's environment
            if needed.
        auth_handler_factory :
            An optional factory to build urllib2 authenticators.  The credentials
            will be passed as keyword arguments to this handler's add_password
            method.
            
        """
        if credentials is not None:
            if auth_handler_factory is None:
                auth_handler_factory = urllib2.HTTPBasicAuthHandler
            args = {'uri': self.root_url, 'realm': 'encore.storage'}
            args.update(credentials)
            auth_handler = auth_handler_factory()
            auth_handler.add_password(**args)
            if proxy_handler is None:
                self._opener = urllib2.build_opener(auth_handler)
            else:
                self._opener = urllib2.build_opener(proxy_handler, auth_handler)
        else:
            if proxy_handler is None:
                self._opener = urllib2.build_opener()
            else:
                self._opener = urllib2.build_opener(auth_handler)
        
        self.update_index()
        if self.poll > 0:
            self._index_thread = threading.Thread(target=self._poll)
            self._index_thread.start()

        
    def disconnect(self):
        """ Disconnect from the key-value store
        
        This method disposes or disconnects to any long-lived resources that the
        store requires.
        
        """
        self._opener = None
        
        if self._index_thread is not None:
            self._index_thread.join()
            self._index_thread = None


    def is_connected(self):
        """ Whether or not the store is currently connected
        
        Returns
        -------
        connected : bool
            Whether or not the store is currently connected.

        """
        return self._opener is not None

    
    def info(self):
        """ Get information about the key-value store
        
        Returns
        -------
        metadata : dict
            A dictionary of metadata giving information about the key-value store.
        """
        return {}

        
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
            key-value store.  This is the same type of filelike object returned
            by urllib2's urlopen function.
        metadata : dictionary
            A dictionary of metadata for the key.
        
        Raises
        ------
        KeyError :
            If the key is not found in the store, a KeyError is raised.

        """
        metadata = self.get_metadata(key)
        data = self.get_data(key)
        return (data, metadata)
        
    
    def set(self, key, value, buffer_size=1048576):
        """ Store a stream of data into a given key in the key-value store.
        
        This is unimplemented.
        
        """
        raise NotImplementedError

    
    def delete(self, key):
        """ Delete a key from the repsository.
        
        This is unimplemented
                
        """
        raise NotImplementedError

    
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
            key-value store.  This is the same type of filelike object returned
            by urllib2's urlopen function.

        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.

        """
        if self.exists(key):
            url = self.root_url + urllib.quote(self.data_path + key)
            return self._opener.open(url)
        else:
            raise KeyError(key)

    
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
        with self._index_lock:
            if select is None:
                return self._index[key].copy()
            else:
                metadata = self._index[key]
                return dict((s, metadata[s]) for s in select if s in metadata)

    
    def set_data(self, key, data, buffer_size=1048576):
        """ Replace the data for a given key in the key-value store.
        
        This is unimplemented
                
        """
        raise NotImplementedError
        
    
    def set_metadata(self, key, metadata):
        """ Set new metadata for a given key in the key-value store.
        
        This is unimplemented
                
        """
        raise NotImplementedError

    
    def update_metadata(self, key, metadata):
        """ Update the metadata for a given key in the key-value store.
        
        This is unimplemented
                
        """
        raise NotImplementedError
        
                
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
        with self._index_lock:
            return key in self._index

        
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
        return super(HTTPStore, self).multiget(keys)
   
    
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
        return super(HTTPStore, self).multiget_data(keys)

    
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
        return super(HTTPStore, self).multiget_metadata(keys, select)
            
    
    def multiset(self, keys, values, buffer_size=1048576):
        """ Set the data and metadata for a collection of keys.
        
        This is unimplemented

        """
        raise NotImplementedError   
    
    def multiset_data(self, keys, datas, buffer_size=1048576):
        """ Set the data for a collection of keys.
        
        This is unimplemented

        """
        raise NotImplementedError   
   
    
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
        raise NotImplementedError  
   
    
    def multiupdate_metadata(self, keys, metadatas):
        """ Update the metadata for a collection of keys.
        
        This is unimplemented

        """
        raise NotImplementedError   
    
   
    ##########################################################################
    # Transaction Methods
    ##########################################################################
    
    def transaction(self, notes):
        """ Provide a transaction context manager
        
        This is unimplemented

        """
        raise NotImplementedError   
        
    ##########################################################################
    # Querying Methods
    ##########################################################################
        
    
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
        with self._index_lock:
            if select is not None:
                for key, metadata in self._index.items():
                    if all(metadata.get(arg) == value for arg, value in kwargs.items()):
                        yield key, dict((metadata_key, metadata[metadata_key])
                            for metadata_key in select if metadata_key in metadata)
            else:
                for key, metadata in self._index.items():
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
        with self._index_lock:
            for key, metadata in self._index.items():
                if all(metadata.get(arg) == value for arg, value in kwargs.items()):
                    yield key

    
    ##########################################################################
    # Utility Methods
    ##########################################################################

    def update_index(self):
        """ Request the most recent version of the metadata
        
        This downloads the json file at the query_path location, and updates
        the local metadata cache with this information.  It then emits events
        that represent the difference between the old metadata and the new
        metadata.
        
        This method is normally called from the polling thread, but can be called
        by other code when needed.  It locks the metadata index whilst performing
        the update.
        
        """
        url = self.root_url + self.query_path
        with self._index_lock:
            result = self._opener.open(url)
            index = json.loads(result.read())
            old_index = self._index
            self._index = index

        # emit update events
        # XXX won't detect changes to data if metadata doesn't change as well!
        if old_index is not None:
            old_keys = set(old_index)
            new_keys = set(index)
            for key in (old_keys - new_keys):
                self.event_manager.emit(StoreDeleteEvent(self, key=key, metadata=old_index[key]))
            for key in (new_keys - old_keys):
                self.event_manager.emit(StoreSetEvent(self, key=key, metadata=index[key]))
            for key in (new_keys & old_keys):
                if old_index[key] != index[key]:
                    self.event_manager.emit(StoreUpdateEvent(self, key=key, metadata=index[key]))

    def from_file(self, key, path, buffer_size=1048576):
        """ Efficiently read data from a file into a key in the key-value store.
        
        Not implemented.
        
        """
        raise NotImplementedError
    
    def from_bytes(self, key, data, buffer_size=1048576):
        """ Efficiently read data from a bytes object into a key in the key-value store.
        
        Not implemented.
        
        """
        raise NotImplementedError
    
    ##########################################################################
    # Private Methods
    ##########################################################################
    
    def _poll(self):
        t = time.time()
        while self._opener is not None:
            if time.time()-t >= self.poll:
                self.update_index()
                t = time.time()
            # tick
            time.sleep(0.5)


