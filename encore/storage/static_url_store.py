#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

"""
Static URL Store
================

This module contains the :py:class:`~StaticURLStore` store that communicates
with a remote HTTP server which provides the actual data storage.  This is a
simple read-only store that can be run against a static HTTP server which
provides a json file with all metadata and then serves data from URLs from
another path.  The metadata URL is polled periodically for updates.

A typical static server might be layed out as::

    base_directory/
        index.json
        data/
            key1
            key2
            ...

"""

import threading
import json
import urllib
import time

from .abstract_store import AbstractReadOnlyStore
from .events import StoreUpdateEvent, StoreSetEvent, StoreDeleteEvent
from .url_value import URLValue
from .utils import add_context_manager_support


def basic_auth_factory(**kwargs):
    """ A factory that creates a :py:class:`~.HTTPBasicAuthHandler` instance

    The arguments are passed directly through to the :py:meth:`add_password`
    method of the handler.

    """
    auth_handler = urllib.request.HTTPBasicAuthHandler()
    auth_handler.add_password(**kwargs)


class StaticURLStore(AbstractReadOnlyStore):
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

    All queries are performed using urllib.urlopen, so this store can be
    implemented by an HTTP, FTP or file server which serves static files.  When
    connecting, if appropriate credentials are supplied then HTTP authentication
    will be used when connecting the remote server

    .. warning::

        Since we use urllib without any further modifications, HTTPS requests
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
    def __init__(self, root_url, data_path, query_path, poll=300):
        super(StaticURLStore, self).__init__()
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

        This method creates appropriate urllib openers for the store.

        Parameters
        ----------
        credentials : dict
            A dictionary which has at least keys 'username' and 'password'
            and optional keys 'uri' and 'realm'.  The 'uri' will default to
            the root url of the store, and 'realm' will default to
            'encore.storage'.
        proxy_handler : urllib.ProxyHandler
            An optional urllib.ProxyHandler instance.  If none is provided
            then urllib will create a proxy handler from the user's environment
            if needed.
        auth_handler_factory :
            An optional factory to build urllib authenticators.  The credentials
            will be passed as keyword arguments to this handler's add_password
            method.

        """
        if credentials is not None:
            if auth_handler_factory is None:
                auth_handler_factory = urllib.request.HTTPBasicAuthHandler
            args = {'uri': self.root_url, 'realm': 'encore.storage'}
            args.update(credentials)
            auth_handler = auth_handler_factory()
            auth_handler.add_password(**args)
            if proxy_handler is None:
                self._opener = urllib.request.build_opener(auth_handler)
            else:
                self._opener = urllib.request.build_opener(proxy_handler, auth_handler)
        else:
            if proxy_handler is None:
                self._opener = urllib.request.build_opener()
            else:
                self._opener = urllib.request.build_opener(auth_handler)

        self.update_index()
        if self.poll > 0:
            self._index_thread = threading.Thread(target=self._poll)
            self._index_thread.start()


    def disconnect(self):
        """ Disconnect from the key-value store

        This method disposes or disconnects to any long-lived resources that the
        store requires.

        """

        if self._index_thread is not None:
            self._index_thread.join()
            self._index_thread = None

        self._opener = None

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
        return {
            'readonly': True
        }


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
            by urllib's urlopen function.
        metadata : dictionary
            A dictionary of metadata for the key.

        Raises
        ------
        KeyError :
            If the key is not found in the store, a KeyError is raised.

        """
        url = self.root_url + urllib.parse.quote(self.data_path + key)
        with self._index_lock:
            metadata = self._index[key].copy()
        return URLValue(url, metadata, self._opener)


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
            by urllib's urlopen function.

        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.

        """
        if self.exists(key):
            url = self.root_url + urllib.parse.quote(self.data_path + key)
            stream = self._opener.open(url)
            add_context_manager_support(stream)
            return stream
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
            # Py3: http.client.HTTPResponse always returns bytes --> convert to
            # str/unicode to make sure loads is happy
            index = json.loads(result.read().decode('ascii'))
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
