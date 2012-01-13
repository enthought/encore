#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

"""
Simple Authenticating Store
===========================


"""

import hashlib

from .abstract_store import AbstractStore
from .utils import DummyTransactionContext

def make_encoder(salt, hasher=lambda s: hashlib.sha1(s).disgest()):
    """ Create a moderately secure salted encoder
    """
    return eval("lambda password: hasher("+repr(salt)+"+password)")

class SimpleAuthStore(AbstractStore):
    """ A key-value store that joins together several other Key-Value Stores
    
    A joined store is a composite store which takes a list of stores and
    presents a set of keys that is the union of all the keys that are available
    in all the stores.  When a key is available in multiple stores, then the
    store which comes first in the list has priority.
    
    All writes are performed into the first store in the list.
    
    Parameters
    ----------
    event_manager :
        An event_manager which implements the :py:class:`~.abstract_event_manager.BaseEventManager`
        API.
    stores : list of stores
        The stores that are joined together by this store.
        
    """
    def __init__(self, event_manager, store, encoder, user_key_path='.user_'):
        self.event_manager = event_manager
        self.store = store
        self.encoder = encoder
        self.user_key_path
        
        self._username = None
        self._token = None
        self._connected = False

                                
    def connect(self, credentials=None):
        """ Connect to the key-value store, optionally with authentication
        
        This method creates or connects to any long-lived resources that the
        store requires.
        
        Parameters
        ----------
        credentials :
            A dictionary with keys 'username' and 'password'.
            
        """
        self._username = credentials['username']
        self._token = self.encoder(credentials['password'])
        
        if 'connect' not in self._check_permissions():
            raise AuthenticationError('User "%s" is not permitted to connect')
        self._connected = True
        
        
    def disconnect(self):
        """ Disconnect from the key-value store
        
        This method disposes or disconnects to any long-lived resources that the
        store requires.
        
        """
        self._connected = False
        self._token = None
        self.store = None


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
            'readonly': self.store.info.get('readonly', True),
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
            key-value store
        metadata : dictionary
            A dictionary of metadata for the key.
        
        Raises
        ------
        KeyError :
            If the key is not found in the store, or does not exist for the user,
            a KeyError is raised.
        AuthenticationError :
            If the user has no rights to get the key, then an Authentication error is raised.

        """
        permissions = self._check_permissions(key)
        if 'exists' in permissions:
            if 'get' in permissions:
                return self.store.get(key)
            else:
                raise AuthenticationError('User "%s" is not permitted to get "%s"' % (self._username, key))
        else:
            raise KeyError(key)

    
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
        buffer_size :  int
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
        permissions = self._check_permissions(key)
        if 'exists' in permissions:
            if 'set' in permissions:
                return self.store.set(key, value, buffer_size)
            else:
                raise AuthenticationError('User "%s" is not permitted to set "%s"' % (self._username, key))
        else:
            raise KeyError(key)

    
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
        permissions = self._check_permissions(key)
        if 'exists' in permissions:
            if 'delete' in permissions:
                return self.store.delete(key)
            else:
                raise AuthenticationError('User "%s" is not permitted to delete "%s"' % (self._username, key))
        else:
            raise KeyError(key)

    
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
        permissions = self._check_permissions(key)
        if 'exists' in permissions:
            if 'get' in permissions:
                return self.store.get_data(key)
            else:
                raise AuthenticationError('User "%s" is not permitted to get "%s"' % (self._username, key))
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
        permissions = self._check_permissions(key)
        if 'exists' in permissions:
            if 'get' in permissions:
                return self.store.get_metadata(key, select)
            else:
                raise AuthenticationError('User "%s" is not permitted to get "%s"' % (self._username, key))
        else:
            raise KeyError(key)
            

    
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
        buffer_size :  int
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
        permissions = self._check_permissions(key)
        if 'exists' in permissions:
            if 'set' in permissions:
                return self.store.set_data(key, value, buffer_size)
            else:
                raise AuthenticationError('User "%s" is not permitted to set "%s"' % (self._username, key))
        else:
            raise KeyError(key)

    
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
        permissions = self._check_permissions(key)
        if 'exists' in permissions:
            if 'set' in permissions:
                return self.store.set_metadata(key, value, buffer_size)
            else:
                raise AuthenticationError('User "%s" is not permitted to set "%s"' % (self._username, key))
        else:
            raise KeyError(key)

    
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
        permissions = self._check_permissions(key)
        if 'exists' in permissions:
            if 'set' in permissions:
                return self.store.update_metadata(key, value, buffer_size)
            else:
                raise AuthenticationError('User "%s" is not permitted to set "%s"' % (self._username, key))
        else:
            raise KeyError(key)
            
    
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
        permissions = self._check_permissions(key)
        if 'exists' in permissions:
            return self.store.exists(key)
        else:
            return False


    ##########################################################################
    # Private Methods
    ##########################################################################

    def _check_permissions(self, key=None):
        """ Return permissions that the user has for the provided key
        
        Parameters
        ----------
        key : str or None
            The key which the permissions are being requested for, or the global
            permissions if the key is None.
        """
        if self._username:
            user_key = self.user_key_path + self._username
            try:
                token = self.store.get(user_key).read()
            except KeyError:
                return set()
            if self._token == token:
                return set(['connect', 'exists', 'get', 'set', 'delete'])
        return set()
