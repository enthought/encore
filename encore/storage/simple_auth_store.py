#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

"""
Simple Authenticating Store
===========================

This module provides a simple wrapper for a store that implements a simple
authentication scheme.  This may be used as a base for more complex and fine-grained
authentication.

By default it authenticates by computing a (salted) hash of the user's password
and validates it against the hash stored in an appropriate key.  Authenticated
users then have full access to all keys.

Subclasses can refine this behaviour by overriding the check_permissions()
method to provide different or more controlled permissioning.

"""

import hashlib

from .abstract_store import AbstractStore

class AuthenticationError(Exception):
    pass


def sha1_hasher(s):
    """ A simple utility function for producing a sha1 digest of a string. """
    return hashlib.sha1(s).digest()


def make_encoder(salt, hasher=None):
    """ Create a moderately secure salted encoder

    Parameters
    ----------
    salt : bytes
        A salt that is added to the user-supplied password before hashing.
        This salt should be kept secret, but needs to be remembered across
        invocations (ie. the same salt needs to be used every time the password
        is encoded).
    hasher : callable
        A callable that takes a string and returns a cryptographic hash of the
        string.  The default is :py:func:`sha1_hasher`.

    """
    # we eval so that the value of the salt is a little more obscured.  An
    # attacker who knows what they are doing can probably get at the value, but
    # if they have the level of access required to see this function then they
    # probably have access to the raw underlying store as well.
    if hasher is None:
        hasher = sha1_hasher
    return eval("lambda password: hasher("+repr(salt)+"+password)", {'hasher': hasher})


class SimpleAuthStore(AbstractStore):
    """ A key-value store that wraps another store and implements simple authentication

    This wraps an existing store with no notion of authentication and provides
    simple username/password authentication, storing a hash of the password in
    the wrapped store.

    The base implementation has all-or-nothing

    Parameters
    ----------
    event_manager :
        An event_manager which implements the :py:class:`~.abstract_event_manager.BaseEventManager`
        API.
    store : AbstractStore instance
        The wrapped store that actually holds the data.
    encoder : callable
        A callable that computes the password hash.
    user_key_path : str
        The prefix to put before the username for the keys that store the user's
        information.  At present these keys must simply hold the encoded hash of
        the user's password.
    user_key_store : AbstractStore instance
        The store to store the user keys in.  Defaults to the wrapped store.

    """
    def __init__(self, store, encoder, user_key_path='.user_', user_key_store=None):
        super(SimpleAuthStore, self).__init__()
        self.store = store
        self.encoder = encoder
        self.user_key_path = user_key_path
        self.user_key_store = store if user_key_store is None else user_key_store

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
        # We only support utf-8 encoded byte strings for the encoding
        self._token = self.encoder(credentials['password'].encode('utf-8'))

        if 'connect' not in self.check_permissions():
            raise AuthenticationError('User "%s" is not authenticated for connection' % self._username)
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
        permissions = self.check_permissions(key)
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

        Raises
        ------
        AuthenticationError :
            If the user has no rights to set the key, then an Authentication error is raised.

        """
        permissions = self.check_permissions(key)
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

        Raises
        ------
        AuthenticationError :
            If the user has no rights to delete the key, then an Authentication error is raised.

        """
        permissions = self.check_permissions(key)
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
        AuthenticationError :
            If the user has no rights to get the key, then an Authentication error is raised.


        """
        permissions = self.check_permissions(key)
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
        AuthenticationError :
            If the user has no rights to get the key, then an Authentication error is raised.

        """
        permissions = self.check_permissions(key)
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

        Raises
        ------
        AuthenticationError :
            If the user has no rights to set the key, then an Authentication error is raised.

        """
        permissions = self.check_permissions(key)
        if 'exists' in permissions:
            if 'set' in permissions:
                return self.store.set_data(key, data, buffer_size)
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

        Raises
        ------
        AuthenticationError :
            If the user has no rights to set the key, then an Authentication error is raised.

        """
        permissions = self.check_permissions(key)
        if 'exists' in permissions:
            if 'set' in permissions:
                return self.store.set_metadata(key, metadata)
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

        Raises
        ------
        AuthenticationError :
            If the user has no rights to set the key, then an Authentication error is raised.

        """
        permissions = self.check_permissions(key)
        if 'exists' in permissions:
            if 'set' in permissions:
                return self.store.update_metadata(key, metadata)
            else:
                raise AuthenticationError('User "%s" is not permitted to set "%s"' % (self._username, key))
        else:
            raise KeyError(key)


    def exists(self, key):
        """ Test whether or not a key exists in the key-value store

        If a user does not have 'exists' permissions for this key, then it will
        return ``False``, even if the key exists in the underlying store.

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
        permissions = self.check_permissions(key)
        if 'exists' in permissions:
            return self.store.exists(key)
        else:
            return False


    def transaction(self, note):
        return self.store.transaction(note)

    def query(self, select=None, **kwargs):
        for key, metadata in self.store.query(select, **kwargs):
            if self.exists(key):
                yield key, metadata

    def query_keys(self, **kwargs):
        for key in self.store.query_keys(**kwargs):
            if self.exists(key):
                yield key

    ##########################################################################
    # Private Methods
    ##########################################################################

    def check_permissions(self, key=None):
        """ Return permissions that the user has for the provided key

        The default behaviour gives all authenticated users full access to all
        keys.  Subclasses may implement finer-grained controls based on user
        groups or other permissioning systems.

        Parameters
        ----------
        key : str or None
            The key which the permissions are being requested for, or the global
            permissions if the key is None.

        Returns
        -------
        permissions : set
            A set of strings chosen from 'connect', 'exists', 'get', 'set', and/or
            'delete' which express the permissions that the user has on that
            particular key.

        """
        if self._username:
            user_key = self.user_key_path + self._username
            try:
                token = self.user_key_store.get_data(user_key).read()
            except KeyError:
                return set()
            if self._token == token:
                return set(['connect', 'exists', 'get', 'set', 'delete'])
        return set()
