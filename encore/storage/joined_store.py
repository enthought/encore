#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

"""
Joined Store
============


"""

from .abstract_store import AbstractAuthorizingStore
from .utils import DummyTransactionContext

class JoinedStore(AbstractAuthorizingStore):
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
    def __init__(self, stores):
        super(JoinedStore, self).__init__()
        self.stores = stores


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
        for store in self.stores:
            if not store.is_connected():
                store.connect(credentials)
        self._connected = True


    def disconnect(self):
        """ Disconnect from the key-value store

        This method disposes or disconnects to any long-lived resources that the
        store requires.

        """
        self._connected = False
        self.stores = []


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
            'readonly': False,
        }

    def user_tag(self):
        return self.stores[0].user_tag()


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
        for store in self.stores:
            if store.exists(key):
                return store.get(key)
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
        data, metadata = value
        with self.transaction('Setting key "%s"' % key):
            self.set_data(key, data, buffer_size)
            self.set_metadata(key, metadata)



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
        for store in self.stores:
            if store.exists(key):
                store.delete(key)
                return
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
        for store in self.stores:
            if store.exists(key):
                return store.get_data(key)
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
        for store in self.stores:
            if store.exists(key):
                return store.get_metadata(key, select)
        else:
            raise KeyError(key)


    def get_permissions(self, key):
        """ Return the set of permissions the user has

        Parameters
        ----------
        key : str
            The key for the resource which you want to know the permissions.

        Returns
        -------
        permissions : dict of str: set of str
            A dictionary whose keys are the permissions and values are sets of
            tags which have that permission.

        Raises
        ------
        KeyError :
            This error will be raised if the key does not exist or the user is
            not authorized to see it.

        AuthorizationError :
            This error will be raised if user is authorized to see the key, but
            is not an owner.

        """
        for store in self.stores:
            if store.exists(key):
                return store.get_permissions(key)
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

        # Tries to set data on first store that will allow for it (eg spaces).
        for store in self.stores:
            try:
                store.set_data(key, data, buffer_size)
            except KeyError:
                pass
            else:
                return
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
        for store in self.stores:
            if store.exists(key):
                store.set_metadata(key, metadata)
                return
        raise KeyError(key)


    def set_permissions(self, key, permissions):
        """ Set the permissions on a key the user owns

        Parameters
        ----------
        key : str
            The key for the resource which you want to know the permissions.

        permissions : dict of str: set of str
            A dictionary whose keys are the permissions and values are sets of
            tags which have that permission.  There must be an 'owned'
            permission with at least one tag.

        Raises
        ------
        KeyError :
            This error will be raised if the key does not exist or the user is
            not authorized to see it.

        AuthorizationError :
            This error will be raised if user is authorized to see the key, but
            is not an owner.

        """
        for store in self.stores:
            if store.exists(key):
                store.set_permissions(key, permissions)
                return
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
        if self.stores:
            current_metadata = self.get_metadata(key)
            current_metadata.update(metadata)
            self.set_metadata(key, current_metadata)
        else:
            raise KeyError(key)


    def update_permissions(self, key, permissions):
        """ Add permissions on a key the user owns

        The tags provided in the permissions dictionary will be added to the
        existing set of tags for each permission.

        Parameters
        ----------
        key : str
            The key for the resource which you want to know the permissions.

        permissions : dict of str: set of str
            A dictionary whose keys are the permissions and values are sets of
            tags which have that permission.


        Raises
        ------
        KeyError :
            This error will be raised if the key does not exist or the user is
            not authorized to see it.

        AuthorizationError :
            This error will be raised if user is authorized to see the key, but
            is not an owner.

        """

        for store in self.stores:
            if store.exists(key):
                store.update_permissions(key, permissions)
                return
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
        for store in self.stores:
            if store.exists(key):
                return True
        else:
            return False


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
        if self.stores:
            self.stores[0].multiset(keys, values, buffer_size)


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
        if self.stores:
            self.stores[0].multiset_data(keys, datas)


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
        if self.stores:
            self.stores[0].multiset_metadata(keys, metadatas)


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
        if self.stores:
            self.stores[0].multiupdate_metadata(keys, metadatas)


    ##########################################################################
    # Transaction Methods
    ##########################################################################

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
        return DummyTransactionContext(self)

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
        for i, store in enumerate(self.stores):
            for key, metadata in store.query(select, **kwargs):
                if all(not s.exists(key) for s in self.stores[:i]):
                    yield key, metadata


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
        for i, store in enumerate(self.stores):
            for key in store.query_keys(**kwargs):
                if not any(s.exists(key) for s in self.stores[:i]):
                    yield key


    ##########################################################################
    # Utility Methods
    ##########################################################################

    # superclass methods are fine
