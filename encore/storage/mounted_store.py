#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#
"""
MountedStore
============

A store which combines two stores by mounting one of the stores at a particular
point in the other store's key space, prefixing all references to keys with
the mount point.  This is similar in concept to mounting filesystems.

"""


from .abstract_store import AbstractStore

class MountedStore(AbstractStore):
    """ A key-value store that mounts another store at a particular key prefix

    The backing store is treated as read-only, and only modifications are
    allowed to the first store, and only for keys which match the mounting
    prefix.

    The primary purpose for this is to have a local cache of a subsection of
    a remote store, such as a StaticURLStore or DynamicURLStore.

    Parameters
    ----------
    mount_point : str
        Key prefix for the mounted store.
    mount_store : AbstractStore
        The store to be mounted
    backing_store : AbstractStore
        The store that we are mounting against

    """

    # MountedStore interface

    def push(self, key):
        """ Move a key from the mount store to the backing store """
        if key.startswith(self.mount_point):
            short_key = key[len(self.mount_point):]
            self.backing_store.set(key, self.mount_store.get(short_key))
            self.mount_store.delete(short_key)
            return
        raise KeyError(key)


    # `AbstractStore` Interface

    def __init__(self, mount_point, mount_store, backing_store):
        super(MountedStore, self).__init__()
        self.mount_store = mount_store
        self.backing_store = backing_store
        self.stores = [mount_store, backing_store]
        self.mount_point = mount_point

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
        if key.startswith(self.mount_point):
            short_key = key[len(self.mount_point):]
            if self.mount_store.exists(short_key):
                return self.mount_store.get(short_key)

        if self.backing_store.exists(key):
            return self.backing_store.get(key)
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
        super(MountedStore, self).set(key, value, buffer_size)


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
        if key.startswith(self.mount_point):
            short_key = key[len(self.mount_point):]
            if self.mount_store.exists(short_key):
                return self.mount_store.delete(short_key)

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
        if key.startswith(self.mount_point):
            short_key = key[len(self.mount_point):]
            if not self.mount_store.exists(short_key) and self.backing_store.exists(key):
                self.mount_store.set(short_key, self.backing_store.get(key))
            return self.mount_store.set_data(short_key, data, buffer_size)

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
        if key.startswith(self.mount_point):
            short_key = key[len(self.mount_point):]
            if not self.mount_store.exists(short_key) and self.backing_store.exists(key):
                self.mount_store.set(short_key, self.backing_store.get(key))
            return self.mount_store.set_metadata(short_key, metadata)

        raise KeyError(key)


    def update_metadata(self, key, metadata):
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
        if key.startswith(self.mount_point):
            short_key = key[len(self.mount_point):]
            if not self.mount_store.exists(short_key) and self.backing_store.exists(key):
                self.mount_store.set(short_key, self.backing_store.get(key))
            return self.mount_store.update_metadata(short_key, metadata)

        raise KeyError(key)

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
        return self.mount_store.transaction(notes)

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
        keys = set()
        for key, metadata in self.mount_store.query(select, **kwargs):
            full_key = self.mount_point+key
            yield full_key, metadata
            keys.add(full_key)

        for key, metadata in self.backing_store.query(select, **kwargs):
            if key not in keys:
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
        keys = set()
        for key in self.mount_store.query_keys(**kwargs):
            full_key = self.mount_point+key
            yield full_key
            keys.add(full_key)

        for key in self.backing_store.query_keys(**kwargs):
            if key not in keys:
                yield key
