#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

"""
Sqlite Store
------------

This is a simple implementation of the key-value store API that lives in a sqlite
database.  Each key is stored in a row which consists of the key, index columns,
metadata and data.  The index columns are a specified subset of the metadata that
can be queried more quickly.

This class is provided in part as a sample implementation of the API.

"""


import sqlite3
import time

from io import BytesIO
import pickle

from .abstract_store import AbstractStore
from .string_value import StringValue
from .events import StoreSetEvent, StoreUpdateEvent, StoreDeleteEvent
from .utils import (
    buffer_iterator, SimpleTransactionContext, StoreProgressManager,
    add_context_manager_support
)

buffer = sqlite3.Binary


def adapt_dict(d):
    return buffer(pickle.dumps(d, protocol=2))


def convert_dict(s):
    return pickle.loads(s, encoding='ascii')

sqlite3.register_adapter(dict, adapt_dict)
sqlite3.register_converter('dict', convert_dict)


class SqliteStore(AbstractStore):
    """ Sqlite-based Store

    The file-like objects returned by data methods are cStringIO objects.

    .. warning::

        The table name and metadata names used as index columns are not sanitized.
        To prevent SQL injection these should never be directly derived from
        user-supplied values.  This is particularly important for indexed queries.
    """

    def __init__(self, location=':memory:', table='store', index='dynamic', index_columns=None):
        super(SqliteStore, self).__init__()
        self.location = location
        self.table = table

        self._index = index
        self.index_columns = set(index_columns) if index_columns is not None else set()

        self._connection = None

    def connect(self, credentials=None):
        """ Connect to the key-value store

        This connects to the specified location and creates the table, if needed.
        Sqlite has no notion of authentication, so credentials are ignored.

        """
        self._connection = sqlite3.connect(self.location, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = self._connection.execute(
            "select name from sqlite_master where type='table' and name=?",
            (self.table,)
        )
        if len(cursor.fetchall()) == 0:
            # we need to create the table (substitution OK since self.table is internal
            query = """create table %s (
                    key text primary key,
                    metadata dict,
                    created float,
                    modified float,
                    data blob
                )""" % self.table
            self._connection.execute(query)
        elif self._index is not None:
            # we need to find the names of the existing index columns
            rows = self._connection.execute('PRAGMA table_info(%s)' % self.table)
            index_columns = set(row[0] for row in rows
                if row[0] not in ('key', 'metadata', 'created', 'modified', 'data'))
            if not self.index_columns.issubset(index_columns):
                # being paranoid here
                self._build_index()

    def disconnect(self):
        """ Disconnect from the key-value store

        This clears the reference to the sqlite connection object, allowing it
        to be garbage-collected.

        """
        self._connection.close()
        self._connection = None

    def is_connected(self):
        """ Whether or not the store is currently connected

        Returns
        -------
        connected : bool
            Whether or not the store is currently connected.

        """
        return self._connection is not None

    def info(self):
        """ Get information about the key-value store

        Returns
        -------
        metadata : dict
            A dictionary of metadata giving information about the key-value store.

        """
        return {
            'readonly': False,
            'location': self.location,
            'table': self.table,
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
        row = self._get_columns_by_key(key, ['metadata', 'data', 'created', 'modified'])
        if row is None:
            raise KeyError(key)
        return StringValue(
            row['data'], row['metadata'], row['created'], row['modified']
        )

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

        """
        if isinstance(value, tuple):
            data_stream, metadata = value
            metadata = metadata.copy()
            steps = -1
        else:
            data_stream = value.data
            metadata = value.metadata
            steps = value.size
        update = self.exists(key)

        progress = StoreProgressManager(source=self, steps=steps,
                message="Setting data into '%s'" % (key,), key=key,
                metadata=metadata)

        with progress:
            chunks = list(buffer_iterator(data_stream, buffer_size, progress))
            data = buffer(b''.join(chunks))

        with self.transaction('Setting key "%s"' % key):
            if update:
                created = self._get_columns_by_key(key, ['created'])['created']
                modified = time.time()
            else:
                modified = created = time.time()
            self._insert_row(key, metadata, data, created, modified)
            self._update_index(key, metadata)

        if update:
            self.event_manager.emit(StoreUpdateEvent(self, key=key, metadata=metadata))
        else:
            self.event_manager.emit(StoreSetEvent(self, key=key, metadata=metadata))

    def delete(self, key):
        """ Delete a key from the repsository.

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.

        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.

        """
        row = self._get_columns_by_key(key, ['metadata'])
        if row is None:
            raise KeyError(key)
        metadata = row['metadata']

        with self.transaction('Deleting "%s"' % key):
            query = 'delete from %s where key=?' % self.table
            self._connection.execute(query, (key,))
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
        return self._get_columns_by_key(key, ['metadata']) is not None


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
        row = self._get_columns_by_key(key, ['data'])
        if row is None:
            raise KeyError(key)
        data = add_context_manager_support(BytesIO(row['data']))
        return data


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
        row = self._get_columns_by_key(key, ['metadata'])
        if row is None:
            raise KeyError(key)
        metadata = row['metadata']
        if select is not None:
            return dict((metadata_key, metadata[metadata_key])
                for metadata_key in select if metadata_key in metadata)
        return metadata


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
        row = self._get_columns_by_key(key, ['metadata'])
        if row is not None:
            metadata = row['metadata']
        else:
            metadata = {}
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

        """
        update = self.exists(key)
        if update:
            with self.transaction('Setting metadata for "%s"' % key):
                created = self._get_columns_by_key(key, ['created'])['created']
                modified = time.time()
                self._update_columns(key, ['metadata', 'modified'], [metadata, modified])
                self.event_manager.emit(StoreUpdateEvent(self, key=key, metadata=metadata))
        else:
            with self.transaction('Setting metadata for "%s"' % key):
                modified = created = time.time()
                self._insert_row(key, metadata, buffer(''), created, modified)
                self._update_index(key, metadata)
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

        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.

        """
        row = self._get_columns_by_key(key, ['metadata'])
        if row is not None:
            temp_metadata = row['metadata']
        else:
            raise KeyError(key)
        temp_metadata.update(metadata)
        with self.transaction('Setting metadata for "%s"' % key):
            self._update_column(key, 'metadata', temp_metadata)
            self._update_index(key, metadata)
            self.event_manager.emit(StoreUpdateEvent(self, key=key, metadata=temp_metadata))


    def transaction(self, notes):
        """ Provide a transaction context manager"""
        return SimpleTransactionContext(self)


    def _commit_transaction(self):
        self._connection.commit()


    def _rollback_transaction(self):
        self._connection.rollback()


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
        if self._index and kwargs:
            columns = list(column for column in kwargs if column in self.index_columns)
            unindexed_columns = set(kwargs) - set(columns)
            if columns:
                query = 'select key, metadata from "%s" where %s' % (self.table,
                    ' and '.join('"'+column+'"=?' for column in columns))
            else:
                query = 'select key, metadata from "%s"' % (self.table,)
            rows = self._connection.execute(query, [buffer(pickle.dumps(kwargs[column], protocol=2))
                for column in columns])
        else:
            unindexed_columns = set(kwargs)
            rows = self._connection.execute('select key, metadata from "' +
                self.table + '"')

        if select is not None:
            for key, metadata in rows:
                if all(metadata.get(arg) == kwargs[arg] for arg in unindexed_columns):
                    yield key, dict((metadata_key, metadata[metadata_key])
                        for metadata_key in select if metadata_key in metadata)
        else:
            for key, metadata in rows:
                if all(metadata.get(arg) == kwargs[arg] for arg in unindexed_columns):
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
        if self._index and kwargs:
            columns = list(column for column in kwargs if column in self.index_columns)
            unindexed_columns = set(kwargs) - set(columns)
            if unindexed_columns:
                if columns:
                    query = 'select key, metadata from "%s" where %s' % (self.table,
                        ' and '.join('"'+column+'"=?' for column in columns))
                else:
                    query = 'select key, metadata from "%s"' % (self.table,)
            else:
                if columns:
                    query = 'select key from %s where "%s"' % (self.table,
                        ' and '.join('"'+column+'"=?' for column in columns))
                else:
                    query = 'select key from "%s"' % (self.table,)
            rows = self._connection.execute(query, [buffer(pickle.dumps(kwargs[column], protocol=2))
                for column in columns])
        else:
            unindexed_columns = set(kwargs)
            if unindexed_columns:
                rows = self._connection.execute('select key, metadata from "'
                    + self.table + '"')
            else:
                rows = self._connection.execute('select key from "' +
                    self.table + '"')

        if unindexed_columns:
            for key, metadata in rows:
                if all(metadata.get(arg) == kwargs[arg] for arg in unindexed_columns):
                    yield key
        else:
            for key, in rows:
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

        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.

        """
        return super(SqliteStore, self).to_file(key, path, buffer_size)


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
        return super(SqliteStore, self).from_file(key, path, buffer_size)

    def to_bytes(self, key, buffer_size=1048576):
        """ Efficiently store the data associated with a key into a bytes object.

        Parameters
        ----------
        key : string
            The key for the resource in the key-value store.  They key is a unique
            identifier for the resource within the key-value store.
        buffer_size : int
            This is ignored.

        Raises
        ------
        KeyError :
            This will raise a key error if the key is not present in the store.

        """
        return super(SqliteStore, self).to_bytes(key, buffer_size)

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
        return super(SqliteStore, self).from_bytes(key, data, buffer_size)

    # Private API

    def _get_columns_by_key(self, key, columns=None):
        """ Query the sqlite database for columns in the row with the given key
        """
        columns = columns if columns is not None else ['metadata', 'data']

        # substitution OK, since these values are not user-defined
        query = 'select %s from "%s" where key == ?' % (', '.join('"'+column+'"'
            for column in columns), self.table)
        rows = self._connection.execute(query, (key,)).fetchall()

        # only expect 0 or 1 row, since primary key is unique
        if len(rows) == 0:
            return None
        else:
            return dict(zip(columns, rows[0]))

    def _insert_row(self, key, metadata, data, created, modified):
        """ Insert or replace a row into the underlying sqlite table

        This simply constructs and executes the query. It does not attempt any
        sort of transaction control.
        """
        query = 'insert or replace into "%s" (key, metadata, created, modified, data) values (?, ?, ?, ?, ?)' % self.table
        self._connection.execute(query, (key, metadata, created, modified, data))

    def _update_column(self, key, column, value):
        """ Update an existing column value in the a row with the given key

        This simply constructs and executes the query. It does not attempt any
        sort of transaction control.
        """
        query = 'update "%s" set "%s"=? where key=?' % (self.table, column)
        self._connection.execute(query, (value, key))

    def _update_columns(self, key, columns, values):
        """ Update an existing column value in the a row with the given key

        This simply constructs and executes the query. It does not attempt any
        sort of transaction control.
        """
        query = 'update "%s" set %s where key=?' % (self.table,
            ', '.join('"'+column+'"=?' for column in columns))
        self._connection.execute(query, tuple(values)+(key,))

    def _update_index(self, key, metadata):
        if not self._index:
            return
        if self._index == 'dynamic':
            missing_columns = set(column for column in metadata
                if column not in self.index_columns)
            query1 = 'alter table "%s" add column "%s" blob'
            query2 = 'create index "%s" on "%s" ("%s")'
            for column in missing_columns:
                self._connection.execute(query1 % (self.table, column))
                self._connection.execute(query2 % (column, self.table, column))
            self.index_columns |= missing_columns

        columns = [column for column in metadata if column in self.index_columns]
        values = [buffer(pickle.dumps(metadata[column], protocol=2)) for column in columns]
        if columns:
            self._update_columns(key, columns, values)

    def _build_index(self):
        for row in self._connection.execute('select key, metadata from "%s"' % self.table):
            self._update_index(*row)
            self._commit_transaction()
