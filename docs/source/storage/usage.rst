Usage
=====

.. currentmodule:: encore.storage.abstract_store

The key-value store API gives a common API that can be used with a variety of
different backends to provide a consistent interface for storage.  If used
correctly you can swap out the backend used with little or no modification of
the user code.

Creating and Connecting
-----------------------

Before you use a store, you need to create an instance of the appropriate type,
and then connect to it, possibly authenticating if that is required.  For
example, the following connects to a read-only remote store via HTTP, using
HTTP Authentication::

    from encore.events.api import EventManager
    from encore.storage.static_url_store import StaticURLStore

    event_manager = EventManager()
    store = StaticURLStore(event_manager, 'http://localhost:8080/', 'data', 'index.json')
    store.connect(credentials={'username': 'alibaba', password: 'Open Sesame'})

At this point the store is ready to use.  You can check to see whether the store
has connected using the :py:meth:`~.AbstractReadOnlyStore.is_connected` method.  When you are finished
with a store, you should call its :py:meth:`~.AbstractReadOnlyStore.disconnect` method to allow it to
cleanly release any resources it may be using, such as database connections.

Reading
-------

To read from a store, you use one of the :py:meth:`~.AbstractReadOnlyStore.get`
methods::

    value = store.get('my_document')
    datastream = value.data
    metadata = value.metadata

In this case datastream is a file-like object that streams bytes::

    data = datastream.read()
    print data

More likely you will have used some sort of serialization format like XML, JSON
or YAML to store your data in the document, so instead you can do::

    import json
    data = json.load(datastream)

If the data is raw bytes to store into a numpy array, you can do something like
this::

    import numpy
    data = datastream.read()
    dtype = numpy.int32
    size = len(data)/dtype().nbytes
    arr = numpy.empty(shape=size, dtype=dtype)
    arr.data[:] = data

The :py:meth:`~.Filelike.read` method supports buffered reads if your data is
larger than would comfortably fit into memory.

If you need to support random-access streaming, the value API also supports
a :py:meth:`~.Value.range(start, end)` method that return the requested
bytes as a readable stream.

The metadata stores auxilliary information about the data that is stored in the
key.  It is a dictionary of reasonably serializable values (frequently it will
serialize to JSON or similar format)::

    print 'Document title:', metadata['title']
    print 'Document author:', metadata['author']
    print 'Document encoding:', metadata['encoding']

    # checksum
    import hashlib
    assert hashlib.sha1(document.read()).digest() == metadata['sha1']

What metadata is stored is completely dependent on the use-case for the key-value
store: the key-value store makes no assumptions.

If you try to read a key which doe not exist, then the store will raise a KeyError.
If you want to see whether or not a particular key is populated, you can use the
:py:meth:`~.AbstractReadOnlyStore.exists` method.

Frequently you will only be interested in the data or the metadata, not both.
For these cases there are methods :py:meth:`~.AbstractReadOnlyStore.get_data` and :py:meth:`~.AbstractReadOnlyStore.get_metadata`
which return the appropriate entities.  For metadata, if you are only interested
in the values of some of the dictionary keys, you can supply an additional argument
``select`` which will restrict the returned keys to this subset of all the keys:

    author_info = store.get_metadata('document', select=['author', 'organization'])

It is very common that you either want to extract the stream of bytes from a value
into a Python bytes object (ie. a string in Python 2, as opposed to unicode) or
into a file on the local filesystem.  Two utility methods :py:meth:`~.AbstractReadOnlyStore.to_file`
and :py:meth:`~.AbstractReadOnlyStore.to_bytes` are provided which perform these operations.  If the
data source is larger than will comfortably fit into memory (particularly for
:py:meth:`~.AbstractReadOnlyStore.to_file`) you can supply an optional buffer size::

    store.to_file('document', 'local_document.txt', buffer=8096)

Querying
--------

Frequently you want to find keys whose metadata match certain criteria.  The
key-value store API gives a simple query mechanism that permits this sort of
matching::

    for key, metadata in store.query(author='alibaba', organization='40 Thieves'):
        print key, ':', metadata['title']

This will print the key and title of all documents which have an ``author`` key
with value ``'alibaba'`` and an ``organization`` key with value ``'40 Thieves'``.
The current API only permits querying for exact matches and matching all of the
query terms.  More complex queries would need to be performed on an ad-hoc basis
on top of this API.

If all the user is concerned with is which keys match, there is an alternative
method :py:meth:`~.AbstractReadOnlyStore.query_keys`::

    for key in store.query_keys(author='alibaba', organization='40 Thieves'):
        print key

To iterate over all the keys in a store, you can simply call :py:meth:`~.AbstractReadOnlyStore.query_keys`
with no arguments::

    for key in store.query_keys():
        print key

Finally, as a useful utility, you can use glob-style matching on the keys using
the :py:meth:`~.AbstractReadOnlyStore.glob` method::

    for key in store.glob('*.jpg'):
        print key

Writing
-------

Most, but not all, stores also allow you to write data to keys.  The basic method
is :py:meth:`~.AbstractStore.set` which is the inverse of :py:meth:`~.AbstractReadOnlyStore.get`.  It expects a
file-like object with a :py:meth:`~.Filelike.read` method that can do buffering, and a
dictionary of metadata as arguments::

    from cStringIO import StringIO

    data = StringIO("Hello World")
    metadata = {'title': "Greeting", 'author': 'alibaba'}
    store.set('hello', (data, metadata))

As with reading, there are methods :py:meth:`~.AbstractStore.set_data` and :py:meth:`~.AbstractStore.set_metadata`
that permit you to set just one of the two parts of the value, and there are
utility methods :py:meth:`~.AbstractStore.from_bytes` and :py:meth:`~.AbstractStore.from_file` that populate
the data of a key from either a byte string or a binary file.  The latter two
methods do not set any metadata: that must be done manually if needed.

If you want to add to the metadata without overwriting it, there is a convenience
method :py:meth:`~.AbstractStore.update_metadata` method that will update the
metadata dictionary in mych the same way that the standard Python dictionary's
``update`` method works.

You can delete a key with the :py:meth:`~.AbstractStore.delete` method::

    store.delete('hello')

Transactions
------------

The key-value store API does not assume that the underlying storage mechanism
has a notion of transactions, but if it does then it can be supported by the
key-value store.  Transactions are handled by context managers and the with
statement::

    with store.transaction('Setting some values'):
        store.set('key1', (data1, metadata1))
        store.set('key2', (data2, metadata2))

If any exception were to occur in the with statement, the context manager will
ensure that the transaction gets rolled back.  Otherwise the transaction will
be committed when the with statement finishes.

Transactions are re-entrant, so it is safe to do the following::

    def add_keypair(keypair):
        with store.transaction('Adding keypair'):
            store.set(keypair.key1, (keypair.data1, keypair.metadata1))
            store.set(keypair.key2, (keypair.data2, keypair.metadata2))

    def add_many_keypairs(keypairs):
        with store.transaction('Adding many keypairs'):
            for keypair in keypairs:
                add_keypair(keypair)

The transaction in the function is effectively ignored, with only the outermost
transaction applying.


The "Multi" Methods
-------------------

For convenience there are a collection of methods prefixed by "multi", such as
:py:meth:`~.AbstractReadOnlyStore.multiget` and :py:meth:`~.AbstractStore.multiset_data`,
which perform the specified operations on a collection of keys at once.  If
transactions are available, then these will be done as a single transaction.


Events
------

The various stores use the Encore event system, which is why the stores must
be supplied with a reference to an EventManager instance.  The events which are
emitted are referenced in the documentation for each method.
