=======
Storage
=======

The ``encore.storage`` package provides an abstract API for key-value stores,
as well as some reference implementations, utilities, and building-blocks
for creating more complex stores from simple ones.

The API is both agnostic to the type of data being stored and the
underlying data storage medium.  Being agnostic to the type of data permits the
API to be used in appropriate situations other than the ones that we currently
envision for current Enthought projects, while being agnostic to the underlying
storage mechanism permits the same code to be used no matter how the data is
stored - whether in memory, a filesystem, a web service, or a SQL or NoSQL
database - permitting greater flexibility in deployment depending on user needs.

All abstractions are leaky, so we don't anticipate that this API will cover all
possible functionality that a data store could provide, but the hope is that the
API provides a common language for the most fundamental operations, and a
baseline which can be extended as we find more commonalities in the data stores
that we develop.


Contents
--------

.. toctree::
   :maxdepth: 2
   
   concepts.rst
   usage.rst
   abstract_store.rst
   events.rst
   utils.rst

Implementations
===============

.. toctree::
   :maxdepth: 2

   dict_memory_store.rst
   sqlite_store.rst
   filesystem_store.rst
   static_url_store.rst
   joined_store.rst
   simple_auth_store.rst


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

License
-------

.. include:: ../../../LICENSE.txt
