.. currentmodule:: encore.storage.utils

.. automodule:: encore.storage.utils
    
    File-like Interface Utilities
    -----------------------------
    
    These utilities help with the management of file-like objects.  In
    particular :py:func:`buffer_iterator` is of particular use, as it produces
    an iterator which generates chunks of bytes in the file-like object which
    permits memory-efficient streaming of the data.  This is preferred over
    reading in all the data and then processing it if the data is even
    moderately big.
    
    The :py:class:`BufferIteratorIO` class is a class whick provides a
    file-like API around a buffer iterator.  This is particularly useful for
    Stores which wrap another store and implementing streaming filters on the
    data.
    
    .. autoclass:: BufferIteratorIO
        :members:
    
    .. autofunction:: buffer_iterator
    
    .. autofunction:: tee
    
    Transaction Support
    -------------------
    
    These are two simple context managers for transactions.  The
    :py:class:`DummyTransactionContext` should be used by Store implementations
    which have no notion of a transaction.  The
    :py:class:`SimpleTransactionContext` is a complete transaction manager
    for implementations with begin/commit/rollback semantics.
    
    .. autoclass:: DummyTransactionContext
        :members:
    
    .. autoclass:: SimpleTransactionContext
        :members:
    
    
    Event Support
    -------------
    
    .. autoclass:: StoreProgressManager
        :members:
    
