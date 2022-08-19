.. currentmodule:: encore.storage.abstract_store

.. automodule:: encore.storage.abstract_store

    .. autoclass:: Value
        :members:

    .. autoclass:: AbstractReadOnlyStore
        :members: connect, disconnect, is_connected, get, exists,
            query, query_keys, glob,
            get_data, get_metadata, get_data_range,
            multiget, multiget_data, multiget_metadata,
            to_file, to_bytes

    .. autoclass:: AbstractStore
        :members: set, delete, transaction,
            set_data, set_metadata, update_metadata,
            multiset, multiset_data,
            multiset_metadata, multiupdate_metadata,
            from_file, from_bytes

    .. autoclass:: AbstractAuthorizingStore
        :members: user_tag, get_permissions, set_permissions,
            update_permissions
