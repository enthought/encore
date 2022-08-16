#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

import time
from unittest import TestCase

from .abstract_test import StoreReadTestMixin, StoreWriteTestMixin
from ..joined_store import JoinedStore
from ..dict_memory_store import DictMemoryStore

class JoinedStoreReadTest(TestCase, StoreReadTestMixin):

    def setUp(self):
        """ Set up a data store for the test case

        The store should have:

            * a key 'test1' with a file-like data object containing the
              bytes 'test2\n' and metadata {'a_str': 'test3', 'an_int': 1,
              'a_float': 2.0, 'a_bool': True, 'a_list': ['one', 'two', 'three'],
              'a_dict': {'one': 1, 'two': 2, 'three': 3}}

            * keys 'key0' through 'key9' with values 'value0' through 'value9'
              in filelike objects, and metadata {'query_test1': 'value',
              'query_test2': 0 through 9, 'optional': True for even,
              not present for odd}

        and set into 'self.store'.
        """
        super(JoinedStoreReadTest, self).setUp()
        self.store1 = DictMemoryStore()
        self.store2 = DictMemoryStore()
        self.store3 = DictMemoryStore()
        t = time.time()
        self.store2._store['test1'] = (
            b'test2\n',
            {
                'a_str': 'test3',
                'an_int': 1,
                'a_float': 2.0,
                'a_bool': True,
                'a_list': ['one', 'two', 'three'],
                'a_dict': {'one': 1, 'two': 2, 'three': 3}
            }, t, t
        )
        stores = [self.store1, self.store2, self.store3]
        for i in range(10):
            metadata = {'query_test1': 'value',
                'query_test2': i}
            if i % 2 == 0:
                metadata['optional'] = True
            t = time.time()
            stores[i%3]._store['key%d'%i] = (b'value%d' % i, metadata, t, t)
        self.store = JoinedStore(stores)


class JoinedStoreWriteTest(TestCase, StoreWriteTestMixin):

    def setUp(self):
        """ Set up a data store for the test case

        The store should have:

            * a key 'test1' with a file-like data object containing the
              bytes 'test2\n' and metadata {'a_str': 'test3', 'an_int': 1,
              'a_float': 2.0, 'a_bool': True, 'a_list': ['one', 'two', 'three'],
              'a_dict': {'one': 1, 'two': 2, 'three': 3}}

            * a series of keys 'existing_key0' through 'existing_key9' with
              data containing 'existing_value0' throigh 'existing_value9' and
              metadata {'meta': True, 'meta1': 0} through {'meta': True, 'meta1': -9}

        and set into 'self.store'.
        """
        super(JoinedStoreWriteTest, self).setUp()
        self.store1 = DictMemoryStore()
        self.store2 = DictMemoryStore()
        self.store3 = DictMemoryStore()
        t = time.time()
        self.store2._store['test1'] = (
            b'test2\n',
            {
                'a_str': 'test3',
                'an_int': 1,
                'a_float': 2.0,
                'a_bool': True,
                'a_list': ['one', 'two', 'three'],
                'a_dict': {'one': 1, 'two': 2, 'three': 3}
            }, t, t
        )
        stores = [self.store1, self.store2, self.store3]
        for i in range(10):
            key = 'existing_key'+str(i)
            data = b'existing_value%i' % i
            metadata = {'meta': True, 'meta1': -i}
            t = time.time()
            stores[i%3]._store[key] = (data, metadata, t, t)
        self.store = JoinedStore(stores)

    def test_multiset_metadata(self):
        super(JoinedStoreWriteTest, self).test_multiset_metadata()
        keys = ['existing_key'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        for i in range(10):
            self.assertTrue(self.store1.exists(keys[i]))
            self.assertEquals(self.store1.get_metadata(keys[i]), metadatas[i])
