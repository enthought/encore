#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

import time
from unittest import TestCase

from .abstract_test import StoreReadTestMixin, StoreWriteTestMixin
from ..mounted_store import MountedStore
from ..dict_memory_store import DictMemoryStore
from ..string_value import StringValue

class MountedStoreTest(TestCase):

    def setUp(self):
        super(MountedStoreTest, self).setUp()
        self.mounted_store = DictMemoryStore()
        self.backing_store = DictMemoryStore()
        t = time.time()
        self.mounted_store._store['test1'] = (
            b'mounted\n', {'metakey': 'mounted'}, t, t
        )
        self.backing_store._store['test/test1'] = (
            b'backing\n', {'metakey': 'backing',}, t, t
        )
        self.backing_store._store['test/test2'] = (
            b'backing\n', {'metakey': 'backing',}, t, t
        )
        self.store = MountedStore('test/', self.mounted_store, self.backing_store)

    def test_get_masked(self):
        value = self.store.get('test/test1')
        self.assertEqual(value.data.read(), b"mounted\n")
        self.assertEqual(value.metadata, {"metakey": "mounted"})

    def test_get_unmasked(self):
        value = self.store.get('test/test2')
        self.assertEqual(value.data.read(), b"backing\n")
        self.assertEqual(value.metadata, {"metakey": "backing"})

    def test_get_data_masked(self):
        value = self.store.get_data('test/test1')
        self.assertEqual(value.read(), b"mounted\n")

    def test_get_data_unmasked(self):
        value = self.store.get_data('test/test2')
        self.assertEqual(value.read(), b"backing\n")

    def test_get_metadata_masked(self):
        value = self.store.get_metadata('test/test1')
        self.assertEqual(value, {"metakey": "mounted"})

    def test_get_metadata_unmasked(self):
        value = self.store.get_metadata('test/test2')
        self.assertEqual(value, {"metakey": "backing"})

    def test_set_masked(self):
        self.store.set('test/test2', StringValue(b'mounted\n', {'metakey': 'mounted'}))
        # test value in combined store
        value = self.store.get('test/test2')
        self.assertEqual(value.data.read(), b"mounted\n")
        self.assertEqual(value.metadata, {"metakey": "mounted"})
        # test value in underlying store
        value = self.mounted_store.get('test2')
        self.assertEqual(value.data.read(), b"mounted\n")
        self.assertEqual(value.metadata, {"metakey": "mounted"})

    def test_set_data_masked(self):
        self.store.set_data('test/test2', StringValue(b'mounted\n').data)
        # test value in combined store
        value = self.store.get('test/test2')
        self.assertEqual(value.data.read(), b"mounted\n")
        self.assertEqual(value.metadata, {"metakey": "backing"})
        # test value in underlying store
        value = self.mounted_store.get('test2')
        self.assertEqual(value.data.read(), b"mounted\n")
        self.assertEqual(value.metadata, {"metakey": "backing"})

    def test_set_metadata_masked(self):
        self.store.set_metadata('test/test2', {'metakey': 'mounted'})
        # test value in combined store
        value = self.store.get('test/test2')
        self.assertEqual(value.data.read(), b"backing\n")
        self.assertEqual(value.metadata, {"metakey": "mounted"})
        # test value in underlying store
        value = self.mounted_store.get('test2')
        self.assertEqual(value.data.read(), b"backing\n")
        self.assertEqual(value.metadata, {"metakey": "mounted"})

    def test_update_metadata_masked_1(self):
        self.store.update_metadata('test/test2', {'metakey': 'mounted'})
        # test value in combined store
        value = self.store.get('test/test2')
        self.assertEqual(value.data.read(), b"backing\n")
        self.assertEqual(value.metadata, {"metakey": "mounted"})
        # test value in underlying store
        value = self.mounted_store.get('test2')
        self.assertEqual(value.data.read(), b"backing\n")
        self.assertEqual(value.metadata, {"metakey": "mounted"})

    def test_update_metadata_masked_2(self):
        self.store.update_metadata('test/test2', {'newkey': 'mounted'})
        # test value in combined store
        value = self.store.get('test/test2')
        self.assertEqual(value.data.read(), b"backing\n")
        self.assertEqual(value.metadata, {"metakey": "backing", 'newkey': "mounted"})
        # test value in underlying store
        value = self.mounted_store.get('test2')
        self.assertEqual(value.data.read(), b"backing\n")
        self.assertEqual(value.metadata, {"metakey": "backing", 'newkey': "mounted"})

    def test_push(self):
        self.store.push('test/test1')
        # test value in combined store
        value = self.store.get('test/test1')
        self.assertEqual(value.data.read(), b"mounted\n")
        self.assertEqual(value.metadata, {"metakey": "mounted"})
        # check that it is missing in mounted_store
        self.assertFalse(self.mounted_store.exists('test1'))
        # test value in backing store
        value = self.backing_store.get('test/test1')
        self.assertEqual(value.data.read(), b"mounted\n")
        self.assertEqual(value.metadata, {"metakey": "mounted"})


class MountedStoreReadTest(TestCase, StoreReadTestMixin):

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
        super(MountedStoreReadTest, self).setUp()
        self.mounted_store = DictMemoryStore()
        self.backing_store = DictMemoryStore()
        t = time.time()
        self.backing_store._store['test1'] = (
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
        stores = [self.mounted_store, self.backing_store]
        for i in range(10):
            metadata = {'query_test1': 'value',
                'query_test2': i}
            if i % 2 == 0:
                metadata['optional'] = True
            t = time.time()
            stores[i%2]._store['key%d'%i] = (b'value%d' % i, metadata, t, t)
        self.store = MountedStore('', self.mounted_store, self.backing_store)

class MountedStoreWriteTest(TestCase, StoreWriteTestMixin):

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
        super(MountedStoreWriteTest, self).setUp()
        self.mounted_store = DictMemoryStore()
        self.backing_store = DictMemoryStore()
        t = time.time()
        self.backing_store._store['test1'] = (
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
        stores = [self.mounted_store, self.backing_store]
        for i in range(10):
            key = 'existing_key'+str(i)
            data = b'existing_value%i' % i
            metadata = {'meta': True, 'meta1': -i}
            t = time.time()
            stores[i%2]._store[key] = (data, metadata, t, t)
        self.store = MountedStore('', self.mounted_store, self.backing_store)

    def test_multiset_metadata(self):
        super(MountedStoreWriteTest, self).test_multiset_metadata()
        keys = ['existing_key'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        for i in range(10):
            self.assertTrue(self.mounted_store.exists(keys[i]))
            self.assertEquals(self.mounted_store.get_metadata(keys[i]), metadatas[i])

    def test_delete(self):
        """ Test that delete works for keys in mounted store """
        t = time.time()
        self.mounted_store._store['test2'] = (
            b'test2\n', {}, t, t
        )
        self.store.delete('test2')
        self.assertFalse(self.store.exists('test2'))
