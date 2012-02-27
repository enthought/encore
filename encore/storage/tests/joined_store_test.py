#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

import time

import encore.storage.tests.abstract_test as abstract_test
from ..joined_store import JoinedStore
from ..dict_memory_store import DictMemoryStore

class JoinedStoreReadTest(abstract_test.AbstractStoreReadTest):
    
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
            'test2\n',
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
            stores[i%3]._store['key%d'%i] = ('value%d' % i, metadata, t, t)
        self.store = JoinedStore(stores)

    #def utils_large(self):
    #    self.store2.from_bytes('test3', '')#'test4'*10000000)

class JoinedStoreWriteTest(abstract_test.AbstractStoreWriteTest):
    
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
            'test2\n',
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
            data = 'existing_value'+str(i)
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

    """
    def test_set(self):
        super(DictMemoryStoreWriteTest, self).test_set()
        self.assertEqual(self.store._data['test3'], 'test4')
        self.assertEqual(self.store._metadata['test3'], {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        })

    def test_set_copies(self):
        super(DictMemoryStoreWriteTest, self).test_set_copies()
        self.assertEqual(self.store._metadata['test3'], {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        })

    def test_set_large(self):
        super(DictMemoryStoreWriteTest, self).test_set_large()
        self.assertEqual(self.store._data['test3'], 'test4'*10000000)
        self.assertEqual(self.store._metadata['test3'], {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        })

    def test_set_buffer(self):
        super(DictMemoryStoreWriteTest, self).test_set_buffer()
        self.assertEqual(self.store._data['test3'], 'test4'*8000)
        self.assertEqual(self.store._metadata['test3'], {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        })

    def test_set_data(self):
        super(DictMemoryStoreWriteTest, self).test_set_data()
        self.assertEqual(self.store._data['test1'], 'test4')

    def test_set_data_large(self):
        super(DictMemoryStoreWriteTest, self).test_set_data_large()
        self.assertEqual(self.store._data['test3'], 'test4'*10000000)

    def test_set_data_buffer(self):
        super(DictMemoryStoreWriteTest, self).test_set_data_buffer()
        self.assertEqual(self.store._data['test1'], 'test4'*8000)

    def test_set_metadata(self):
        super(DictMemoryStoreWriteTest, self).test_set_metadata()
        self.assertEqual(self.store._metadata['test1'], {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        })

    def test_update_metadata(self):
        super(DictMemoryStoreWriteTest, self).test_update_metadata()
        self.assertEqual(self.store._metadata['test1'], {
            'a_float': 2.0,
            'a_list': ['one', 'two', 'three'],
            'a_dict': {'one': 1, 'two': 2, 'three': 3},
            'a_str': 'test5',
            'a_bool': True,
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        })
    
    def test_delete(self):
        super(DictMemoryStoreWriteTest, self).test_delete()
        self.assertFalse('test1' in self.store._data)
        self.assertFalse('test1' in self.store._metadata)

    def test_multiset(self):
        super(DictMemoryStoreWriteTest, self).test_multiset()
        keys = ['set_key'+str(i) for i in range(10)]
        values = ['set_value'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        for i in range(10):
            self.assertEquals(self.store._data[keys[i]], values[i])
            self.assertEquals(self.store._metadata[keys[i]], metadatas[i])

    def test_multiset_overwrite(self):
        super(DictMemoryStoreWriteTest, self).test_multiset_overwrite()
        keys = ['existing_key'+str(i) for i in range(10)]
        values = ['set_value'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        for i in range(10):
            self.assertEquals(self.store._data[keys[i]], values[i])
            self.assertEquals(self.store._metadata[keys[i]], metadatas[i])

    def test_multiset_data(self):
        super(DictMemoryStoreWriteTest, self).test_multiset_data()
        keys = ['existing_key'+str(i) for i in range(10)]
        values = ['set_value'+str(i) for i in range(10)]
        for i in range(10):
            self.assertEquals(self.store._data[keys[i]], values[i])

    def test_multiset_metadata(self):
        super(DictMemoryStoreWriteTest, self).test_multiset_metadata()
        keys = ['existing_key'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        for i in range(10):
            self.assertEquals(self.store._metadata[keys[i]], metadatas[i])

    def test_multiupdate_metadata(self):
        super(DictMemoryStoreWriteTest, self).test_multiupdate_metadata()
        keys = ['existing_key'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        for i in range(10):
            expected = {'meta': True}
            expected.update(metadatas[i])
            self.assertEquals(self.store._metadata[keys[i]], metadatas[i])

    def test_from_file(self):
        super(DictMemoryStoreWriteTest, self).test_from_file()
        self.assertEqual(self.store._data['test3'], 'test4')

    def test_from_file_large(self):
        super(DictMemoryStoreWriteTest, self).test_from_file_large()
        self.assertEqual(self.store._data['test3'], 'test4'*10000000)

    def test_from_bytes(self):
        super(DictMemoryStoreWriteTest, self).test_from_bytes()
        self.assertEqual(self.store._data['test3'], 'test4')
    """
