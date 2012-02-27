#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

from unittest import TestCase, skip
from cStringIO import StringIO
from contextlib import contextmanager
from tempfile import mkdtemp
from shutil import rmtree
import os
import time

@contextmanager
def temp_dir():
    """ Create a temporary directory and ensure it is always cleaned up.
    """
    path = mkdtemp()
    yield path
    rmtree(path)

class AbstractStoreReadTest(TestCase):
    resolution = 'arbitrary'
    
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
        self.store = None
        self.test_start = time.time()
        if self.resolution == 'second':
            self.test_start = int(self.test_start)

    def utils_large(self):
        self.store.from_bytes('test3', 'test4'*10000000)

    def test_get(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        value = self.store.get('test1')
        self.assertEqual(value.data.read(), 'test2\n')
        self.assertEqual(value.metadata, {
            'a_str': 'test3',
            'an_int': 1,
            'a_float': 2.0,
            'a_bool': True,
            'a_list': ['one', 'two', 'three'],
            'a_dict': {'one': 1, 'two': 2, 'three': 3}
        })
        self.assertEqual(value.size, 6)
        # can't guarantee a particular modified and created, but should exist and be
        # greater than the test start time.
        #self.assertGreaterEqual(value.created, self.test_start)
        self.assertGreaterEqual(value.modified, self.test_start)
    
    def test_get_copies(self):
        """ Metadata returned from separate get()s should not be same object"""
        if self.store is None:
            self.skipTest('Abstract test case')
        data1, metadata1 = self.store.get('test1')
        metadata1['extra_key'] = 'extra_value'
        data2, metadata2 = self.store.get('test1')
        self.assertNotEqual(metadata2, metadata1)
    
    def test_get_data(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        data = self.store.get_data('test1')
        self.assertEqual(data.read(), 'test2\n')
    
    def test_get_metadata(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        metadata = self.store.get_metadata('test1')
        self.assertEqual(metadata, {
            'a_str': 'test3',
            'an_int': 1,
            'a_float': 2.0,
            'a_bool': True,
            'a_list': ['one', 'two', 'three'],
            'a_dict': {'one': 1, 'two': 2, 'three': 3}
        })
    
    def test_get_metadata_copies(self):
        """ Results of separate get_metadata()s should not be same object"""
        if self.store is None:
            self.skipTest('Abstract test case')
        metadata1 = self.store.get_metadata('test1')
        metadata1['extra_key'] = 'extra_value'
        metadata2 = self.store.get('test1')
        self.assertNotEqual(metadata2, metadata1)
    
    def test_get_metadata_select(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        metadata = self.store.get_metadata('test1', ['a_str', 'an_int'])
        self.assertEqual(metadata, {
            'a_str': 'test3',
            'an_int': 1,
        })
    
    def test_get_metadata_select_missing(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        metadata = self.store.get_metadata('test1', ['a_str', 'an_int', 'missing'])
        self.assertEqual(metadata, {
            'a_str': 'test3',
            'an_int': 1,
        })

    def test_exists(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        self.assertEqual(self.store.exists('test1'), True)
        self.assertEqual(self.store.exists('test2'), False)
    
    def test_multiget(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = self.store.multiget('key'+str(i) for i in range(10))
        for i, value in enumerate(result):
            self.assertEqual(value.data.read(), 'value'+str(i))
            self.assertEqual(value.size, 6)
            expected = {'query_test1': 'value', 'query_test2': i}
            if i % 2 == 0:
                expected['optional'] = True
            self.assertEqual(expected, value.metadata)        
    
    def test_multiget_data(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = self.store.multiget_data('key'+str(i) for i in range(10))
        for i, data in enumerate(result):
            self.assertEqual(data.read(), 'value'+str(i))
    
    def test_multiget_metadata(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = self.store.multiget_metadata('key'+str(i) for i in range(10))
        for i, metadata in enumerate(result):
            expected = {'query_test1': 'value', 'query_test2': i}
            if i % 2 == 0:
                expected['optional'] = True
            self.assertEqual(expected, metadata)        
    
    def test_multiget_metadata_select(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = self.store.multiget_metadata(('key'+str(i) for i in range(10)),
            select=['query_test1', 'optional'])
        for i, metadata in enumerate(result):
            expected = {'query_test1': 'value'}
            if i % 2 == 0:
                expected['optional'] = True
            self.assertEqual(expected, metadata)        
    
    def test_query(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = sorted(self.store.query(a_str='test3'))
        self.assertEqual(result, [('test1', {'a_str': 'test3', 'an_int': 1,
            'a_float': 2.0, 'a_bool': True, 'a_list': ['one', 'two', 'three'],
            'a_dict': {'one': 1, 'two': 2, 'three': 3}})])
    
    def test_query_copy(self):
        """ Metadata returned from separate query()s should not be same object"""
        if self.store is None:
            self.skipTest('Abstract test case')
        result1 = sorted(self.store.query(a_str='test3'))
        result1[0][1]['extra_key'] = 'extra_value'
        result2 = sorted(self.store.query(a_str='test3'))
        self.assertNotEqual(result1, result2)
    
    def test_query1(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = sorted(self.store.query(query_test1='value'))
        expected = sorted(('key%d' % i, {'query_test1': 'value',
            'query_test2': i}) for i in range(10))
        for i, (key, metadata) in enumerate(expected):
            if i % 2 == 0:
                metadata['optional'] = True
        self.assertEqual(result, expected)
    
    def test_query2(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        for i in range(10):
            result = sorted(self.store.query(query_test2=i))
            expected = {'query_test1': 'value', 'query_test2': i}
            if i % 2 == 0:
                expected['optional'] = True
            self.assertEqual(result, [('key%d' % i, expected)])
    
    def test_query_empty(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = list(self.store.query(a_str='test1'))
        self.assertEqual(result, [])
    
    def test_query_select(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = sorted(self.store.query(['a_str', 'an_int'], a_str='test3'))
        self.assertEqual(result, [('test1', {'a_str': 'test3', 'an_int': 1})])
    
    def test_query_select_missing(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = sorted(self.store.query(['query_test1', 'optional'], query_test1='value'))
        expected = sorted(('key%d' % i, {'query_test1': 'value'}) for i in range(10))
        for i, (key, metadata) in enumerate(expected):
            if i % 2 == 0:
                metadata['optional'] = True
        self.assertEqual(result, expected)

    def test_query_keys(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = sorted(self.store.query_keys(a_str='test3'))
        self.assertEqual(result, ['test1'])
    
    def test_query1_keys(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = sorted(self.store.query_keys(query_test1='value'))
        self.assertEqual(result, sorted('key%d' % i for i in range(10)))
    
    def test_query2_keys(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        for i in range(10):
            result = sorted(self.store.query_keys(query_test2=i))
            self.assertEqual(result, ['key%d' % i])
    
    def test_query_keys_empty(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = list(self.store.query_keys(a_str='test1'))
        self.assertEqual(result, [])

    def test_glob(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        result = sorted(self.store.glob('key*'))
        self.assertEqual(result, sorted('key%d' % i for i in range(10)))

    def test_to_bytes(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        data = self.store.to_bytes('test1')
        self.assertEqual(data, 'test2\n')        
    
    def test_to_file(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        with temp_dir() as directory:
            filepath = os.path.join(directory, 'test')
            self.store.to_file('test1', filepath)
            written = open(filepath, 'rb').read()
            self.assertEquals(written, 'test2\n')
    
    def test_to_file_large(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        self.utils_large()
        print list(self.store.query_keys())
        #print list(self.store1.query_keys())
        #print list(self.store2.query_keys())
        #print list(self.store3.query_keys())
        with temp_dir() as directory:
            filepath = os.path.join(directory, 'test')
            self.store.to_file('test3', filepath)
            written = open(filepath).read()
            self.assertEquals(written, 'test4'*10000000)
            

class AbstractStoreWriteTest(TestCase):
    resolution = 'arbitrary'
    
    def setUp(self):
        """ Set up a data store for the test case
        
        The store should have:
            
            * a key 'test1' with a file-like data object containing the
              bytes 'test2\n' and metadata {'a_str': 'test3', 'an_int': 1,
              'a_float': 2.0, 'a_bool': True, 'a_list': ['one', 'two', 'three'],
              'a_dict': {'one': 1, 'two': 2, 'three': 3}} with creation and
              modification times of 12:00 am 1st January, 2012 localtime
            
            * a series of keys 'existing_key0' through 'existing_key9' with
              data containing 'existing_value0' throigh 'existing_value9' and
              metadata {'meta': True, 'meta1': 0} through {'meta': True, 'meta1': -9}
        
        and set into 'self.store'.
        """
        self.store = None
    
    def test_set(self):
        """ Test that set works
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        data = StringIO('test4')
        metadata = {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        }
        test_start = time.time()
        if self.resolution == 'second':
            test_start = int(test_start)
        self.store.set('test3', (data, metadata))
        test_end = time.time()
        self.assertEqual(self.store.to_bytes('test3'), 'test4')
        self.assertEqual(self.store.get_metadata('test3'), metadata)

        value = self.store.get('test3')
        self.assertEqual(value.size, 5)
        self.assertGreaterEqual(value.modified, test_start)
        #self.assertGreaterEqual(value.created, test_start)
        self.assertLessEqual(value.modified, test_end)
        #self.assertLessEqual(value.created, test_end)

    def test_set_copies(self):
        """ Test that set copies the provided metadata
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        data = StringIO('test4')
        metadata = {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        }
        self.store.set('test3', (data, metadata))
        metadata['extra_key'] = 'extra_value'
        self.assertNotEqual(self.store.get_metadata('test3'), metadata)
        
    def test_set_large(self):
        """ Test that set works with large (~50 MB) data
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        data = StringIO('test4'*10000000) # 50 MB of data
        metadata = {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        }
        self.store.set('test3', (data, metadata))
        self.assertEqual(self.store.to_bytes('test3'), 'test4'*10000000)
        self.assertEqual(self.store.get_metadata('test3'), metadata)
        
    def test_set_buffer(self):
        """ Test that set works with a different size buffer
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        data = StringIO('test4'*8000)
        metadata = {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        }
        self.store.set('test3', (data, metadata), 8000)
        self.assertEqual(self.store.to_bytes('test3'), 'test4'*8000)
        self.assertEqual(self.store.get_metadata('test3'), metadata)

    def test_set_data(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        data = StringIO('test4')
        test_start = time.time()
        if self.resolution == 'second':
            test_start = int(test_start)
        self.store.set_data('test1', data)
        test_end = time.time()
        self.assertEqual(self.store.to_bytes('test1'), 'test4')
        value = self.store.get('test1')
        self.assertGreaterEqual(value.modified, test_start)
        #self.assertLessEqual(value.created, test_start)
        self.assertLessEqual(value.modified, test_end)
        #self.assertLessEqual(value.created, test_end)
        # for the time being we make no assertions about what happens to the
        # metadata of an existing object because of behaviour of JoinedStore
        #self.assertEqual(self.store.get_metadata('test1'), {
        #    'a_str': 'test3',
        #    'an_int': 1,
        #    'a_float': 2.0,
        #    'a_bool': True,
        #    'a_list': ['one', 'two', 'three'],
        #    'a_dict': {'one': 1, 'two': 2, 'three': 3}
        #})

    def test_set_data_new(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        data = StringIO('test4')
        test_start = time.time()
        if self.resolution == 'second':
            test_start = int(test_start)
        self.store.set_data('test3', data)
        test_end = time.time()
        self.assertEqual(self.store.to_bytes('test3'), 'test4')
        value = self.store.get('test3')
        self.assertGreaterEqual(value.modified, test_start)
        #self.assertGreaterEqual(value.created, test_start)
        self.assertLessEqual(value.modified, test_end)
        #self.assertLessEqual(value.created, test_end)
        # for the time being we make no assertions about what happens to the
        # metadata of an new object because of behaviour of JoinedStore
        #self.assertEqual(self.store.get_metadata('test3'), {})

    def test_set_data_large(self):
        """ Test that set works with large (~50 MB) data
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        data = StringIO('test4'*10000000) # 50 MB of data
        self.store.set_data('test3', data)
        self.assertEqual(self.store.to_bytes('test3'), 'test4'*10000000)

    def test_set_data_buffer(self):
        """ Test that set works with a different-sized buffer
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        data = StringIO('test4'*8000)
        self.store.set_data('test1', data)
        self.assertEqual(self.store.to_bytes('test1'), 'test4'*8000)

    def test_set_metadata(self):
        """ Test that set_metadata works
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        metadata = {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        }
        test_start = time.time()
        if self.resolution == 'second':
            test_start = int(test_start)
        self.store.set_metadata('test1', metadata)
        test_end = time.time()
        self.assertEqual(self.store.get_metadata('test1'), metadata)
        value = self.store.get('test1')
        self.assertGreaterEqual(value.modified, test_start)
        #self.assertLessEqual(value.created, test_start)
        self.assertLessEqual(value.modified, test_end)
        #self.assertLessEqual(value.created, test_end)
        # for the time being we make no assertions about what happens to the
        # data of an existing object because of behaviour of JoinedStore
        #self.assertEqual(self.store.to_bytes('test1'), 'test2\n')

    def test_set_metadata_copies(self):
        """ Test that set_metadata copies the provided metadata
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        metadata = {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        }
        self.store.set_metadata('test3', metadata)
        metadata['extra_key'] = 'extra_value'
        self.assertNotEqual(self.store.get_metadata('test1'), metadata)

    def test_update_metadata(self):
        """ Test that update_metadata works
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        metadata = {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        }
        self.store.update_metadata('test1', metadata)

    def test_delete(self):
        """ Test that delete works
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        self.store.delete('test1')
        self.assertFalse(self.store.exists('test1'))

    def test_multiset(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        keys = ['set_key'+str(i) for i in range(10)]
        values = ['set_value'+str(i) for i in range(10)]
        datas = [StringIO(value) for value in values]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        self.store.multiset(keys, zip(datas, metadatas))
        for i in range(10):
            self.assertTrue(self.store.exists(keys[i]))
            self.assertEquals(self.store.get_data(keys[i]).read(), values[i])
            self.assertEquals(self.store.get_metadata(keys[i]), metadatas[i])

    def test_multiset_overwrite(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        keys = ['existing_key'+str(i) for i in range(10)]
        values = ['set_value'+str(i) for i in range(10)]
        datas = [StringIO(value) for value in values]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        self.store.multiset(keys, zip(datas, metadatas))
        for i in range(10):
            self.assertTrue(self.store.exists(keys[i]))
            self.assertEquals(self.store.get_data(keys[i]).read(), values[i])
            self.assertEquals(self.store.get_metadata(keys[i]), metadatas[i])

    def test_multiset_data(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        keys = ['existing_key'+str(i) for i in range(10)]
        values = ['set_value'+str(i) for i in range(10)]
        datas = [StringIO(value) for value in values]
        self.store.multiset_data(keys, datas)
        metadatas = [{'meta': True, 'meta1': -i} for i in range(10)]
        for i in range(10):
            self.assertTrue(self.store.exists(keys[i]))
            self.assertEquals(self.store.get_data(keys[i]).read(), values[i])
            # for the time being we make no assertions about what happens to the
            # data of an object because of behaviour of JoinedStore
            #self.assertEquals(self.store.get_metadata(keys[i]), metadatas[i])

    def test_multiset_metadata(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        keys = ['existing_key'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        self.store.multiset_metadata(keys, metadatas)
        values = ['existing_value'+str(i) for i in range(10)]
        for i in range(10):
            self.assertTrue(self.store.exists(keys[i]))
            self.assertEquals(self.store.get_metadata(keys[i]), metadatas[i])
            # for the time being we make no assertions about what happens to the
            # metadata of an object because of behaviour of JoinedStore
            #self.assertEquals(self.store.get_data(keys[i]).read(), values[i])

    def test_multiupdate_metadata(self):
        if self.store is None:
            self.skipTest('Abstract test case')
        keys = ['existing_key'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        self.store.multiset_metadata(keys, metadatas)
        for i in range(10):
            self.assertTrue(self.store.exists(keys[i]))
            expected = {'meta': True}
            expected.update(metadatas[i])
            self.assertEquals(self.store.get_metadata(keys[i]), metadatas[i])

    def test_from_file(self):
        """ Test that from_file works
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        with temp_dir() as directory:
            filepath = os.path.join(directory, 'test')
            with open(filepath, 'wb') as fp:
                fp.write('test4')
            self.store.from_file('test3', filepath)
        self.assertEqual(self.store.to_bytes('test3'), 'test4')

    def test_from_file_large(self):
        """ Test that from_file works for large files (~50 MB)
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        with temp_dir() as directory:
            filepath = os.path.join(directory, 'test')
            with open(filepath, 'wb') as fp:
                fp.write('test4'*10000000)
            self.store.from_file('test3', filepath)
        self.assertEqual(self.store.to_bytes('test3'), 'test4'*10000000)

    def test_from_bytes(self):
        """ Test that from bytes works
        
        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        if self.store is None:
            self.skipTest('Abstract test case')
        self.store.from_bytes('test3', 'test4')
        self.assertEqual(self.store.to_bytes('test3'), 'test4')
        
