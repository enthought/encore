#
# (C) Copyright 2011 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

import os
from tempfile import mkdtemp
from shutil import rmtree
import sqlite3

from encore.events.api import EventManager
import encore.storage.tests.abstract_test as abstract_test
from ..sqlite_store import SqliteStore

class SqliteStoreReadTest(abstract_test.AbstractStoreReadTest):
    
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
        self.path = mkdtemp()
        self.db_file = os.path.join(self.path, 'db.sqlite')
        
        connection = sqlite3.connect(self.db_file)
        
        connection.execute("""
            create table store (
                key text primary key,
                metadata dict,
                data blob
            )
            """)
        
        connection.execute("""insert into store values (?, ?, ?)""", (
            b'test1',
            {
                'a_str': 'test3',
                'an_int': 1,
                'a_float': 2.0,
                'a_bool': True,
                'a_list': ['one', 'two', 'three'],
                'a_dict': {'one': 1, 'two': 2, 'three': 3}
            },
            buffer(b'test2\n')))
        for i in range(10):
            key = b'key%d'%i
            data = buffer(b'value%d' % i)
            metadata = {'query_test1': 'value', 'query_test2': i}
            if i % 2 == 0:
                metadata['optional'] = True
            connection.execute("""insert into store values (?, ?, ?)""", (key, metadata, data))
        connection.commit()
        
        connection = None
        
        self.store = SqliteStore(EventManager(), self.db_file, 'store')
        self.store.connect()
    
    def tearDown(self):
        rmtree(self.path)


class SqliteStoreWriteTest(abstract_test.AbstractStoreWriteTest):
    
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
        self.path = mkdtemp()
        self.db_file = os.path.join(self.path, 'db.sqlite')
        
        connection = sqlite3.connect(self.db_file)
        
        connection.execute("""
            create table store (
                key text primary key,
                metadata dict,
                data blob
            )
            """)
        
        connection.execute("""insert into store values (?, ?, ?)""", (
            b'test1',
            {
                'a_str': 'test3',
                'an_int': 1,
                'a_float': 2.0,
                'a_bool': True,
                'a_list': ['one', 'two', 'three'],
                'a_dict': {'one': 1, 'two': 2, 'three': 3}
            },
            buffer(b'test2\n')))
        for i in range(10):
            key = b'existing_key%d'%i
            data = buffer(b'existing_value%d' % i)
            metadata = {'meta': True, 'meta1': -i}
            connection.execute("""insert into store values (?, ?, ?)""", (key, metadata, data))
        connection.commit()
        
        connection = None

        self.store = SqliteStore(EventManager(), self.db_file, 'store')
        self.store.connect()

    """
    def test_set(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_set()
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
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_set_copies()
        self.assertEqual(self.store._metadata['test3'], {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        })

    def test_set_large(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_set_large()
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
        self.skipTest('Not Implemented')
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
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_set_data()
        self.assertEqual(self.store._data['test1'], 'test4')

    def test_set_data_large(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_set_data_large()
        self.assertEqual(self.store._data['test3'], 'test4'*10000000)

    def test_set_data_buffer(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_set_data_buffer()
        self.assertEqual(self.store._data['test1'], 'test4'*8000)

    def test_set_metadata(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_set_metadata()
        self.assertEqual(self.store._metadata['test1'], {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        })

    def test_update_metadata(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_update_metadata()
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
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_delete()
        self.assertFalse('test1' in self.store._data)
        self.assertFalse('test1' in self.store._metadata)

    def test_multiset(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_multiset()
        keys = ['set_key'+str(i) for i in range(10)]
        values = ['set_value'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        for i in range(10):
            self.assertEquals(self.store._data[keys[i]], values[i])
            self.assertEquals(self.store._metadata[keys[i]], metadatas[i])

    def test_multiset_overwrite(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_multiset_overwrite()
        keys = ['existing_key'+str(i) for i in range(10)]
        values = ['set_value'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        for i in range(10):
            self.assertEquals(self.store._data[keys[i]], values[i])
            self.assertEquals(self.store._metadata[keys[i]], metadatas[i])

    def test_multiset_data(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_multiset_data()
        keys = ['existing_key'+str(i) for i in range(10)]
        values = ['set_value'+str(i) for i in range(10)]
        for i in range(10):
            self.assertEquals(self.store._data[keys[i]], values[i])

    def test_multiset_metadata(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_multiset_metadata()
        keys = ['existing_key'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        for i in range(10):
            self.assertEquals(self.store._metadata[keys[i]], metadatas[i])

    def test_multiupdate_metadata(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_multiupdate_metadata()
        keys = ['existing_key'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        for i in range(10):
            expected = {'meta': True}
            expected.update(metadatas[i])
            self.assertEquals(self.store._metadata[keys[i]], metadatas[i])

    def test_from_file(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_from_file()
        self.assertEqual(self.store._data['test3'], 'test4')

    def test_from_file_large(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_from_file_large()
        self.assertEqual(self.store._data['test3'], 'test4'*10000000)

    def test_from_bytes(self):
        self.skipTest('Not Implemented')
        super(SqliteStoreWriteTest, self).test_from_bytes()
        self.assertEqual(self.store._data['test3'], 'test4')
    """
        