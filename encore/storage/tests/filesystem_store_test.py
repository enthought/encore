#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

import os
import threading
from tempfile import mkdtemp
from shutil import rmtree
import json
import time
import random

import encore.storage.tests.abstract_test as abstract_test
from ..filesystem_store import FileSystemStore, init_shared_store

class FileSystemStoreMixin(object):
    
    def utils_large(self):
        self._write_data('test3', 'test4'*10000000)
        self._write_metadata('test3', {})
            
    def _write_data(self, filename, data):
        with file(os.path.join(self.path, filename+'.data'), 'wb') as fp:
            fp.write(data)
    
    def _write_metadata(self, filename, metadata):
        with file(os.path.join(self.path, filename+'.metadata'), 'wb') as fp:
            json.dump(metadata, fp)
    

class FileSystemStoreReadTest(abstract_test.AbstractStoreReadTest, FileSystemStoreMixin):
    resolution = 'second'
    
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
        super(FileSystemStoreReadTest, self).setUp()
        self.path = mkdtemp()
        init_shared_store(self.path)
        self._write_data('test1', 'test2\n')

        self._write_metadata('test1', {
            'a_str': 'test3',
            'an_int': 1,
            'a_float': 2.0,
            'a_bool': True,
            'a_list': ['one', 'two', 'three'],
            'a_dict': {'one': 1, 'two': 2, 'three': 3}
        })
        
        for i in range(10):
            self._write_data('key%d'%i, 'value%d' % i)
            metadata = {'query_test1': 'value',
                'query_test2': i}
            if i % 2 == 0:
                metadata['optional'] = True
            self._write_metadata('key%d'%i, metadata)

        self.store = FileSystemStore(self.path)
        self.store.connect()

    def tearDown(self):
        rmtree(self.path)

    def utils_large(self):
        self._write_data('test3', 'test4'*10000000)
        self._write_metadata('test3', {})

class FileSystemStoreWriteTest(abstract_test.AbstractStoreWriteTest, FileSystemStoreMixin):
    resolution = 'second'

    def setUp(self):
        """ Set up a data store for the test case
        
        The store should have:
            
            * a key 'test1' with a file-like data object containing the
              bytes 'test2\n' and metadata {'a_str': 'test3', 'an_int': 1,
              'a_float': 2.0, 'a_bool': True, 'a_list': ['one', 'two', 'three'],
              'a_dict': {'one': 1, 'two': 2, 'three': 3}}
             
            * a series of keys 'existing_key0' through 'existing_key9' with
              data containing 'existing_value0' through 'existing_value9' and
              metadata {'meta': True, 'meta1': 0} through {'meta': True, 'meta1': -9}
       
        and set into 'self.store'.
        """
        super(FileSystemStoreWriteTest, self).setUp()
        self.path = mkdtemp()
        init_shared_store(self.path)
        self._write_data('test1', 'test2\n')

        self._write_metadata('test1', {
            'a_str': 'test3',
            'an_int': 1,
            'a_float': 2.0,
            'a_bool': True,
            'a_list': ['one', 'two', 'three'],
            'a_dict': {'one': 1, 'two': 2, 'three': 3}
        })
        
        for i in range(10):
            key = 'existing_key'+str(i)
            data = 'existing_value'+str(i)
            metadata = {'meta': True, 'meta1': -i}
            self._write_data(key, data)
            self._write_metadata(key, metadata)

        self.store = FileSystemStore(self.path)
        self.store.connect()

    def tearDown(self):
        rmtree(self.path)
