#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

# Standard Library imports.
import glob

# Local imports.
from encore.events.api import EventManager
from encore.storage.locking_filesystem_store import LockingFileSystemStore
from .filesystem_store_test import FileSystemStoreReadTest, FileSystemStoreWriteTest


class LockingFileSystemStoreReadTest(FileSystemStoreReadTest):

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
        super(LockingFileSystemStoreReadTest, self).setUp()
        self.store = LockingFileSystemStore(EventManager(), self.path)
        self.store.connect()

class LockingFileSystemStoreWriteTest(FileSystemStoreWriteTest):
    
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
        super(LockingFileSystemStoreWriteTest, self).setUp()
        self.store = LockingFileSystemStore(EventManager(), self.path)
        self.store.connect()

    def test_changelog(self):
        store = self.store
        store._logrotate_limit = 10
        store._log_keep = 1
        store.set_metadata('key', {'name':'key'})
        for i in range(12):
            store.set_metadata('key%d'%i, {'name':'key%d'%i})
            store.update_metadata('key', {'name':'key-%d'%i})

        log = [line.split(' ', 4) for line in open(self.store._log_file).readlines()]
        self.assertEqual(log[-1][0], '25')
        self.assertEqual(len(glob.glob(store._log_file+'.*')), store._log_keep)

if __name__ == '__main__':
    import unittest
    unittest.main()
