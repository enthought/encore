#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

# Standard Library imports.
import glob
import threading
import datetime
import time

# Local imports.
from encore.storage.locking_filesystem_store import LockingFileSystemStore
from .filesystem_store_test import FileSystemStoreReadTest, FileSystemStoreWriteTest
from encore.storage.events import StoreSetEvent


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
        self.store = LockingFileSystemStore(self.path)
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
        self.store = LockingFileSystemStore(self.path)
        self.store.connect()

    def test_changelog(self):
        """ Test whether changelog is being written. """
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

    def test_remote_change_event(self):
        """ Test whether changes by other users result in events. """
        store2 = LockingFileSystemStore(self.path)
        store2._remote_event_poll_interval = self.store._remote_event_poll_interval = 0.1

        events = []
        lock = threading.Lock()
        def callback(event):
            events.append(event)
            lock.release()
        self.store.event_manager.connect(StoreSetEvent, callback)
        lock.acquire()
        store2.set_metadata('key', {'name':'key'})

        # Wait until the event is emitted.
        with lock:
            self.assertEqual(len(events), 1)

        thread = self.store._remote_poll_thread
        self.store._remote_poll_thread = None
        thread.join()
        thread = store2._remote_poll_thread
        store2._remote_poll_thread = None
        thread.join()

    def test_fast_query(self):
        """ Test fast query for specific cases. """
        store = self.store
        # Single machine test, no timedelta.
        store._max_time_delta = datetime.timedelta()

        old = datetime.datetime.utcnow()
        store.set_metadata('file.1', {'timestamp':str(old)})
        time.sleep(1)
        new = datetime.datetime.utcnow()
        not_so_old = new - datetime.timedelta(seconds=0.5)
        store.set_metadata('file.2', {'timestamp':str(new)})
        store.set_metadata('dir.3', {'timestamp':str(new)})
        store.set_metadata('foo.4', {'timestamp':str(new)})

        # Query on types.
        self.assertItemsEqual(store.query_keys(type='file'),
                              ['file.1', 'file.2'])
        self.assertItemsEqual(store.query_keys(type='dir'),
                              ['dir.3'])
        self.assertItemsEqual(store.query_keys(type='foo'),
                              [])

        # Note: The timestamp metadata key is not used to return results.
        # The results are for service side logged last_modified, which
        # is not a metadata key but a special query.
        self.assertItemsEqual(store.get_modified_keys(not_so_old),
                              ['file.2', 'dir.3', 'foo.4'])


if __name__ == '__main__':
    import unittest
    unittest.main()

