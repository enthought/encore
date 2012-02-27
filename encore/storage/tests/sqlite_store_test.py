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
import time

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
        super(SqliteStoreReadTest, self).setUp()
        self.path = mkdtemp()
        self.db_file = os.path.join(self.path, 'db.sqlite')
        
        connection = sqlite3.connect(self.db_file)
        
        connection.execute("""
            create table store (
                key text primary key,
                metadata dict,
                created float,
                modified float,
                data blob
            )
            """)
        
        t = time.time()
        connection.execute("""insert into store values (?, ?, ?, ?, ?)""", (
            b'test1',
            {
                'a_str': 'test3',
                'an_int': 1,
                'a_float': 2.0,
                'a_bool': True,
                'a_list': ['one', 'two', 'three'],
                'a_dict': {'one': 1, 'two': 2, 'three': 3}
            }, t, t,
            buffer(b'test2\n')))
        for i in range(10):
            key = b'key%d'%i
            data = buffer(b'value%d' % i)
            metadata = {'query_test1': 'value', 'query_test2': i}
            if i % 2 == 0:
                metadata['optional'] = True
            connection.execute("""insert into store values (?, ?, ?, ?, ?)""", (key, metadata, t, t, data))
        connection.commit()
        
        connection = None
        
        self.store = SqliteStore(self.db_file, 'store')
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
                created float,
                modified float,
                data blob
            )
            """)
        
        t = time.time()
        connection.execute("""insert into store values (?, ?, ?, ?, ?)""", (
            b'test1',
            {
                'a_str': 'test3',
                'an_int': 1,
                'a_float': 2.0,
                'a_bool': True,
                'a_list': ['one', 'two', 'three'],
                'a_dict': {'one': 1, 'two': 2, 'three': 3}
            }, t, t,
            buffer(b'test2\n')))
        for i in range(10):
            key = b'existing_key%d'%i
            data = buffer(b'existing_value%d' % i)
            metadata = {'meta': True, 'meta1': -i}
            connection.execute("""insert into store values (?, ?, ?, ?, ?)""", (key, metadata, t, t, data))
        connection.commit()
        
        connection = None

        self.store = SqliteStore(self.db_file, 'store')
        self.store.connect()
