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
import SocketServer
from BaseHTTPServer import HTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler

from encore.events.api import EventManager
import encore.storage.tests.abstract_test as abstract_test
from ..static_url_store import StaticURLStore

class StaticURLStoreReadTest(abstract_test.AbstractStoreReadTest):
    
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
        os.mkdir(os.path.join(self.path, 'data'))
        self._write_data('test1', 'test2\n')

        metadata = {}
        metadata['test1'] = {
            'a_str': 'test3',
            'an_int': 1,
            'a_float': 2.0,
            'a_bool': True,
            'a_list': ['one', 'two', 'three'],
            'a_dict': {'one': 1, 'two': 2, 'three': 3}
        }
        for i in range(10):
            self._write_data('key%d'%i, 'value%d' % i)
            metadata['key%d'%i] = {'query_test1': 'value',
                'query_test2': i}
            if i % 2 == 0:
                metadata['key%d'%i]['optional'] = True
        self._write_index('index.json', json.dumps(metadata))
        
        self._running = True
        
        self._set_up_server()
        time.sleep(1)


        self.store = StaticURLStore(EventManager(), self._get_base_url(), 'data/', 'index.json')
        self.store.connect()
    
    def tearDown(self):
        self.store.disconnect()
        self._running = False
        self._tear_down_server()
        rmtree(self.path)

    def utils_large(self):
        self._write_data('test3', 'test4'*10000000)
        metadata = json.load(open(os.path.join(self.path, 'index.json'), 'rb'))
        metadata['test3'] = {}
        self._write_index('index.json', json.dumps(metadata))
        self.store.update_index()

    def _set_up_server(self):
        pass

    def _tear_down_server(self):
        pass

    def _get_base_url(self):
        return 'file://'+self.path+'/'
            
    def _write_data(self, filename, data):
        with file(os.path.join(self.path, 'data', filename), 'wb') as fp:
            fp.write(data)
    
    def _write_index(self, filename, data):
        with file(os.path.join(self.path, filename), 'wb') as fp:
            fp.write(data)

class ThreadedHTTPServer(SocketServer.ThreadingMixIn, HTTPServer):
    pass

class StaticURLStoreHTTPReadTest(StaticURLStoreReadTest):
    
    def _get_base_url(self):
        return 'http://localhost:%s/' % self.port
 
    def _set_up_server(self):
        self.port = 8080
        self._oldwd = os.getcwd()
        os.chdir(self.path)
        
        self.server = ThreadedHTTPServer(('localhost', self.port), SimpleHTTPRequestHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, args=(0.1,))
        self.server_thread.daemon = True
        self.server_thread.start()

    def _tear_down_server(self):
        self.server.shutdown()
        del self.server
        os.chdir(self._oldwd)


