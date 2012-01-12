#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

import re
import json
from cgi import parse_qs

from wsgiref.headers import Headers
from wsgiref.simple_server import make_server

from encore.storage.utils import buffer_iterator

arg_pattern = re.compile(r'[a-zA-Z][_a-zA-Z0-9]*')

patterns = {
    'GET': [],
    'POST': [],
    'DELETE': [],
}

def get(regex):
    compiled_regex = re.compile(regex)
    def wrapper(f):
        patterns['GET'].append((compiled_regex, f))
        return f
    return wrapper

def post(regex):
    compiled_regex = re.compile(regex)
    def wrapper(f):
        patterns['POST'].append((compiled_regex, f))
        return f
    return wrapper

def delete(regex):
    compiled_regex = re.compile(regex)
    def wrapper(f):
        patterns['DELETE'].append((compiled_regex, f))
        return f
    return wrapper

class StoreResponse(object):
    def __init__(self, application, environ, start_response):
        self.application = application
        self.store = application.store
        self.environ = environ
        self.start_response = start_response

class StoreApplication(object):
    patterns = patterns
    
    def __init__(self, store):
        self.store = store
    
    def __call__(self, environ, start_response):
        for pattern, callback in self.patterns['GET']:
            match = pattern.match(environ['PATH_INFO'])
            if match is not None:
                response = StoreResponse(self, environ, start_response)
                return callback(response, *match.groups())
        else:
            # no match
            status = '404 Not Found'
            response_headers = Headers([])
            response_headers.add_header('Content-Type', 'text/plain'),
            response_headers.add_header('Content-Length', str(len('Not Found')))
            
            start_response(status, str(response_headers))
            return ['Not Found']
    
def key_error(response, key):
    result = 'KeyError: "%s" not found' % key
    status = '404 Not Found'
    response_headers = [
        ('Content-Type', 'text/plain'),
        ('Content-Length', str(len(result)))
    ]
    response.start_response(status, response_headers)
    return [result]
    
    
@get(r'/data/(.*)')
def get_data(response, key):
    try:
        data, metadata = response.store.get(key)
        status = '200 OK'
        response.start_response(status, [])
        return buffer_iterator(data)
    except KeyError as exc:
        return key_error(response, key)


@post(r'/data/(.*)')
def set_data(response, key):
    filelike = hashing_file(response.environ['wsgi.input'], hashlib.sha1())
    response.store.set_data(key, filelike)
    result = {'sha1': filelike.hash.hexdigest(), 'len': filelike.len}
    status = '200 OK'
    response_headers = Headers([])
    response_headers.add_header('Content-Type', 'text/plain'),
    response_headers.add_header('Content-Length', str(len(result)))
    response.start_response(status, str(response_headers))
    return [result]


@get(r'/metadata/(.*)')
def get_metadata(response, key):
    try:
        metadata = response.store.get_metadata(key)
        result = json.dumps(metadata)
        
        status = '200 OK'
        response_headers = Headers([])
        response_headers.add_header('Content-Type', 'text/json'),
        response_headers.add_header('Content-Length', str(len(result)))
        response.start_response(status, str(response_headers))
        return [result]
    except KeyError as exc:
        return key_error(response, key)
        
        
@post(r'/metadata/(.*)')
def set_metadata(response, key):
    metadata = json.load(response.environ['wsgi.input'])
    response.store.set_metadata(key, metadata)
    result = ''
    status = '200 OK'
    response_headers = Headers([])
    response_headers.add_header('Content-Type', 'text/json'),
    response_headers.add_header('Content-Length', str(len(result)))
    response.start_response(status, str(response_headers))
    return [result]


@get(r'/exists/(.*)')
def exists(response, key):
    result = str(response.store.exists(key))
    status = '200 OK'
    response_headers = Headers([])
    response_headers.add_header('Content-Type', 'text/json'),
    response_headers.add_header('Content-Length', str(len(result)))
    response.start_response(status, str(response_headers))
    return [result]


@delete(r'/(.*)')
def delete_key(response, key):
    try:
        response.store.delete(key)
        result = ''
        status = '200 OK'
        response_headers = Headers([])
        response_headers.add_header('Content-Type', 'text/json'),
        response_headers.add_header('Content-Length', str(len(result)))
        response.start_response(status, str(response_headers))
        return [result]
    except KeyError as exc:
        return key_error(response, key)


@get(r'/query$')
def get_query(response):
    try:
        query_args = parse_qs(response.environ['QUERY_STRING'])
        
        # sanitize args
        sanitized = dict((arg, json.loads(value[0])) for arg, value in query_args.items() if arg_pattern.match(arg))
        result = json.dumps(list(response.store.query(**sanitized)))
        
        status = '200 OK'
        response_headers = Headers([])
        response_headers.add_header('Content-Type', 'text/json'),
        response_headers.add_header('Content-Length', str(len(result)))
        response.start_response(status, [])
        return [result]
    except KeyError as exc:
        return key_error(response, key)


@post(r'/query$')
def post_query(response):
    try:
        query_args = json.load(response.environ['wsgi.input'])
        
        # sanitize args
        sanitized = dict((arg, value) for arg, value in query_args.items() if arg_pattern.match(arg))
        result = json.dumps(list(response.store.query(*sanitized)))
        
        status = '200 OK'
        response_headers = Headers([])
        response_headers.add_header('Content-Type', 'text/json'),
        response_headers.add_header('Content-Length', str(len(result)))
        response.start_response(status, [])
        return [result]
    except KeyError as exc:
        return key_error(response, key)
   

@get(r'/query_keys$')
def get_query_keys(response):
    try:
        query_args = parse_qs(response.environ['QUERY_STRING'])
        
        # sanitize args
        sanitized = dict((arg, json.loads(value[0])) for arg, value in query_args.items() if arg_pattern.match(arg))
        result = json.dumps(list(response.store.query_keys(**sanitized)))
        
        status = '200 OK'
        response_headers = Headers([])
        response_headers.add_header('Content-Type', 'text/json'),
        response_headers.add_header('Content-Length', str(len(result)))
        response.start_response(status, [])
        return [result]
    except KeyError as exc:
        return key_error(response, key)

@post(r'/query_keys$')
def post_query_keys(response):
    try:
        query_args = json.load(response.environ['wsgi.input'])
        
        # sanitize args
        sanitized = dict((arg, value) for arg, value in query_args.items() if arg_pattern.match(arg))
        result = json.dumps(list(response.store.query_keys(*sanitized)))
        
        status = '200 OK'
        response_headers = Headers([])
        response_headers.add_header('Content-Type', 'text/json'),
        response_headers.add_header('Content-Length', str(len(result)))
        response.start_response(status, [])
        return [result]
    except KeyError as exc:
        return key_error(response, key)
   

def serve(address, port, store):
    application = StoreApplication(store)
    server = make_server(address, port, application)
    server.serve_forever()

if __name__ == '__main__':
    import sys
    from encore.events.api import EventManager
    from encore.storage.dict_memory_store import DictMemoryStore
    address = sys.argv[1]
    port = int(sys.argv[2])
    store = DictMemoryStore(EventManager())
    store.from_bytes('test_path', 'foo')
    store.set_metadata('test_path', {'mimetype': 'text/plain', 'len': 3})
    store.from_bytes('test_path_2', 'foobar')
    store.set_metadata('test_path_2', {'mimetype': 'text/plain', 'len': 6})
    store.connect()
    serve(address, port, store)
    