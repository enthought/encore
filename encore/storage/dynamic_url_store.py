#
# (C) Copyright 2012 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#

import json
import urllib
import rfc822

from .abstract_store import AbstractAuthorizingStore, Value, AuthorizationError
from .utils import DummyTransactionContext

class RequestsURLValue(Value):
    
    def __init__(self, session, base_url, key, url_format='{base}/{key}/{part}',
            parts=None):
        self._session = session
        self._base_url = base_url
        self._key = key
        self._url_format = url_format
        self._parts = parts if parts is not None else {'data': 'data',
            'metadata': 'metadata', 'permissions': 'auth'}
        self._data_response = None
        self._size = None
        self._created = None
        self._modified = None
    
    def _url(self, part):
        return self._url_format.format(base=self._base_url, key=self._key,
            part=part)
    
    @property
    def data(self):
        if self._data_response is None:
            self.open()
        return self._data_response.raw
    
    @property
    def metadata(self):
        headers = {'Accept': 'application/json'}
        response = self._session.get(self._url('metadata'), headers=headers)
        if response.status_code == 404:
            raise KeyError(self._key)
        elif response.status_code == 403:
            raise AuthorizationError(self._key)
        response.raise_for_status()

        metadata = json.loads(response.text)
        return metadata
        
    @property
    def permissions(self):
        headers = {'Accept': 'application/json'}
        response = self._session.get(self._url('permissions'), headers=headers)
        if response.status_code == 404:
            raise KeyError(self._key)
        elif response.status_code == 403:
            raise AuthorizationError(self._key)
        response.raise_for_status()

        permissions = json.loads(response.text)
        return permissions
        
    @property
    def size(self):
        if self._data_stream is not None:
            self.open()
        return self._size
        
    @property
    def modified(self):
        if self._data_stream is not None:
            self.open()
        return self._modified
       
    def open(self):
        self._data_response = self._session.get(self._url('data'),
            prefetch=False)
        if self._data_response.status_code == 404:
            raise KeyError(self._key)
        elif self._data_response.status_code == 403:
            raise AuthorizationError(self._key)
        self._data_response.raise_for_status()
            
        size = self._data_response.headers['Content-Length']
        if size is not None:
            size = int(size)
        self._size = size
        
        modified = self._data_response.headers['Last-Modified']
        if modified is not None:
            modified = rfc822.mktime_tz(rfc822.parsedate_tz(modified))
        self._modified = modified
    
        return self._data_response.raw
        

class DynamicURLStore(AbstractAuthorizingStore):
    
    def __init__(self, base_url, query_url, url_format='{base}/{key}/{part}',
            parts=None):
        super(AbstractAuthorizingStore, self).__init__()
        self.base_url = base_url
        self.query_url = query_url
        self.url_format = url_format
        self.parts = parts if parts is not None else {'data': 'data',
            'metadata': 'metadata', 'permissions': 'auth'}
    
    def connect(self, credentials=None):
        """ Connect to a DynamicURLStore
        
        Parameters
        ----------
        credentials : requests.Session
            The credentials are a requests Session with appropriate
            authentication already set up.
        
        """
        self._session = credentials
        super(DynamicURLStore, self).connect()

    def _url(self, key, part):
        key = urllib.quote(key, safe="/~!$&'()*+,;=:@")
        return self._url_format.format(base=self._base_url, key=key, part=part)

    def get(self, key):
        safe_key = urllib.quote(key, safe="/~!$&'()*+,;=:@")
        return RequestsURLValue(self._session, self.base_url, safe_key, self.url_format,
            self.parts)
    get.__doc__ = AbstractAuthorizingStore.get.__doc__
    
    def set(self, key, value, buffer_size=1048576):
        super(DynamicURLStore, self).set(key, value, buffer_size)
    set.__doc__ = AbstractAuthorizingStore.set.__doc__
 
    def delete(self, key):
        pass
    delete.__doc__ = AbstractAuthorizingStore.delete.__doc__
   
    def set_data(self, key, data, buffer_size=1048576):
        response = self._session.put(self._url(key, 'data'), data)
        if response.status_code == 404:
            raise KeyError(self._key)
        elif response.status_code == 403:
            raise AuthorizationError(self._key)
        response.raise_for_status()
    set_data.__doc__ = AbstractAuthorizingStore.set_data.__doc__
    
    def set_metadata(self, key, metadata):
        response = self._session.put(self._url(key, 'metadata'),
            json.dumps(metadata))
        if response.status_code == 404:
            raise KeyError(self._key)
        elif response.status_code == 403:
            raise AuthorizationError(self._key)
        response.raise_for_status()
    set_metadata.__doc__ = AbstractAuthorizingStore.set_metadata.__doc__
    
    def update_metadata(self, key, metadata):
        response = self._session.post(self._url(key, 'metadata'),
            json.dumps(metadata))
        if response.status_code == 404:
            raise KeyError(self._key)
        elif response.status_code == 403:
            raise AuthorizationError(self._key)
        response.raise_for_status()
    update_metadata.__doc__ = AbstractAuthorizingStore.update_metadata.__doc__

    def set_permissions(self, key, permissions):
        response = self._session.put(self._url(key, 'permissions'),
            json.dumps(permissions))
        if response.status_code == 404:
            raise KeyError(self._key)
        elif response.status_code == 403:
            raise AuthorizationError(self._key)
        response.raise_for_status()
    set_metadata.__doc__ = AbstractAuthorizingStore.set_metadata.__doc__
    
    def update_permissions(self, key, permissions):
        response = self._session.post(self._url(key, 'permissions'),
            json.dumps(permissions))
        if response.status_code == 404:
            raise KeyError(self._key)
        elif response.status_code == 403:
            raise AuthorizationError(self._key)
        response.raise_for_status()
    update_metadata.__doc__ = AbstractAuthorizingStore.update_metadata.__doc__

    def transaction(self, notes):
        """ Provide a transaction context manager
        
        This class does not support transactions, so it returns a dummy object.

        """
        return DummyTransactionContext(self)

    def query(self, select=None, **kwargs):
        for key in self.query_keys(**kwargs):
            yield self.get_metadata(key, select=select)
    query.__doc__ = AbstractAuthorizingStore.query.__doc__

    def query_keys(self, **kwargs):
        response = self._session.get(self.query_url, params=kwargs)
        if response.status_code == 403:
            raise AuthorizationError(self._key)
        response.raise_for_status()
        for line in response.iter_lines():
            yield line
    query_keys.__doc__ = AbstractAuthorizingStore.query_keys.__doc__
            