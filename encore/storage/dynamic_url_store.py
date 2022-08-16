#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in
# LICENSE.txt
#
"""
Dynamic URL Store
=================

This module contains the :py:class:`~DynamicURLStore` store that communicates
with a remote HTTP server which provides the actual data storage.  This is a
store which implements the basic operations via HTTP GET, POST, PUT and DELETE
commands as described in the class documentation.

The implementation relies on the third-party `requests` library to handle the
HTTP operations.

"""

from email.utils import parsedate_tz, mktime_tz
import json

from urllib.parse import quote

import requests

from .abstract_store import AbstractAuthorizingStore, Value, AuthorizationError
from .utils import DummyTransactionContext, BufferIteratorIO, buffer_iterator

_requests_version = requests.__version__.split('.')[0]

DEFAULT_PARTS = {'data': 'data',
                 'metadata': 'metadata',
                 'permissions': 'auth'}


class RequestsURLValue(Value):

    def __init__(self, session, base_url, key,
                 url_format='{base}/{key}/{part}', parts=DEFAULT_PARTS):
        self._session = session
        self._base_url = base_url
        self._key = key
        self._url_format = url_format
        self._parts = parts
        self._data_response = None
        self._get_info()

    def _get_info(self):
        response = self._session.head(self._url('data'))
        self._validate_response(response)
        size = response.headers.get('Content-Length', None)
        if size is not None:
            size = int(size)
        self._size = size

        modified = response.headers.get('Last-Modified', None)
        if modified is not None:
            modified = mktime_tz(parsedate_tz(modified))
        self._modified = modified

        mimetype = response.headers.get('Content-Type',
                                        'application/octet-stream')
        self._mimetype = mimetype

    def _url(self, part):
        return self._url_format.format(base=self._base_url,
                                       key=self._key,
                                       part=self._parts[part])

    def _validate_response(self, response):
        if response.status_code == 404:
            raise KeyError(self._key)
        elif response.status_code == 403 or response.status_code == 401:
            raise AuthorizationError(self._key)
        response.raise_for_status()

    @property
    def data(self):
        if self._data_response is None:
            self.open()
        return self._data_response.raw

    @property
    def metadata(self):
        headers = {'Accept': 'application/json'}
        response = self._session.get(self._url('metadata'), headers=headers)
        self._validate_response(response)

        metadata = json.loads(response.text)
        return metadata

    @property
    def permissions(self):
        headers = {'Accept': 'application/json'}
        response = self._session.get(self._url('permissions'),
                                     headers=headers)
        self._validate_response(response)
        permissions = json.loads(response.text)
        return permissions

    @property
    def size(self):
        if self._data_response is not None:
            self.open()
        return self._size

    @property
    def modified(self):
        if self._data_response is not None:
            self.open()
        return self._modified

    @property
    def mimetype(self):
        if self._data_response is not None:
            self.open()
        return self._mimetype

    def range(self, start=None, end=None):
        # need to build a reqquest with a range header
        start_string = str(start) if start is not None else ''
        end_string = str(end) if end is not None else ''
        headers = {
            'range': 'bytes={0}-{1}'.format(start_string, end_string)
        }
        if _requests_version == '0':
            data = self._session.get(self._url('data'),
                                     headers=headers, prefetch=False)
        else:
            data = self._session.get(self._url('data'),
                                     headers=headers, stream=True)
        if data.status_code == 206:
            # it worked!
            return data.raw
        else:
            # we don't support range requests...
            self._validate_response(data)
            if start is not None:
                data.raw.read(start)
            else:
                start = 0
            if end is not None:
                max_bytes = end - start
                return BufferIteratorIO(buffer_iterator(data.raw,
                                                        max_bytes=max_bytes))
            else:
                return data.raw

    def open(self):
        # XXX in future add support for compression
        headers = {'Accept-Encoding': ''}
        if _requests_version == '0':
            self._data_response = self._session.get(self._url('data'),
                                                    prefetch=False,
                                                    headers=headers)
        else:
            self._data_response = self._session.get(self._url('data'),
                                                    stream=True,
                                                    headers=headers)
        self._validate_response(self._data_response)

        size = self._data_response.headers.get('Content-Length', None)
        if size is not None:
            size = int(size)
        self._size = size

        modified = self._data_response.headers.get('Last-Modified', None)
        if modified is not None:
            modified = mktime_tz(parsedate_tz(modified))
        self._modified = modified

        mimetype = self._data_response.headers.get('Content-Type',
                                                   'application/octet-stream')
        self._mimetype = mimetype

        return self._data_response.raw


class DynamicURLStore(AbstractAuthorizingStore):
    """ Store implementation which gets and sets from a web server

    This store expects a server which exposes URLs for each key.  By default
    these URLs are of the form::

        <base>/<key>/<part>

    Where `<base>` is a common prefix, `<key>` is the key of interest, and
    `<part>` is one of "data", "metadata" or "auth".  If the store does not
    follow this format, you can provide a differnt ``url_format`` argument
    and a different mapping of `<part>` to aspects of the key.

    The server is expected to respond to queries against these URLS in the
    following ways:

        `GET <base>/<key>/data`
            return the bytes in the body of the response

        `PUT <base>/<key>/data`
            accept the data bytes from the body of the request

        `GET <base>/<key>/metadata`
            return metadata as JSON

        `PUT <base>/<key>/metadata`
            set the metadata based on JSON contained in the body of the request

        `POST <base>/<key>/metadata`
            update the metadata based on JSON contained in the body of the
            request (as `dict.update()`)

        `GET <base>/<key>/auth`
            return permissions information as JSON

        `PUT <base>/<key>/auth`
            set the permissions based on JSON contained in the body of the
            request

        `POST <base>/<key>/metadata`
            update the permissions based on JSON contained in the body of the
            request

    In addition, a DELETE request to a URL of the form `<base>/<key>` should
    remove the key from the remote store.  This pattern is configurable via
    the ``url_format_no_part`` argument to the constructor.

    In addition, the server should have a query URL which accepts GET reuqests
    containing a JSON data structure of metadata key, value pairs to filter
    with, and should return a list of macthing keys, one per line.

    """

    def __init__(self, base_url, query_url, url_format='{base}/{key}/{part}',
                 url_format_no_part='{base}/{key}', parts=DEFAULT_PARTS):
        super(AbstractAuthorizingStore, self).__init__()
        self.base_url = base_url
        self.query_url = query_url
        self._user_tag = None
        self.url_format = url_format
        self.url_format_no_part = url_format_no_part
        self.parts = parts

    def user_tag(self):
        return self._user_tag

    def _url(self, key, part=""):
        safe_key = quote(key, safe="/~!$&'()*+,;=:@")

        if part:
            url = self.url_format.format(base=self.base_url,
                                         key=safe_key,
                                         part=self.parts[part])
            return url
        else:
            url = self.url_format_no_part.format(base=self.base_url,
                                                 key=safe_key)
            return url

    def _validate_response(self, response, key):
        if response.status_code == 404:
            raise KeyError(key)
        elif response.status_code == 403 or response.status_code == 401:
            raise AuthorizationError(key)
        response.raise_for_status()

    def get(self, key):
        safe_key = quote(key, safe="/~!$&'()*+,;=:@")
        result = RequestsURLValue(self._session, self.base_url, safe_key,
                                  self.url_format, self.parts)

        return result
    get.__doc__ = AbstractAuthorizingStore.get.__doc__

    def connect(self, credentials=None):
        """ Connect to a DynamicURLStore

        Parameters
        ----------
        credentials : (user_tag, requests.Session)
            The credentials are a tuple containing ther user's permission tag
            and a requests Session initialized with appropriate authentication.

        """
        self._user_tag, self._session = credentials
        super(DynamicURLStore, self).connect()

    def disconnect(self):
        super(DynamicURLStore, self).disconnect()

    def is_connected(self):
        return super(DynamicURLStore, self).is_connected()

    def info(self):
        return super(DynamicURLStore, self).info()

    def set(self, key, value, buffer_size=1048576):
        if isinstance(value, tuple):
            data, metadata = value
        else:
            data = value.data
            metadata = value.metadata
        with self.transaction('Setting key "%s"' % key):
            self.set_data(key, data, buffer_size)
            self.set_metadata(key, metadata)
    set.__doc__ = AbstractAuthorizingStore.set.__doc__

    def delete(self, key):
        self._session.delete(self._url(key))
    delete.__doc__ = AbstractAuthorizingStore.delete.__doc__

    def get_data(self, key):
        headers = {'Accept-Encoding': ''}
        if _requests_version == '0':
            response = self._session.get(self._url(key, 'data'),
                                         prefetch=False, headers=headers)
        else:
            response = self._session.get(self._url(key, 'data'),
                                         stream=True, headers=headers)
        self._validate_response(response, key)
        return response.raw
    get_data.__doc__ = AbstractAuthorizingStore.get_data.__doc__

    def set_data(self, key, data, buffer_size=1048576):
        response = self._session.put(self._url(key, 'data'), data=data)
        self._validate_response(response, key)
    set_data.__doc__ = AbstractAuthorizingStore.set_data.__doc__

    def set_metadata(self, key, metadata):
        response = self._session.put(self._url(key, 'metadata'),
                                     json.dumps(metadata))
        self._validate_response(response, key)
    set_metadata.__doc__ = AbstractAuthorizingStore.set_metadata.__doc__

    def get_metadata(self, key, select=None):
        response = self._session.get(self._url(key, 'metadata'))
        self._validate_response(response, key)
        if _requests_version == '0':
            metadata = response.json
        else:
            metadata = response.json()
        if select is not None:
            return dict((k, metadata[k]) for k in select if k in metadata)
        else:
            return metadata
    get_metadata.__doc__ = AbstractAuthorizingStore.get_metadata.__doc__

    def update_metadata(self, key, metadata):
        response = self._session.post(self._url(key, 'metadata'),
                                      json.dumps(metadata))
        self._validate_response(response, key)
    update_metadata.__doc__ = AbstractAuthorizingStore.update_metadata.__doc__

    def get_permissions(self, key):
        response = self._session.get(self._url(key, 'permissions'))
        self._validate_response(response, key)
        if _requests_version == '0':
            return response.json
        else:
            return response.json()
    get_permissions.__doc__ = AbstractAuthorizingStore.get_permissions.__doc__

    def set_permissions(self, key, permissions):
        response = self._session.put(self._url(key, 'permissions'),
                                     json.dumps(permissions))
        self._validate_response(response, key)
        response.raise_for_status()
    set_permissions.__doc__ = AbstractAuthorizingStore.set_permissions.__doc__

    def update_permissions(self, key, permissions):
        response = self._session.post(self._url(key, 'permissions'),
                                      json.dumps(permissions))
        self._validate_response(response, key)
        response.raise_for_status()
    update_permissions.__doc__ = AbstractAuthorizingStore.update_permissions.__doc__  # noqa

    def transaction(self, notes):
        """ Provide a transaction context manager

        This class does not support transactions, so it returns a dummy object.

        """
        return DummyTransactionContext(self)

    def query(self, select=None, **kwargs):
        for key in self.query_keys(**kwargs):
            yield (key, self.get_metadata(key, select=select))
    query.__doc__ = AbstractAuthorizingStore.query.__doc__

    def query_keys(self, **kwargs):
        params = {key: json.dumps(value) for key, value in kwargs.items()}
        response = self._session.get(self.query_url, params=params)
        self._validate_response(response, params)
        for line in response.iter_lines():
            yield line
    query_keys.__doc__ = AbstractAuthorizingStore.query_keys.__doc__
