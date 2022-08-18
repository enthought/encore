#
# (C) Copyright 2011-2022 Enthought, Inc., Austin, TX
# All right reserved.
#
# This file is open source software distributed according to the terms in LICENSE.txt
#

from contextlib import contextmanager
import os
from shutil import rmtree
from tempfile import mkdtemp
import time
from unittest import skip

from io import BytesIO

from ..utils import add_context_manager_support


def create_file_like_data(data_bytes):
    # The store are supposed to received file-like data streams
    return add_context_manager_support(
        BytesIO(data_bytes)
    )


@contextmanager
def temp_dir():
    """ Create a temporary directory and ensure it is always cleaned up.
    """
    path = mkdtemp()
    yield path
    rmtree(path)


class StoreReadTestMixin(object):

    resolution = 'arbitrary'

    def utils_large(self):
        self.store.from_bytes('test3', 'test4'*10000000)

    def test_get(self):
        value = self.store.get('test1')
        with value.data as data:
            self.assertEqual(data.read(), b'test2\n')
        self.assertEqual(value.metadata, {
            'a_str': 'test3',
            'an_int': 1,
            'a_float': 2.0,
            'a_bool': True,
            'a_list': ['one', 'two', 'three'],
            'a_dict': {'one': 1, 'two': 2, 'three': 3}
        })
        self.assertEqual(value.size, 6)

    def test_get_copies(self):
        # Metadata returned from separate get()s should not be same object
        data1, metadata1 = self.store.get('test1')
        metadata1['extra_key'] = 'extra_value'
        data2, metadata2 = self.store.get('test1')
        self.assertNotEqual(metadata2, metadata1)
        data1.close()
        data2.close()

    def test_get_data(self):
        data = self.store.get_data('test1')
        with data:
            self.assertEqual(data.read(), b'test2\n')

    def test_get_data_range(self):
        data = self.store.get_data_range('test1', 1, 3)
        with data:
            self.assertEqual(data.read(), b'es')

    def test_get_metadata(self):
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
        metadata1 = self.store.get_metadata('test1')
        metadata1['extra_key'] = 'extra_value'
        metadata2 = self.store.get('test1')
        self.assertNotEqual(metadata2, metadata1)

    def test_get_metadata_select(self):
        metadata = self.store.get_metadata('test1', ['a_str', 'an_int'])
        self.assertEqual(metadata, {
            'a_str': 'test3',
            'an_int': 1,
        })

    def test_get_metadata_select_missing(self):
        metadata = self.store.get_metadata('test1', ['a_str', 'an_int', 'missing'])
        self.assertEqual(metadata, {
            'a_str': 'test3',
            'an_int': 1,
        })

    def test_exists(self):
        self.assertEqual(self.store.exists('test1'), True)
        self.assertEqual(self.store.exists('test2'), False)

    def test_multiget(self):
        result = self.store.multiget('key'+str(i) for i in range(10))
        for i, value in enumerate(result):
            with value.data as data:
                self.assertEqual(data.read(), b'value%i' % i)
            self.assertEqual(value.size, 6)
            expected = {'query_test1': 'value', 'query_test2': i}
            if i % 2 == 0:
                expected['optional'] = True
            self.assertEqual(expected, value.metadata)

    def test_multiget_data(self):
        result = self.store.multiget_data('key'+str(i) for i in range(10))
        for i, data in enumerate(result):
            with data:
                self.assertEqual(data.read(), b'value%i' % i)

    def test_multiget_metadata(self):
        result = self.store.multiget_metadata('key'+str(i) for i in range(10))
        for i, metadata in enumerate(result):
            expected = {'query_test1': 'value', 'query_test2': i}
            if i % 2 == 0:
                expected['optional'] = True
            self.assertEqual(expected, metadata)

    def test_multiget_metadata_select(self):
        result = self.store.multiget_metadata(('key'+str(i) for i in range(10)),
            select=['query_test1', 'optional'])
        for i, metadata in enumerate(result):
            expected = {'query_test1': 'value'}
            if i % 2 == 0:
                expected['optional'] = True
            self.assertEqual(expected, metadata)

    def test_query(self):
        result = sorted(self.store.query(a_str='test3'))
        self.assertEqual(result, [('test1', {'a_str': 'test3', 'an_int': 1,
            'a_float': 2.0, 'a_bool': True, 'a_list': ['one', 'two', 'three'],
            'a_dict': {'one': 1, 'two': 2, 'three': 3}})])

    def test_query_copy(self):
        """ Metadata returned from separate query()s should not be same object"""
        result1 = sorted(self.store.query(a_str='test3'))
        result1[0][1]['extra_key'] = 'extra_value'
        result2 = sorted(self.store.query(a_str='test3'))
        self.assertNotEqual(result1, result2)

    def test_query1(self):
        result = sorted(self.store.query(query_test1='value'))
        expected = sorted(('key%d' % i, {'query_test1': 'value',
            'query_test2': i}) for i in range(10))
        for i, (key, metadata) in enumerate(expected):
            if i % 2 == 0:
                metadata['optional'] = True
        self.assertEqual(result, expected)

    def test_query2(self):
        for i in range(10):
            result = sorted(self.store.query(query_test2=i))
            expected = {'query_test1': 'value', 'query_test2': i}
            if i % 2 == 0:
                expected['optional'] = True
            self.assertEqual(result, [('key%d' % i, expected)])

    def test_query_empty(self):
        result = list(self.store.query(a_str='test1'))
        self.assertEqual(result, [])

    def test_query_select(self):
        result = sorted(self.store.query(['a_str', 'an_int'], a_str='test3'))
        self.assertEqual(result, [('test1', {'a_str': 'test3', 'an_int': 1})])

    def test_query_select_missing(self):
        result = sorted(self.store.query(['query_test1', 'optional'], query_test1='value'))
        expected = sorted(('key%d' % i, {'query_test1': 'value'}) for i in range(10))
        for i, (key, metadata) in enumerate(expected):
            if i % 2 == 0:
                metadata['optional'] = True
        self.assertEqual(result, expected)

    def test_query_keys(self):
        result = sorted(self.store.query_keys(a_str='test3'))
        self.assertEqual(result, ['test1'])

    def test_query1_keys(self):
        result = sorted(self.store.query_keys(query_test1='value'))
        self.assertEqual(result, sorted('key%d' % i for i in range(10)))

    def test_query2_keys(self):
        for i in range(10):
            result = sorted(self.store.query_keys(query_test2=i))
            self.assertEqual(result, ['key%d' % i])

    def test_query_keys_empty(self):
        result = list(self.store.query_keys(a_str='test1'))
        self.assertEqual(result, [])

    def test_glob(self):
        result = sorted(self.store.glob('key*'))
        self.assertEqual(result, sorted('key%d' % i for i in range(10)))

    def test_to_bytes(self):
        data = self.store.to_bytes('test1')
        self.assertEqual(data, b'test2\n')

    def test_to_file(self):
        with temp_dir() as directory:
            filepath = os.path.join(directory, 'test')
            self.store.to_file('test1', filepath)
            with open(filepath, 'rb') as fh:
                written = fh.read()
            self.assertEqual(written, b'test2\n')

    @skip('not sure why this test was marked as skipped')
    def test_to_file_large(self):
        self.utils_large()
        with temp_dir() as directory:
            filepath = os.path.join(directory, 'test')
            self.store.to_file('test3', filepath)
            written = open(filepath).read()
            self.assertEqual(written, 'test4'*10000000)


class StoreWriteTestMixin(object):

    resolution = 'arbitrary'

    def test_set(self):

        data = create_file_like_data(b'test4')
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
        if self.resolution == 'second':
            test_end = int(test_end)+1
        self.assertEqual(self.store.to_bytes('test3'), b'test4')
        self.assertEqual(self.store.get_metadata('test3'), metadata)

        value = self.store.get('test3')
        self.assertEqual(value.size, 5)
        self.assertGreaterEqual(value.modified, test_start)
        self.assertLessEqual(value.modified, test_end)

    def test_set_copies(self):
        """ Test that set copies the provided metadata

        Subclasses should call this via super(), then validate that things
        were stored correctly.

        """
        data = create_file_like_data(b'test4')
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
        data = create_file_like_data(b'test4'*10000000) # 50 MB of data
        metadata = {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        }
        self.store.set('test3', (data, metadata))
        self.assertEqual(self.store.to_bytes('test3'), b'test4'*10000000)
        self.assertEqual(self.store.get_metadata('test3'), metadata)

    def test_set_buffer(self):
        """ Test that set works with a different size buffer

        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        data = create_file_like_data(b'test4'*8000)
        metadata = {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        }
        self.store.set('test3', (data, metadata), 8000)
        self.assertEqual(self.store.to_bytes('test3'), b'test4'*8000)
        self.assertEqual(self.store.get_metadata('test3'), metadata)

    def test_set_data(self):
        data = create_file_like_data(b'test4')
        test_start = time.time()
        if self.resolution == 'second':
            test_start = int(test_start)
        self.store.set_data('test1', data)
        test_end = time.time()
        if self.resolution == 'second':
            test_end = int(test_end)+1
        self.assertEqual(self.store.to_bytes('test1'), b'test4')
        value = self.store.get('test1')
        self.assertGreaterEqual(value.modified, test_start)
        self.assertLessEqual(value.modified, test_end)

    def test_set_data_new(self):
        data = create_file_like_data(b'test4')
        test_start = time.time()
        if self.resolution == 'second':
            test_start = int(test_start)
        self.store.set_data('test3', data)
        test_end = time.time()
        if self.resolution == 'second':
            test_end = int(test_end)+1
        self.assertEqual(self.store.to_bytes('test3'), b'test4')
        value = self.store.get('test3')
        self.assertGreaterEqual(value.modified, test_start)
        self.assertLessEqual(value.modified, test_end)

    def test_set_data_large(self):
        """ Test that set works with large (~50 MB) data

        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        data = create_file_like_data(b'test4'*10000000) # 50 MB of data
        self.store.set_data('test3', data)
        self.assertEqual(self.store.to_bytes('test3'), b'test4'*10000000)

    def test_set_data_buffer(self):
        """ Test that set works with a different-sized buffer

        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        data = create_file_like_data(b'test4'*8000)
        self.store.set_data('test1', data)
        self.assertEqual(self.store.to_bytes('test1'), b'test4'*8000)

    def test_set_metadata(self):
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
        if self.resolution == 'second':
            test_end = int(test_end)+1
        self.assertEqual(self.store.get_metadata('test1'), metadata)
        value = self.store.get('test1')
        self.assertGreaterEqual(value.modified, test_start)
        self.assertLessEqual(value.modified, test_end)

    def test_set_metadata_copies(self):
        """ Test that set_metadata copies the provided metadata

        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        metadata = {
            'a_str': 'test5',
            'an_int': 2,
            'a_float_1': 3.0,
            'a_bool_1': True,
            'a_list_1': ['one', 'two', 'three'],
            'a_dict_1': {'one': 1, 'two': 2, 'three': 3}
        }
        self.store.set_metadata('test1', metadata)
        metadata['extra_key'] = 'extra_value'
        self.assertNotEqual(self.store.get_metadata('test1'), metadata)

    def test_update_metadata(self):
        """ Test that update_metadata works

        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
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
        self.store.delete('test1')
        self.assertFalse(self.store.exists('test1'))

    def test_multiset(self):
        keys = ['set_key'+str(i) for i in range(10)]
        values = [b'set_value%i' % i for i in range(10)]
        datas = [create_file_like_data(value) for value in values]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        self.store.multiset(keys, zip(datas, metadatas))
        for i in range(10):
            self.assertTrue(self.store.exists(keys[i]))
            with self.store.get_data(keys[i]) as data_fh:
                self.assertEqual(data_fh.read(), values[i])
            self.assertEqual(self.store.get_metadata(keys[i]), metadatas[i])

    def test_multiset_overwrite(self):
        keys = ['existing_key'+str(i) for i in range(10)]
        values = [b'set_value%i' % i for i in range(10)]
        datas = [create_file_like_data(value) for value in values]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        self.store.multiset(keys, zip(datas, metadatas))
        for i in range(10):
            self.assertTrue(self.store.exists(keys[i]))
            with self.store.get_data(keys[i]) as data_fh:
                self.assertEqual(data_fh.read(), values[i])
            self.assertEqual(self.store.get_metadata(keys[i]), metadatas[i])

    def test_multiset_data(self):
        keys = ['existing_key'+str(i) for i in range(10)]
        values = [b'set_value%i' % i for i in range(10)]
        datas = [create_file_like_data(value) for value in values]
        self.store.multiset_data(keys, datas)
        metadatas = [{'meta': True, 'meta1': -i} for i in range(10)]
        for i in range(10):
            self.assertTrue(self.store.exists(keys[i]))
            with self.store.get_data(keys[i]) as data_fh:
                self.assertEqual(data_fh.read(), values[i])

    def test_multiset_metadata(self):
        keys = ['existing_key'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        self.store.multiset_metadata(keys, metadatas)
        values = ['existing_value'+str(i) for i in range(10)]
        for i in range(10):
            self.assertTrue(self.store.exists(keys[i]))
            self.assertEqual(self.store.get_metadata(keys[i]), metadatas[i])

    def test_multiupdate_metadata(self):
        keys = ['existing_key'+str(i) for i in range(10)]
        metadatas = [{'meta1': i, 'meta2': True} for i in range(10)]
        self.store.multiset_metadata(keys, metadatas)
        for i in range(10):
            self.assertTrue(self.store.exists(keys[i]))
            expected = {'meta': True}
            expected.update(metadatas[i])
            self.assertEqual(self.store.get_metadata(keys[i]), metadatas[i])

    def test_from_file(self):
        """ Test that from_file works

        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        with temp_dir() as directory:
            filepath = os.path.join(directory, 'test')
            with open(filepath, 'wb') as fp:
                fp.write(b'test4')
            self.store.from_file('test3', filepath)
        self.assertEqual(self.store.to_bytes('test3'), b'test4')

    def test_from_file_large(self):
        """ Test that from_file works for large files (~50 MB)

        Subclasses should call this via super(), then validate that things
        were stored correctly.
        """
        with temp_dir() as directory:
            filepath = os.path.join(directory, 'test')
            with open(filepath, 'wb') as fp:
                fp.write(b'test4'*10000000)
            self.store.from_file('test3', filepath)
        self.assertEqual(self.store.to_bytes('test3'), b'test4'*10000000)

    def test_from_bytes(self):
        self.store.from_bytes('test3', b'test4')
        self.assertEqual(self.store.to_bytes('test3'), b'test4')
