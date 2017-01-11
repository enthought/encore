from unittest import TestCase
from ..dynamic_url_store import DynamicURLStore


class DynamicURLStoreTest(TestCase):
    ''' TODO: This should be a full test suite. '''

    def setUp(self):
        self.store = DynamicURLStore('http://localhost',
                                     None,
                                     parts={'data': 'd',
                                            'metadata': 'm',
                                            'permissions': 'p'})

    def test_parts(self):
        url = self.store._url('key', 'data')
        self.assertEqual(url, 'http://localhost/key/d')
