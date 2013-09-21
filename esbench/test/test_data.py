# -*- coding: UTF-8 -*-

import datetime
import os.path
import unittest
import json

from .. import data


class DataTest(unittest.TestCase):

    def test_aa(self):
        l = list(data._aa())
        self.assertEqual(len(l), 676)
        self.assertEqual(l[0], 'aa')
        self.assertEqual(l[-1], 'zz')
        l = list(data._aa(10))
        self.assertEqual(len(l), 10)
        self.assertEqual(l[0], 'aa')
        self.assertEqual(l[-1], 'aj')


    def test_urls(self):
        url = data.urls().next()
        self.assertEqual(url, "https://s3-us-west-1.amazonaws.com/esbench/appl_aa.gz")
    
    
    def test_feed(self):
        line = data.feed().next()
        self.assertEqual(u'2009-01-01', json.loads(line)['_meta']['date_published'])

        
if __name__ == "__main__":
    unittest.main()     

