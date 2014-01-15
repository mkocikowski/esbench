# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import datetime
import os.path
import unittest
import json
import logging

import esbench.data

class DataTest(unittest.TestCase):

    def test_aa(self):

        l = list(esbench.data._aa())
        self.assertEqual(len(l), 676)
        self.assertEqual(l[0], 'aa')
        self.assertEqual(l[-1], 'zz')
        l = list(esbench.data._aa(10))
        self.assertEqual(len(l), 10)
        self.assertEqual(l[0], 'aa')
        self.assertEqual(l[-1], 'aj')


    def test_urls(self):

        urls = list(esbench.data.urls(url_template=esbench.data.URL_TEMPLATE))
        self.assertEqual(urls[0], "https://s3-us-west-1.amazonaws.com/esbench/appl_2005_aa.gz")
        self.assertEqual(urls[75], "https://s3-us-west-1.amazonaws.com/esbench/appl_2006_aa.gz")
        self.assertEqual(len(urls), 8*75)


    def test_download(self):

        resp = esbench.data.download("foo.com/bar")
        self.assertIsNone(resp)
#         self.assertRaises(ValueError, esbench.data.download, "foobar")


    def test_get_data(self):

        line = esbench.data.get_data().next()
        self.assertEqual(u'2005-01-06', json.loads(line)['dates']['date_published'])

        # make sure that we can skip over nonexistent urls
        line = esbench.data.get_data(urls_f=lambda x: ["http://foo.bar/baz", "https://s3-us-west-1.amazonaws.com/esbench/appl_2005_aa.gz"]).next()
        self.assertEqual(u'2005-01-06', json.loads(line)['dates']['date_published'])


    def test_get_feed(self):

        with esbench.data.feed() as f:
            line = f.next()
            self.assertEqual(u'2005-01-06', json.loads(line)['dates']['date_published'])

        with esbench.data.feed(lines_i=iter([1,2,3])) as f:
            line = f.next()
            self.assertEqual(1, line)

        with open("/tmp/esbench_test_get_feed", "w") as f: f.write('foo\nbar\nbaz\n')
        with esbench.data.feed(path="/tmp/esbench_test_get_feed") as f:
            line = f.next().strip()
            self.assertEqual('foo', line)


    def test_batch_iterator(self):

        lines = ("line %i" % i for i in range(100))

        batch = esbench.data.batch_iterator(lines=lines, max_batch_n=10, max_batch_byte_size=0)
        batch_l = list(batch)
        self.assertEqual(10, len(batch_l))

        batch = esbench.data.batch_iterator(lines=lines, max_batch_n=0, max_batch_byte_size=40)
        batch_l = list(batch)
        self.assertEqual(6, len(batch_l))
        self.assertEqual(42, sum([len(l) for l in batch_l]))


    def test_batches_iterator(self):

        lines = ("line %i" % i for i in range(100))

        batches = list(esbench.data.batches_iterator(lines, batch_count=10, max_n=50, max_byte_size=0))
        self.assertEqual(10, len(batches))
        for batch in batches:
            lines = list(batch)
            self.assertEqual(5, len(lines))



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

