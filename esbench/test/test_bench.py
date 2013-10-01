# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import datetime
import os.path
import unittest
import json

import esbench.bench
import esbench.api
import esbench.test.test_api

class BenchTest(unittest.TestCase):

    def test_rands(self):
        s = esbench.bench.rands()
        self.assertIsInstance(s, str)
        self.assertEqual(len(s), 6)
        self.assertNotEqual(esbench.bench.rands(), esbench.bench.rands())
    


class SearchQueryTest(unittest.TestCase):

    def test_execute(self):
    
        with self.assertRaises(ValueError): 
            q = esbench.bench.SearchQuery(
                    name='match', 
                    query=json.dumps({'match': {'foo': 'bar'}}), 
                    observation_id='ABCDEFGH', 
                    index='test', 
                    doctype='doc'
            )

        q = esbench.bench.SearchQuery(
                name='match', 
                query={'match': {'foo': 'bar'}}, 
                observation_id='ABCDEFGH', 
                index='test', 
                doctype='doc'
        )
        c = esbench.api.Conn(conn_cls=esbench.test.test_api.MockHTTPConnection)
        resp = q.execute(c)
        self.assertEqual(resp.curl, """curl -XPOST http://localhost:9200/test/doc/_search -d \'{"match": {"foo": "bar"}, "stats": ["ABCDEFGH_match"]}\'""")


class BenchmarkTest(unittest.TestCase):
    pass
        

        
if __name__ == "__main__":
    unittest.main()     

