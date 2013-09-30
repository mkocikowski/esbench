# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import datetime
import os.path
import unittest
import json
import httplib
import logging

import esbench.api

class MockHTTPResponse(object):

    def __init__(self, req=None): 
        self.method, self.url, self.body = req if req else (None, None, None)
        self.status = 200
        self.reason = 'no reason'
        try: 
            d = json.loads(self.body)
            self.status = d.get('status', 200)
            self.reason = d.get('reason', 'no reason')
        except (TypeError, ValueError):
            pass
    
    def read(self): 
        return self.body
    

class MockHTTPConnection(object):

    def __init__(self, host='localhost', port=9200, timeout=10): 
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = None
        self.req = None
    
    def connect(self):
        self.sock = True
    
    def close(self):
        self.sock = None
    
    def request(self, method, url, body=None, headers={}): 
        self.req = (method, url, body)
        return
    
    def getresponse(self):
        return MockHTTPResponse(self.req)
    

class ApiConnTest(unittest.TestCase):

    def test_conn(self):
        c = esbench.api.Conn()
        self.assertEqual(c.conn_cls, httplib.HTTPConnection)
        c = esbench.api.Conn(conn_cls=MockHTTPConnection)
        self.assertEqual(c.__dict__, {'conn': None, 'host': 'localhost', 'port': 9200, 'timeout': 10, 'conn_cls': MockHTTPConnection})
        c.connect()
        self.assertIs(c.conn.sock, True)                    
        c.close()
        self.assertIsNone(c.conn)

    def test_conn_get(self):
        c = esbench.api.Conn(conn_cls=MockHTTPConnection)
        resp = c.get("test/_stats")
        self.assertIsInstance(resp, esbench.api.ApiResponse)
        self.assertEqual(resp.curl, "curl -XGET http://localhost:9200/test/_stats")
    

class ApiFuncTest(unittest.TestCase):

    def test_document_post(self):
        c = esbench.api.Conn(conn_cls=MockHTTPConnection)
        resp = esbench.api.document_post(c, 'i1', 'd1', 'foo')
        self.assertEqual(resp.curl, "curl -XPOST http://localhost:9200/i1/d1 -d 'foo'")

    def test_index_create(self):
        c = esbench.api.Conn(conn_cls=MockHTTPConnection)
        resp = esbench.api.index_create(c, 'i1', config={'mapping': 'foo'})
        self.assertEqual(resp.curl, """curl -XPUT http://localhost:9200/i1 -d \'{"mapping": "foo"}\'""") 
# 
# def index_create(conn, index, config=None): 
#     data = json.dumps(config)
#     resp = conn.put(index, data)
#     return resp



        
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()     

