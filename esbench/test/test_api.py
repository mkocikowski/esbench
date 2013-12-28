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
    """Mock httplib.HTTPConnection"""

    def __init__(self, host='localhost', port=9200, timeout=10):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = None
        self.req = None
        self.requests = []
        self.responses = []

    def connect(self):
        self.sock = True

    def close(self):
        self.sock = None

    def request(self, method, url, body=None, headers=None):
        self.req = (method, url, body)
        self.requests.append(self.req)
        return

    def getresponse(self):
        resp = MockHTTPResponse(self.req)
        self.responses.append(resp)
        return resp


class ApiConnTest(unittest.TestCase):

    def test_conn(self):
        self.assertEqual(esbench.api.Conn().conn_cls, httplib.HTTPConnection)
        c = esbench.api.Conn(conn_cls=MockHTTPConnection)
        self.assertEqual(c.__dict__, {'conn': None, 'host': 'localhost', 'port': 9200, 'timeout': 10, 'conn_cls': MockHTTPConnection})
        c.connect()
        self.assertIs(c.conn.sock, True)
        c.close()
        self.assertIsNone(c.conn)

    def test_connect(self):
        with esbench.api.connect(conn_cls=MockHTTPConnection) as c:
            resp = c.get("foo/bar")
            self.assertEqual(resp.status, 200)
            self.assertIs(c.conn.sock, True)
        self.assertIsNone(c.conn)

    def test_req_resp(self):
        # test request and response recording
        c = esbench.api.Conn(conn_cls=MockHTTPConnection)
        resp = c.get("foo/bar")
        self.assertEqual(resp.status, 200)
        resp = c.get("foo/bar", json.dumps({'status': 404, 'reason': 'not found'}))
        self.assertEqual(resp.status, 404)
        self.assertEqual(resp.reason, 'not found')
        resp = c.delete("foo/bar")
        self.assertEqual(c.conn.requests, [('GET', 'foo/bar', None), ('GET', 'foo/bar', '{"status": 404, "reason": "not found"}'), ('DELETE', 'foo/bar', None)])
        self.assertEqual([r.status for r in c.conn.responses], [200, 404, 200])

    def test_conn_get(self):
        c = esbench.api.Conn(conn_cls=MockHTTPConnection)
        resp = c.get("test/_stats")
        self.assertIsInstance(resp, esbench.api.ApiResponse)
        self.assertEqual(resp.curl, "curl -XGET http://localhost:9200/test/_stats")
        resp = c.get("foo/bar", 'baz')
        self.assertEqual(resp.curl, "curl -XGET http://localhost:9200/foo/bar -d 'baz'")

    def test_conn_put(self):
        c = esbench.api.Conn(conn_cls=MockHTTPConnection)
        self.assertRaises(TypeError, c.put, "foo/bar")
        self.assertRaises(ValueError, c.put, "foo/bar", "")
        resp = c.put("foo/bar", "baz")
        self.assertEqual(resp.curl, "curl -XPUT http://localhost:9200/foo/bar -d 'baz'")

    def test_conn_post(self):
        c = esbench.api.Conn(conn_cls=MockHTTPConnection)
        # if 'data' is none, it must be explicitly set so
        self.assertRaises(TypeError, c.post, "foo/bar")
        resp = c.post("foo/bar", None)
        self.assertEqual(resp.curl, "curl -XPOST http://localhost:9200/foo/bar")
        resp = c.post("foo/bar", 'baz')
        self.assertEqual(resp.curl, "curl -XPOST http://localhost:9200/foo/bar -d 'baz'")

    def test_conn_delete(self):
        c = esbench.api.Conn(conn_cls=MockHTTPConnection)
        resp = c.delete("foo/bar")
        self.assertEqual(resp.curl, "curl -XDELETE http://localhost:9200/foo/bar")



class ApiFuncTest(unittest.TestCase):

    def setUp(self):
        self.c = esbench.api.Conn(conn_cls=MockHTTPConnection)

    def test_document_post(self):
        resp = esbench.api.document_post(self.c, 'i1', 'd1', 'foo')
        self.assertEqual(resp.curl, "curl -XPOST http://localhost:9200/i1/d1 -d 'foo'")

    def test_index_create(self):
        resp = esbench.api.index_create(self.c, 'i1', config={'mapping': 'foo'})
        self.assertEqual(resp.curl, """curl -XPUT http://localhost:9200/i1 -d \'{"mapping": "foo"}\'""")

    def test_index_delete(self):
        resp = esbench.api.index_delete(self.c, 'i1')
        self.assertEqual(resp.curl, """curl -XDELETE http://localhost:9200/i1""")

    def test_index_get_stats(self):
        resp = esbench.api.index_get_stats(self.c, 'i1', '123_mlt,123_match')
        self.assertEqual(resp.curl, """curl -XGET http://localhost:9200/i1/_stats?clear=true&docs=true&store=true&search=true&merge=true&indexing=true&fielddata=true&fields=*&groups=123_mlt,123_match""")

    def test_index_set_refresh_interval(self):
        resp = esbench.api.index_set_refresh_interval(self.c, 'i1', '5s')
        self.assertEqual(resp.curl, """curl -XPUT http://localhost:9200/i1/_settings -d \'{"index": {"refresh_interval": "5s"}}\'""")

    def test_index_optimize(self):
        resp = esbench.api.index_optimize(self.c, 'i1')
        self.assertEqual(resp.curl, """curl -XPOST http://localhost:9200/i1/_optimize?refresh=true&flush=true&wait_for_merge=true""")
        resp = esbench.api.index_optimize(self.c, 'i1', nseg=10)
        self.assertEqual(resp.curl, """curl -XPOST http://localhost:9200/i1/_optimize?max_num_segments=10&refresh=true&flush=true&wait_for_merge=true""")

    def test_index_get_segments(self):
        resp = esbench.api.index_get_segments(self.c, 'i1')
        self.assertEqual(resp.curl, "curl -XGET http://localhost:9200/i1/_segments")

    def test_cluster_get_info(self):
        resp = esbench.api.cluster_get_info(self.c)
        self.assertEqual(resp.curl, "curl -XGET http://localhost:9200/_cluster/nodes?settings=true&os=true&process=true&jvm=true&thread_pool=true&network=true&transport=true&http=true&plugin=true")

    def test_cluster_get_stats(self):
        resp = esbench.api.cluster_get_stats(self.c)
        self.assertEqual(resp.curl, "curl -XGET http://localhost:9200/_cluster/nodes/stats?indices=true&os=true&process=true&jvm=true&network=true&transport=true&http=true&fs=true&thread_pool=true")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

