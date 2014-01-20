# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

"""Use esbench.api.connect() to get a context manager for use with api calls.
"""


import httplib
import contextlib
import logging
import json
import collections
import re

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10


ApiResponse = collections.namedtuple(
    'ApiResponse', ['status', 'reason', 'data', 'curl']
)


def retry_and_reconnect_on_IOError(method):
    def wrapper(self, *args, **kwargs):
        for i in [1, 2, 5, 10, 25, 50, 75, 100]:
            try:
                if not self.conn:
                    logger.debug("opening %s with timeout: %.2fs...", self.conn_cls, self.timeout*i)
                    self.connect(timeout=self.timeout*i)
                res = method(self, *args, **kwargs)
                # connections with really long timeouts should not be kept
                # around, they are an exceptional thing
                if i >= 25:
                    self.close()
                return res
            except IOError as (exc):
                logger.debug("%s (%s) in retry_and_reconnect_on_IOError (timeout: %.2fs) try: %i, pause: %is", type(exc), exc, self.timeout*i, i, 1, exc_info=False)
                self.close()
        raise # re-raises the last exception, so most likely IOError
    return wrapper


def _massage_request_path(path):
    if not path:
        return "/"
    return "/%s" % re.search(r"^/*(.*)$", path).group(1)

class Conn(object):

    def __init__(self, host='localhost', port=9200, timeout=DEFAULT_TIMEOUT, conn_cls=httplib.HTTPConnection):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.conn_cls = conn_cls
        self.conn = None

    def connect(self, timeout=DEFAULT_TIMEOUT):
        """Use context manager from api.connect() instead."""
        self.conn = self.conn_cls(host=self.host, port=self.port, timeout=timeout)
        self.conn.connect()

    def close(self):
        if self.conn: self.conn.close()
        self.conn = None


    @retry_and_reconnect_on_IOError
    def get(self, path, data=None):
        path = _massage_request_path(path)
        method = 'GET'
        if data:
            curl = "curl -X%s http://%s:%i%s -d '%s'" % (method, self.host, self.port, path, data)
            head = {'Content-type': 'application/json'}
        else:
            curl = "curl -X%s http://%s:%i%s" % (method, self.host, self.port, path)
            head = {}
        self.conn.request(method, path, data, head)
        resp = self.conn.getresponse()
        r_data = resp.read()

        if resp.status not in [200, 201]:
            logger.debug((resp.status, path, curl[:50]))

        return ApiResponse(resp.status, resp.reason, r_data, curl)


    @retry_and_reconnect_on_IOError
    def put(self, path, data):
        if not data:
            raise ValueError('data must not evaluate to false')
        path = _massage_request_path(path)
        method = 'PUT'
        curl = "curl -X%s http://%s:%i%s -d '%s'" % (method, self.host, self.port, path, data)
        head = {'Content-type': 'application/json'}
        self.conn.request(method, path, data, head)
        resp = self.conn.getresponse()
        r_data = resp.read()

        if resp.status == 413:
            logger.debug((resp.status, path, len(data)))
        elif resp.status >= 400:
            logger.debug((resp.status, path, r_data))
        elif resp.status not in [200, 201]:
            logger.debug((resp.status, path, curl[:50]))

        return ApiResponse(resp.status, resp.reason, r_data, curl)


    @retry_and_reconnect_on_IOError
    def post(self, path, data):
        path = _massage_request_path(path)
        method = 'POST'
        if data:
            curl = "curl -X%s http://%s:%i%s -d '%s'" % (method, self.host, self.port, path, data)
            head = {'Content-type': 'application/json'}
        else:
            curl = "curl -X%s http://%s:%i%s" % (method, self.host, self.port, path)
            head = {}
        self.conn.request(method, path, data, head)
        resp = self.conn.getresponse()
        r_data = resp.read()

        if resp.status == 413:
            logger.debug((resp.status, path, len(data)))
        elif resp.status >= 400:
            logger.debug((resp.status, path, r_data))
        elif resp.status not in [200, 201]:
            logger.debug((resp.status, path, curl[:50]))

        return ApiResponse(resp.status, resp.reason, r_data, curl)


    @retry_and_reconnect_on_IOError
    def delete(self, path):
        path = _massage_request_path(path)
        method = 'DELETE'
        curl = "curl -X%s http://%s:%i%s" % (method, self.host, self.port, path)
        self.conn.request(method, path)
        resp = self.conn.getresponse()
        data = resp.read()

        if resp.status not in [200, 201]:
            logger.debug((resp.status, path, curl[:50]))

        return ApiResponse(resp.status, resp.reason, data, curl)


@contextlib.contextmanager
def connect(host='localhost', port=9200, timeout=DEFAULT_TIMEOUT, conn_cls=httplib.HTTPConnection):
    conn = Conn(host=host, port=port, timeout=timeout, conn_cls=conn_cls)
    yield conn
    conn.close()



# def document_put(conn, index, doctype, docid, data):
#     path = '%s/%s/%s' % (index, doctype, docid)
#     resp = conn.put(path, data)
#     return resp


def document_post(conn, index, doctype, data):
    path = '%s/%s' % (index, doctype)
    resp = conn.post(path, data)
    return resp


def index_create(conn, index, config=None):
    data = json.dumps(config)
    resp = conn.put(index, data)
    return resp


def index_delete(conn, index):
    resp = conn.delete(index)
    return resp


def index_get_stats(conn, index, groups):
    path = "%s/_stats?clear=true&docs=true&store=true&search=true&merge=true&indexing=true&fielddata=true&fields=*&groups=%s" % (index, groups)
    resp = conn.get(path)
    return resp


def index_set_refresh_interval(conn, index, ri):
    path = "%s/_settings" % (index, )
    data = '{"index": {"refresh_interval": "%s"}}' % ri
    resp = conn.put(path, data)
    return resp


def index_optimize(conn, index, nseg=0):
    if nseg:
        path = "%s/_optimize?max_num_segments=%i&refresh=true&flush=true&wait_for_merge=true" % (index, nseg)
    else:
        path = "%s/_optimize?refresh=true&flush=true&wait_for_merge=true" % (index, )
    resp = conn.post(path, None)
    return resp


def index_get_segments(conn, index):
    path = "%s/_segments" % (index, )
    resp = conn.get(path)
    return resp


def cluster_get_info(conn):
    path = "_cluster/nodes?settings=true&os=true&process=true&jvm=true&thread_pool=true&network=true&transport=true&http=true&plugin=true"
    resp = conn.get(path)
    return resp


def cluster_get_stats(conn):
    path = "_cluster/nodes/stats?indices=true&os=true&process=true&jvm=true&network=true&transport=true&http=true&fs=true&thread_pool=true"
    resp = conn.get(path)
    return resp


def cluster_get_fielddata_stats(conn):
    path = "_nodes/stats/indices/fielddata/*"
    resp = conn.get(path)
    return resp
