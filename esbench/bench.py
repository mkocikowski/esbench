import urlparse
import httplib
import contextlib
import itertools
import logging
import argparse
import sys

import data

__version__ = "0.0.1"

logger = logging.getLogger(__name__)

conn = None
DEFAULT_TIMEOUT = 180

@contextlib.contextmanager
def connect(host='localhost', port=9200, timeout=DEFAULT_TIMEOUT): 
    conn = httplib.HTTPConnection(host=host, port=port, timeout=timeout)
    yield conn
    conn.close()


def get(conn, path):
    conn.request('GET', path, body=None)
    resp = conn.getresponse()
    data = resp.read()
    return resp.status, resp.reason, data


def put(conn, path, data):
    head = {'Content-type': 'application/json'}
    conn.request('PUT', path, data, head)
    resp = conn.getresponse()
    data = resp.read()
    return resp.status, resp.reason, data


def post(conn, path, data):
    head = {'Content-type': 'application/json'}
    conn.request('POST', path, data, head)
    resp = conn.getresponse()
    data = resp.read()
    return resp.status, resp.reason, data


def delete(conn, path, data=None):
    conn.request('DELETE', path)
    resp = conn.getresponse()
    data = resp.read()
    return resp.status, resp.reason, data


def index_delete(conn): 
    index = 'test'
    curl = """curl -XDELETE 'http://localhost:9200/%s/'""" % (index, )
    status, reason, data = delete(conn, index)
    return (status, reason, curl)
    

def index_create(conn, mapping=""): 
    index = 'test'
    data = """{"settings": {"index": {"number_of_replicas": 0, "number_of_shards": 1}}}"""
    curl = """curl 'http://localhost:9200/%s/' -d '%s'""" % (index, data)
    status, reason, data = put(conn, index, data)
    return (status, reason, curl)


def document_post(conn, docid, data): 
    path = 'test/doc/%s' % docid
    curl = """curl 'http://localhost:9200/%s' -d '%s'""" % (path, data)
    status, reason, data = put(conn, path, data)
    return (status, reason, curl)


def args_parser():
    parser = argparse.ArgumentParser(description="esbench runner.")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--period', type=int, default=50000, help='run tests every n records; (%(default)i)')
    parser.add_argument('n', nargs="?", type=int, default=10, help='number of documents; (%(default)i)')
    return parser


def pager(iterable, page_size=None, fillvalue=(None, None)):
    # see 'grouper' in http://docs.python.org/2/library/itertools.html#recipes
    args = [enumerate(iterable)] * page_size
    return itertools.izip_longest(fillvalue=fillvalue, *args)


VERBOSE = False
def echo(s): 
    if not VERBOSE:
        return
    print(s)
    

def stats(pn):
    logger.info("page: %i" % pn) 


def main():

    logging.basicConfig(level=logging.INFO)
    args = args_parser().parse_args()
    
    global VERBOSE
    VERBOSE = args.verbose

    with connect() as conn: 
        curl = index_delete(conn)[2]
        echo(curl)
        curl = index_create(conn)[2]
        echo(curl)
        
#         for n, line in enumerate(data.feed()):
#             status, reason, curl = document_post(conn, n, line)
#             print(curl)
#             if status != 201:
#                 sys.stderr.write("%s\n" % (curl, ))
#                 break
#             if n == (args.n-1):
#                 break

        lines = itertools.islice(data.feed(), args.n)
        for page_no, page in enumerate(pager(lines, page_size=args.period)):

            for n, line in page:
                if n is None:
                    break
                status, reason, curl = document_post(conn, n, line)
                echo(curl)
                if status != 201:
                    logger.error("%s\n" % (curl, ))
                    break

            stats(page_no)


if __name__ == "__main__":
    main()

