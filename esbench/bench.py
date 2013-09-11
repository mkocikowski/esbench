import urlparse
import httplib
import contextlib
import itertools
import logging
import argparse
import sys
import json
import time
import random
import traceback
import datetime

import data as DATA

__version__ = "0.0.2"

logger = logging.getLogger(__name__)

conn = None
DEFAULT_TIMEOUT = 10


def retry_and_reconnect_on_IOError(method): 
    def wrapper(self, *args, **kwargs): 
        for i in [1, 2, 5, 10, 25]:
            try: 
                if not self._conn:
                    self.connect(timeout=self.timeout*i)
                res = method(self, *args, **kwargs)
                # connections with really long timeouts should not be kept
                # around, they are an exceptional thing
                if i >= 5: 
                    self.close()
                return res
            except IOError as (exc):
                logger.warning("%s (%s) in retry_and_reconnect_on_IOError try: "
                            "%i, pause: %is" % (type(exc), exc, i, 1))
                self.close()
        raise # re-raises the last exception, so most likely IOError
    return wrapper


class Conn(object): 
    
    def __init__(self, host='localhost', port=9200, timeout=DEFAULT_TIMEOUT): 
        self.host = host
        self.port = port
        self.timeout = timeout
        self._conn = None
    
    def connect(self, timeout=DEFAULT_TIMEOUT): 
        logger.debug("Attempting HTTPConnection (%s:%i) with timeout: %s" % (self.host, self.port, timeout))
        self._conn = httplib.HTTPConnection(host=self.host, port=self.port, timeout=timeout)
        self._conn.connect()
    
    def close(self): 
        self._conn.close()
        self._conn = None
    
    @retry_and_reconnect_on_IOError
    def get(self, path):
        self._conn.request('GET', path, body=None)
        resp = self._conn.getresponse()
        data = resp.read()
        return resp.status, resp.reason, data
    
    @retry_and_reconnect_on_IOError
    def put(self, path, data):
        head = {'Content-type': 'application/json'}
        self._conn.request('PUT', path, data, head)
        resp = self._conn.getresponse()
        data = resp.read()
        return resp.status, resp.reason, data

    @retry_and_reconnect_on_IOError
    def post(self, path, data):
        head = {'Content-type': 'application/json'}
        self._conn.request('POST', path, data, head)
        resp = self._conn.getresponse()
        data = resp.read()
        return resp.status, resp.reason, data

    @retry_and_reconnect_on_IOError
    def delete(self, path, data=None):
        self._conn.request('DELETE', path)
        resp = self._conn.getresponse()
        data = resp.read()
        return resp.status, resp.reason, data


@contextlib.contextmanager
def connect(host='localhost', port=9200, timeout=DEFAULT_TIMEOUT): 
    conn = Conn(host=host, port=port, timeout=DEFAULT_TIMEOUT)
    yield conn
    conn.close()


def index_delete(conn, index): 
    curl = """curl -XDELETE 'http://localhost:9200/%s/'""" % (index, )
    status, reason, data = conn.delete(index)
    return (status, reason, curl)
    

def index_create(conn, index, mapping=""): 
    data = """{"settings": {"index": {"number_of_replicas": 0, "number_of_shards": 1}}}"""
    curl = """curl 'http://localhost:9200/%s/' -d '%s'""" % (index, data)
    status, reason, data = conn.put(index, data)
    return (status, reason, curl)


def index_stats(conn, index): 
    path = '%s/_stats' % index
    curl = """curl -XGET 'http://localhost:9200/%s'""" % (path, )
    status, reason, data = conn.get(path)
    return (status, reason, data, curl)


def document_post(conn, index, doctype, docid, data): 
    path = '%s/%s/%s' % (index, doctype, docid)
    curl = """curl -XPUT 'http://localhost:9200/%s' -d '%s'""" % (path, data)
    status, reason, data = conn.put(path, data)
    return (status, reason, curl)


def index_set_refresh_interval(conn, index, ri): 
    path = "%s/_settings" % (index, )
    data = '{"index": {"refresh_interval": "%s"}}' % ri
    curl = "curl -XPUT localhost:9200/%s -d '%s'" % (path, data)
    status, reason, data = conn.put(path, data)
    logger.info("set refresh interval on index %s to %s" % (index, ri))


@contextlib.contextmanager
def refresh_interval(conn, index, ri, default="1s"): 
    try: 
        index_set_refresh_interval(conn, index, ri)
        yield
    finally:
        index_set_refresh_interval(conn, index, default)


def index_optimize(conn, index): 

    logger.info("optimizing...")
    t1 = time.time()
    if SEGMENTS: 
        path = "%s/_optimize?max_num_segments=%i&refresh=true&flush=true&wait_for_merge=true" % (index, SEGMENTS)
    else:
        path = "%s/_optimize?refresh=true&flush=true&wait_for_merge=true" % (index, )
    curl = "curl -XPOST localhost:9200/%s" % (path, )
    status, reason, data = conn.post(path, None)
    td = time.time() - t1
    segments = index_segments(conn, index)
    logger.info("done optimizing: %.2fs, total of %i/%i segments." % (
        td, 
        segments['num_search_segments'], 
        segments['num_committed_segments'], 
    ))
    return td
    

def index_segments(conn, index): 
    path = "%s/_segments" % (index, )
    curl = "curl -XGET http://localhost:9200/test/_segments" 
    status, reason, data = conn.get(path)
    data = json.loads(data)
    segments = { 
        "num_search_segments": data['indices'][index]['shards']['0'][0]['num_search_segments'], 
        "num_committed_segments": data['indices'][index]['shards']['0'][0]['num_committed_segments'], 
    }
    return segments
    



# def query_mlt(conn, docid): 
#     path = 'test/doc/0/_mlt'
#     curl = """curl -XGET 'http://localhost:9200/%s'""" % (path, )
#     status, reason, data = get(conn, path)
#     return (status, reason, data, curl)
#     

def timestamp():
    DEFAULT_DATETIME_FORMAT = r'%Y-%m-%dT%H:%M:%SZ'
    DEFAULT_DATETIME_FORMAT_WITH_MICROSECONDS = r'%Y-%m-%dT%H:%M:%S.%fZ'
    dt = datetime.datetime.utcnow()
    s = dt.strftime(DEFAULT_DATETIME_FORMAT_WITH_MICROSECONDS)
    return s
    

class Stat(object):

    _count = 0

    def __init__(self, index, doctype, queries, cli_args): 
        Stat._count += 1
        self.statid = "%03i" % Stat._count
        self.index = index
        self.doctype = doctype
        self.queries = queries
        self.cli_args = cli_args


    def _sgn(self, query_name): 
        return "%s_%s" % (self.statid, query_name)        


    def run(self, conn): 
    
        self.t_optimize = index_optimize(conn, self.index)
    
        for name, query in self.queries.items(): 
            query['stats'] = [self._sgn(name)]
            qs = json.dumps(query)
            qs = qs % {'variable': random.randint(0, 100000)}
            path = '%s/%s/_search' % (self.index, self.doctype)
            curl = """curl -XPOST 'http://localhost:9200/%s' -d '%s'""" % (path, qs)
            for n in range(1000): 
                status, reason, data = conn.post(path, qs)

    
    def record(self, conn): 

        groups = [self._sgn(q) for q in self.queries]
        path = "%s/_stats?clear=true&docs=true&store=true&search=true&merge=true&indexing=true&groups=%s" %\
            (self.index, ",".join(groups))
        curl = """curl -XGET 'http://localhost:9200/%s'""" % (path, )
        status, reason, data = conn.get(path)
        
        # build the stat record
        #
        data = json.loads(data)
        stat = {
            'meta': {
                'statid': int(self.statid), 
                'ts_bench': BENCH_TS, 
                'ts_stat': timestamp(), 
                'cli_args': self.cli_args, 
            },
            'docs': data['indices'][self.index]['primaries']['docs'], 
            'search': {k.split("_")[1]: v for 
                k, v in data['indices'][self.index]['primaries']['search']['groups'].items() if 
                k.startswith(self.statid)}, 
            'store': data['indices'][self.index]['primaries']['store'], 
            'indexing': data['indices'][self.index]['primaries']['indexing'], 
            'merges': {k: v for 
                k, v in data['indices'][self.index]['primaries']['merges'].items()
                if not k.startswith('current')}, 
            'segments': index_segments(conn, self.index), 
        }
        stat['segments']['t_optimize'] = "%.2fs" % self.t_optimize
        stat['segments']['t_optimize_in_millis'] = int(self.t_optimize * 1000)

        print(json.dumps(stat, indent=4, sort_keys=True))

        # put the stat record into the stats index
        #
        path = 'stats/doc/%i' % int(self.statid)
        data = json.dumps(stat)
        curl = """curl -XPUT 'http://localhost:9200/%s' -d '%s'""" % (path, data)
        status, reason, data = conn.put(path, data)
        return (status, reason, curl)
        

def args_parser():
    parser = argparse.ArgumentParser(description="esbench runner.")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('-r', '--results', action='store_true', help='do not run, analyze existing results; (%(default)s)')
    parser.add_argument('--period', type=int, default=10, help='run tests every n records; (%(default)i)')
    parser.add_argument('--segments', type=int, metavar='N', default=None, help='max_num_segments for optimize calls; (%(default)s)')
    parser.add_argument('--refresh', type=str, metavar='T', default='1s', help="'refresh_interval' for the index, '-1' for none; (%(default)s)")
    parser.add_argument('n', nargs="?", type=int, default=100, help='number of documents; (%(default)i)')
    return parser


VERBOSE = False
def echo(s): 
    if not VERBOSE:
        return
    print(s)
    
with open('queries.json', 'rU') as f:
    s = f.read()
QUERIES = json.loads(s)

SEGMENTS = None

BENCH_TS = timestamp()

def stats(conn, cli_args):
    stat = Stat('test', 'doc', QUERIES, cli_args)
    stat.run(conn)
    stat.record(conn)
    

def results(conn): 
    status, reason, data = conn.get("stats/doc/_count")
    count = json.loads(data)['count']
    for i in range(1, count+1): 
        status, reason, data = conn.get("stats/doc/%i" % i)
        yield json.loads(data)


def main():

    logging.basicConfig(level=logging.DEBUG)
    args = args_parser().parse_args()
    
    global VERBOSE
    VERBOSE = args.verbose

    global SEGMENTS
    SEGMENTS = args.segments

    with connect() as conn: 
    
        if args.results:
            r = [(d['_source']['docs']['count'],
                  d['_source']['search']['mlt']['query_time_in_millis'], 
                  d['_source']['search']['match']['query_time_in_millis'], 
                  d['_source']['segments']['num_search_segments'], 
                  d['_source']['segments']['t_optimize_in_millis'] / 1000.0)
                  for d in results(conn)]
            print("%8s %7s %7s %4s %12s" % ('COUNT', 'MLT', 'MATCH', 'SEG', 'OPTIMIZE'))
            for t in r:
                print("%8i %7i %7i %4i %9.2f" % t)


        else: 
            
            lines = itertools.islice(DATA.feed(), args.n)

            for index in ['stats', 'test']: 
                curl = index_delete(conn, index)[2]; echo(curl)
                curl = index_create(conn, index)[2]; echo(curl)
        
            with refresh_interval(conn, 'test', args.refresh): 
                c = 0
                for n, line in enumerate(lines): 
                    status, reason, curl = document_post(conn, 'test', 'doc', n, line)
                    echo(curl)
                    if status not in (200, 201):
                        logger.error("%s (%s) %s\n" % (status, reason, curl, ))
                        break
                    c += 1
                    if c == args.period: 
                        stats(conn, dict(args.__dict__))
                        c = 0



if __name__ == "__main__":
    main()

