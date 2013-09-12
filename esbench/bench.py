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
        return (resp.status, resp.reason, data)
    
    @retry_and_reconnect_on_IOError
    def put(self, path, data):
        head = {'Content-type': 'application/json'}
        self._conn.request('PUT', path, data, head)
        resp = self._conn.getresponse()
        data = resp.read()
        return (resp.status, resp.reason, data)

    @retry_and_reconnect_on_IOError
    def post(self, path, data):
        head = {'Content-type': 'application/json'}
        self._conn.request('POST', path, data, head)
        resp = self._conn.getresponse()
        data = resp.read()
        return (resp.status, resp.reason, data)

    @retry_and_reconnect_on_IOError
    def delete(self, path, data=None):
        self._conn.request('DELETE', path)
        resp = self._conn.getresponse()
        data = resp.read()
        return (resp.status, resp.reason, data)


@contextlib.contextmanager
def connect(host='localhost', port=9200, timeout=DEFAULT_TIMEOUT): 
    conn = Conn(host=host, port=port, timeout=DEFAULT_TIMEOUT)
    yield conn
    conn.close()


def index_delete(conn, index): 
    curl = """curl -XDELETE 'http://localhost:9200/%s/'""" % (index, )
    status, reason, data = conn.delete(index)
    return (status, reason, data, curl)
    

def index_create(conn, index, mapping=""): 
    data = """{"settings": {"index": {"number_of_replicas": 0, "number_of_shards": 1}}}"""
    curl = """curl 'http://localhost:9200/%s/' -d '%s'""" % (index, data)
    status, reason, data = conn.put(index, data)
    return (status, reason, data, curl)


def index_stats(conn, index): 
    path = '%s/_stats' % index
    curl = """curl -XGET 'http://localhost:9200/%s'""" % (path, )
    status, reason, data = conn.get(path)
    return (status, reason, data, curl)


def document_put(conn, index, doctype, docid, data): 
    path = '%s/%s/%s' % (index, doctype, docid)
    curl = """curl -XPUT 'http://localhost:9200/%s' -d '%s'""" % (path, data)
    status, reason, data = conn.put(path, data)
    return (status, reason, data, curl)


def document_post(conn, index, doctype, data): 
    path = '%s/%s' % (index, doctype)
    curl = """curl -XPOST 'http://localhost:9200/%s' -d '%s'""" % (path, data)
    status, reason, data = conn.post(path, data)
    return (status, reason, data, curl)


def index_set_refresh_interval(conn, index, ri): 
    path = "%s/_settings" % (index, )
    data = '{"index": {"refresh_interval": "%s"}}' % ri
    curl = "curl -XPUT localhost:9200/%s -d '%s'" % (path, data)
    status, reason, data = conn.put(path, data)
    logger.info("set refresh interval on index %s to %s" % (index, ri))
    return (status, reason, data, curl)


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
#     segments = data['indices'][index]['shards']['0'][0]
    return segments
    

class SearchQuery(object): 

    def __init__(self, name, query): 
        """name: str, query: dict"""
        self.name = name
        self.query = query
            
    
    def execute(self, conn, index, doctype, stats): 
    
        path = '%s/%s/_search' % (index, doctype)
        query = dict(self.query)
        query['stats'] = stats if stats else []
        qs = json.dumps(query)
        qs = qs % {'variable': random.randint(0, 100000)}
        curl = """curl -XPOST 'http://localhost:9200/%s' -d '%s'""" % (path, qs)
        status, reason, data = conn.post(path, qs)
        return (status, reason, data, curl)

        
class Observation(object):

    _count = 0

    def __init__(self, benchmark, conn): 
    
        if not isinstance(benchmark, Benchmark):
            raise TypeError

        Observation._count += 1
        self.observation_id = Observation._count
        self.benchmark = benchmark
        self.conn = conn
        self.statgroups = set()
        self.ts_start = None
        self.ts_stop = None

    
    def __str__(self):

        return str(self.observation_id)


    def __enter__(self):

        logger.info("beginning observation no: %s" % self)
        self.t_optimize = index_optimize(self.conn, self.benchmark.index)
        self.ts_start = timestamp()
        return self

    
    def __exit__(self, exctype, value, tb): 

        self.ts_stop = timestamp()
        status, reason, data, curl = self.record()
        logger.info("finished observation no: %s, stats: %s" % (self, self.statgroups))


    def _statsgroupname(self, name): 
        s = "%03i_%s" % (self.observation_id, name) 
        self.statgroups.add(s)
        return s


    def record(self): 

        gs = ",".join(self.statgroups) if self.statgroups else ""
        index = self.benchmark.index
        path = "%s/_stats?clear=true&docs=true&store=true&search=true&merge=true&indexing=true&groups=%s" % (index, gs)
        curl = """curl -XGET 'http://localhost:9200/%s'""" % (path, )
        status, reason, data = self.conn.get(path)
        data = json.loads(data)

        stat = {
            'meta': {
                'benchmark_id': str(self.benchmark), 
                'observation_id': str(self.observation_id), 
                'observation_start': self.ts_start,
                'observation_stop': self.ts_stop, 
                'observation_groups': list(self.statgroups),  
            },
            'stats': data['indices'][index]['primaries'],
            'segments': index_segments(self.conn, index), 
        }
        stat['segments']['t_optimize'] = "%.2fs" % self.t_optimize
        stat['segments']['t_optimize_in_millis'] = int(self.t_optimize * 1000)

        stat['stats']['search']['groups'] = {k.split("_")[1]: v for k, v in stat['stats']['search']['groups'].items()}

#         print(json.dumps(stat, indent=4, sort_keys=True))

        path = 'stats/obs/%s_%i' % (self.benchmark, self.observation_id)
        data = json.dumps(stat)
        curl = """curl -XPUT 'http://localhost:9200/%s' -d '%s'""" % (path, data)
        status, reason, data = self.conn.put(path, data)
        logger.info("Recorded observation into: %s" % path)
        return (status, reason, data, curl)


class Benchmark(object):

    def __init__(self, benchmark_id, index, doctype, argv):

        self.benchmark_id = benchmark_id
        self.index = index
        self.doctype = doctype
        self.argv = argv
        self.time_start = None
        self.time_total_ms = None
        self.observations = []

        self.queries = []
        with open('queries.json', 'rU') as f:
            s = f.read()
        for name, body in json.loads(s).items(): 
            self.queries.append(SearchQuery(name, body))

    
    def __str__(self):
        return str(self.benchmark_id)


    def observe(self, conn): 

        observation = Observation(self, conn)
        self.observations.append(observation)
        return observation
    

    def run(self, conn, lines):

        self.time_start = timestamp()
        t1 = time.time()
                
        c = 0    
        for line in lines: 
            status, reason, data, curl = document_post(conn, self.index, self.doctype, line)
            if status not in (200, 201):
                logger.error("%s (%s) %s\n" % (status, reason, curl, ))
                continue
            c += 1
            if c == self.argv.period: 
                with self.observe(conn) as observation:
                    for query in self.queries: 
                        statname = observation._statsgroupname(query.name)
                        for n in range(1000): 
                            res = query.execute(conn, self.index, self.doctype, [statname])
                c = 0

        self.time_total = time.time() - t1
        
    
    def record(self, conn): 

        stat = {
            'benchmark_id': self.benchmark_id, 
            'argv': self.argv.__dict__, 
            'time_start': self.time_start, 
            'time_total': "%.2fm" % (self.time_total / 60.0), 
            'time_total_in_millis': int(self.time_total * 1000), 
            'queries': {q.name: q.query for q in self.queries}, 
        }
        data = json.dumps(stat)
        
#         print(json.dumps(stat, indent=4, sort_keys=True))

        path = 'stats/bench/%s' % (self,)
        curl = """curl -XPUT 'http://localhost:9200/%s' -d '%s'""" % (path, data)
        status, reason, data = conn.put(path, data)
        logger.info("Recorded benchmark into: %s" % path)
        return (status, reason, data, curl)


def timestamp(microseconds=False):
    DEFAULT_DATETIME_FORMAT = r'%Y-%m-%dT%H:%M:%SZ'
    DEFAULT_DATETIME_FORMAT_WITH_MICROSECONDS = r'%Y-%m-%dT%H:%M:%S.%fZ'
    dt = datetime.datetime.utcnow()
    if microseconds: 
        s = dt.strftime(DEFAULT_DATETIME_FORMAT_WITH_MICROSECONDS)
    else:
        s = dt.strftime(DEFAULT_DATETIME_FORMAT)
    return s


def echo(s): 
    if not VERBOSE:
        return
    print(s)
    

def args_parser():
    parser = argparse.ArgumentParser(description="esbench runner.")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--period', type=int, default=10, help='run tests every n records; (%(default)i)')
    parser.add_argument('--segments', type=int, metavar='N', default=None, help='max_num_segments for optimize calls; (%(default)s)')
    parser.add_argument('--refresh', type=str, metavar='T', default='1s', help="'refresh_interval' for the index, '-1' for none; (%(default)s)")
    parser.add_argument('n', nargs="?", type=int, default=100, help='number of documents; (%(default)i)')
    return parser


VERBOSE = False
SEGMENTS = None

def main():

    logging.basicConfig(level=logging.DEBUG)
    args = args_parser().parse_args()
    
    global VERBOSE
    VERBOSE = args.verbose

    global SEGMENTS
    SEGMENTS = args.segments

    with connect() as conn: 
                
        lines = itertools.islice(DATA.feed(), args.n)

        for index in ['stats', 'test']: 
            curl = index_delete(conn, index)[2]; echo(curl)
            curl = index_create(conn, index)[2]; echo(curl)
    
        index_set_refresh_interval(conn, 'test', args.refresh)
        
        benchmark = Benchmark('b1', 'test', 'doc', args)
        benchmark.run(conn, lines)
        benchmark.record(conn)
        index_set_refresh_interval(conn, 'test', "1s")


if __name__ == "__main__":
    main()

