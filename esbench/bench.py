# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import httplib
import contextlib
import itertools
import logging
import argparse
import json
import time
import random
import datetime
import hashlib
import string

import esbench.data


logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10


def retry_and_reconnect_on_IOError(method): 
    def wrapper(self, *args, **kwargs): 
        for i in [1, 2, 5, 10, 25, 50, 75, 100]:
            try: 
                if not self.conn:
                    self.connect(timeout=self.timeout*i)
                res = method(self, *args, **kwargs)
                # connections with really long timeouts should not be kept
                # around, they are an exceptional thing
                if i >= 25: 
                    self.close()
                return res
            except IOError as (exc):
                logger.warning("%s (%s) in retry_and_reconnect_on_IOError try: %i, pause: %is", type(exc), exc, i, 1)
                self.close()
        raise # re-raises the last exception, so most likely IOError
    return wrapper


class Conn(object): 
    
    def __init__(self, host='localhost', port=9200, timeout=DEFAULT_TIMEOUT): 
        self.host = host
        self.port = port
        self.timeout = timeout
        self.conn = None
    
    def connect(self, timeout=DEFAULT_TIMEOUT): 
#         logger.debug("Attempting HTTPConnection (%s:%i) with timeout: %s", self.host, self.port, timeout)
        self.conn = httplib.HTTPConnection(host=self.host, port=self.port, timeout=timeout)
        self.conn.connect()
    
    def close(self): 
        self.conn.close()
        self.conn = None
    
    @retry_and_reconnect_on_IOError
    def get(self, path):
        self.conn.request('GET', path, body=None)
        resp = self.conn.getresponse()
        data = resp.read()
        return (resp.status, resp.reason, data)
    
    @retry_and_reconnect_on_IOError
    def put(self, path, data):
        head = {'Content-type': 'application/json'}
        self.conn.request('PUT', path, data, head)
        resp = self.conn.getresponse()
        data = resp.read()
        return (resp.status, resp.reason, data)

    @retry_and_reconnect_on_IOError
    def post(self, path, data):
        head = {'Content-type': 'application/json'}
        self.conn.request('POST', path, data, head)
        resp = self.conn.getresponse()
        data = resp.read()
        return (resp.status, resp.reason, data)

    @retry_and_reconnect_on_IOError
    def delete(self, path, data=None):
        self.conn.request('DELETE', path)
        resp = self.conn.getresponse()
        data = resp.read()
        return (resp.status, resp.reason, data)


@contextlib.contextmanager
def connect(host='localhost', port=9200, timeout=DEFAULT_TIMEOUT): 
    conn = Conn(host=host, port=port, timeout=timeout)
    yield conn
    conn.close()



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


def index_create(conn, index, config=None): 
#     data = {
#         "settings": {
#             "index": {
#                 "number_of_replicas": 0, 
#                 "number_of_shards": 1, 
#             }
#         }, 
#         "mappings": {
#             "doc": mapping
#         }
#     }

    data = json.dumps(config)

#     data = """{"settings": {"index": {"number_of_replicas": 0, "number_of_shards": 1}}}"""
    curl = """curl -XPUT 'http://localhost:9200/%s/' -d '%s'""" % (index, data)
    logger.debug(curl)
    status, reason, data = conn.put(index, data)
    return (status, reason, data, curl)


def index_delete(conn, index): 
    curl = """curl -XDELETE 'http://localhost:9200/%s/'""" % (index, )
    status, reason, data = conn.delete(index)
    return (status, reason, data, curl)
    

def index_stats(conn, index, groups): 

    path = "%s/_stats?clear=true&docs=true&store=true&search=true&merge=true&indexing=true&groups=%s" % (index, groups)
    curl = """curl -XGET 'http://localhost:9200/%s'""" % (path, )
    status, reason, data = conn.get(path)
    return (status, reason, data, curl)


def index_set_refresh_interval(conn, index, ri): 
    path = "%s/_settings" % (index, )
    data = '{"index": {"refresh_interval": "%s"}}' % ri
    curl = "curl -XPUT localhost:9200/%s -d '%s'" % (path, data)
    status, reason, data = conn.put(path, data)
    logger.info("set refresh interval on index %s to %s", index, ri)
    return (status, reason, data, curl)


def index_optimize(conn, index, nseg=0): 

#     logger.info("optimizing...")
    t1 = time.time()
    if nseg: 
        path = "%s/_optimize?max_num_segments=%i&refresh=true&flush=true&wait_for_merge=true" % (index, nseg)
    else:
        path = "%s/_optimize?refresh=true&flush=true&wait_for_merge=true" % (index, )
#     curl = "curl -XPOST localhost:9200/%s" % (path, )
    _ = conn.post(path, None)
    td = time.time() - t1
    segments = index_segments(conn, index)
    logger.info("done optimizing: %.2fs, total of %i/%i segments.", td, segments['num_search_segments'], segments['num_committed_segments'])
    return td
    

def index_segments(conn, index): 
    path = "%s/_segments" % (index, )
#     curl = "curl -XGET http://localhost:9200/test/_segments" 
    _, _, data = conn.get(path)
    data = json.loads(data)
    segments = { 
        "num_search_segments": data['indices'][index]['shards']['0'][0]['num_search_segments'], 
        "num_committed_segments": data['indices'][index]['shards']['0'][0]['num_committed_segments'], 
    }
    return segments
    


# timeit.timeit('rands(6)', setup='from __main__ import rands', number=1000)
def rands(length=6):
    l = len(string.ascii_letters)-1
    s = "".join((string.ascii_letters[random.randint(0, l)] for _ in range(length)))
    return s


class SearchQuery(object): 

    def __init__(self, name, query): 
        """name: str, query: dict"""
        self.name = name
        self.query = query
        self._qp = None
        self._qs = None
        

    def prepare(self, index, doctype, stats): 

        self._qp = '%s/%s/_search' % (index, doctype)
        query = dict(self.query)
        query['stats'] = stats if stats else []
        self._qs = json.dumps(query)
        
    
    def execute(self, conn): 
        
        qs = self._qs % {'variable': rands(6)}
        status, reason, data = conn.post(self._qp, qs)
        return (status, reason, data, None)


class Observation(object):

    _count = 0

    def __init__(self, benchmark, conn): 
    
        if not isinstance(benchmark, Benchmark):
            raise TypeError

        Observation._count += 1
        self.observation_sequence_no = Observation._count
        self.observation_id = hashlib.md5(str(time.time())).hexdigest()[:8]
        self.benchmark = benchmark
        self.conn = conn
        self.stats_group_names = set()
        self.ts_start = None
        self.ts_stop = None
        self.t_optimize = None
        self._t1 = None
    
    def __str__(self):
        return str(self.observation_id)


    def __enter__(self):

        logger.info("beginning observation no: %i", self.observation_sequence_no)
        if self.benchmark.argv.no_optimize_calls: 
            self.t_optimize = 0
        else:
            self.t_optimize = index_optimize(self.conn, self.benchmark.config['name_index'], self.benchmark.argv.segments)
        self.ts_start = timestamp()
        self._t1 = time.time()
        return self

    
    def __exit__(self, exctype, value, tb): 

        self.ts_stop = timestamp()
        self.record()
        logger.info("finished observation no: %i, id: %s, time: %.3f", self.observation_sequence_no, self.observation_id, time.time()-self._t1)


    def record(self): 

        index = self.benchmark.config['name_index']
        # we need to specifically ask for the stats groups we want, by name.
        # this if why stats groups are recorded at the time query is prepared.
        groups = ",".join(self.stats_group_names) if self.stats_group_names else ""
        status, reason, data, curl = index_stats(self.conn, index, groups)
        data = json.loads(data)

        stat = {
            'meta': {
                'benchmark_id': self.benchmark.benchmark_id, 
                'observation_id': self.observation_id, 
                'observation_sequence_no': self.observation_sequence_no, 
                'observation_start': self.ts_start,
                'observation_stop': self.ts_stop, 
                'observation_groups': list(self.stats_group_names),  
            },
            'stats': data['indices'][index]['primaries'],
            'segments': index_segments(self.conn, index), 
        }
        stat['segments']['t_optimize'] = "%.2fs" % self.t_optimize
        stat['segments']['t_optimize_in_millis'] = int(self.t_optimize * 1000)

        def _remove_obs_id(s): 
            return "_".join(s.split("_")[1:])

        stat['stats']['search']['groups'] = {
            _remove_obs_id(k): v for 
            k, v in stat['stats']['search']['groups'].items()
        }

#         print(json.dumps(stat, indent=4, sort_keys=True))

        path = 'stats/obs/%s' % (self.observation_id, )
        data = json.dumps(stat)
        curl = """curl -XPUT 'http://localhost:9200/%s' -d '%s'""" % (path, data)
        status, reason, data = self.conn.put(path, data)
#         logger.info("recorded observation into: %s", path)
        return (status, reason, data, curl)


class Benchmark(object):

    def __init__(self, argv):

        self.benchmark_id = hashlib.md5(str(time.time())).hexdigest()[:8]
        self.argv = argv
        
        self.config = None
        with open(argv.config_file_path, 'rU') as f:
            self.config = json.loads(f.read())

        self.queries = []
        for name, body in self.config['queries'].items():
            self.queries.append(SearchQuery(name, body))

        self.observations = []

        self.time_start = None
        self.time_total = 0.0
        self.time_total_ms = None


    def __str__(self):
        return str(self.benchmark_id)


    def observe(self, conn): 

        observation = Observation(self, conn)
        self.observations.append(observation)
        # Observation class implements the context manager protocol, so here
        # we are returning an observation object, which will be used in the
        # 'with' clause of the benchmark
        return observation


    def prepare(self, conn): 
        index_delete(conn, self.config['name_index'])
        index_create(conn, self.config['name_index'], self.config['index'])
        index_set_refresh_interval(conn, self.config['name_index'], self.argv.refresh)


    def run(self, conn, lines):

        self.time_start = timestamp()
        t1 = time.time()

        period = self.argv.n // self.argv.observations
        if period < 10: 
            period = 10
        c = 0    
        for line in lines: 
            status, reason, data, curl = document_post(conn, self.config['name_index'], self.config['name_doctype'], line)
            if status not in (200, 201):
                logger.error("%s (%s) %s\n", status, reason, curl)
                continue
            else:
                logger.debug(data)
            c += 1
            if c == period: 
                time.sleep(1)
                with self.observe(conn) as observation:
                    for query in self.queries: 
                        stats_group_name = "%s_%s" % (observation.observation_id, query.name)
                        # so we need to not only name the stats group
                        # explicitly in the query to make sure it gets
                        # recorded, but then when it is time to look up the
                        # stats, we need to request that stats group by name -
                        # so this is why a given observation maintains a list
                        # of its stats group names. 
                        #
                        observation.stats_group_names.add(stats_group_name)
                        # call to query.prepare() results in the query being
                        # serialized into json with the proper stats group
                        # name etc, basically so that the recurring calls to
                        # query.execute() will not incur the penalty of
                        # serializing into json each time
                        #
                        query.prepare(self.config['name_index'], self.config['name_doctype'], [stats_group_name])
                        tq1 = time.time()
                        for _ in range(1000): 
                            query.execute(conn)
                        logger.debug("query: %s executed %i times, time: %.3f", query.name, 1000, time.time()-tq1)
                c = 0
                # update time_total periodically so that if the benchmark gets
                # interrupted, at least some information is captured. but
                # don't want to do this on each iteration, to not slow things
                # down needlessly
                self.time_total = time.time() - t1

        self.time_total = time.time() - t1
        

    
    def record(self, conn): 

        stat = {
            'benchmark_id': self.benchmark_id, 
            'argv': self.argv.__dict__, 
            'time_start': self.time_start, 
            'time_total': "%.2fm" % (self.time_total / 60.0), 
#             'time_total': "%im%is" % (divmod(self.time_total, 60)), 
            'time_total_in_millis': int(self.time_total * 1000), 
            'queries': {q.name: q.query for q in self.queries}, 
        }
        data = json.dumps(stat)
        
#         print(json.dumps(stat, indent=4, sort_keys=True))

        path = 'stats/bench/%s' % (self,)
        curl = """curl -XPUT 'http://localhost:9200/%s' -d '%s'""" % (path, data)
        status, reason, data = conn.put(path, data)
        logger.info("recorded benchmark into: %s", path)
        index_set_refresh_interval(conn, self.config['name_index'], "1s")
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

