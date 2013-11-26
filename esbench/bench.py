# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import itertools
import logging
import json
import time
import random
import datetime
import hashlib
import string

import esbench.api
import esbench.data


logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10


def uuid():
    return hashlib.md5("%s%f" % (str(time.time()), random.random())).hexdigest()[:8]


# timeit.timeit('rands(6)', setup='from __main__ import rands', number=1000)
def rands(length=6):
    l = len(string.ascii_letters)-1
    s = "".join((string.ascii_letters[random.randint(0, l)] for _ in range(length)))
    return s


def timestamp(microseconds=False):
    DEFAULT_DATETIME_FORMAT = r'%Y-%m-%dT%H:%M:%SZ'
    DEFAULT_DATETIME_FORMAT_WITH_MICROSECONDS = r'%Y-%m-%dT%H:%M:%S.%fZ'
    dt = datetime.datetime.utcnow()
    if microseconds:
        s = dt.strftime(DEFAULT_DATETIME_FORMAT_WITH_MICROSECONDS)
    else:
        s = dt.strftime(DEFAULT_DATETIME_FORMAT)
    return s


class SearchQuery(object):

    def __init__(self, name, query, observation_id, index, doctype):

        self.observation_id = observation_id
        self.name = name
        self.query = dict(query)
        self.execution_count = 0 # how many times it has been executed
        self.stats_group_name = "%s_%s" % (self.observation_id, self.name)
        self.query['stats'] = [self.stats_group_name]

        self.query_path = '%s/%s/_search' % (index, doctype)
        self.query_string = json.dumps(self.query, sort_keys=True)

        self.t_client = None


    def execute(self, conn):

        qs = self.query_string % {'variable': rands(6)}
        resp = conn.post(self.query_path, qs)
        self.execution_count += 1
        return resp



class Observation(object):

    _count = 0

    def __init__(
            self,
            conn=None,
            stats_index_name=None,
            benchmark_id=None,
            queries=None,
            reps=None,
            doc_index_name=None,
            doctype=None):

        self.conn = conn
        self.stats_index_name = stats_index_name
        self.benchmark_id = benchmark_id
        self.reps = reps # how many times each query will be executed
        self.doc_index_name = doc_index_name
        self.doctype = doctype

        Observation._count += 1
        self.observation_sequence_no = Observation._count
        self.observation_id = uuid()

        self.queries = []
        for name, body in queries.items():
            self.queries.append(
                SearchQuery(name, body, self.observation_id, self.doc_index_name, self.doctype)
            )

        self.ts_start = None
        self.ts_stop = None
        self.t_optimize = None


    def run(self):

        self.ts_start = timestamp()
        logger.info("beginning observation no: %i, %s", self.observation_sequence_no, self.ts_start)
        t1 = time.time()

        for query in self.queries:
            tA = time.time()
            for _ in range(self.reps):
                query.execute(self.conn)
            query.t_client = time.time() - tA
            logger.info("ran query '%s' %i times in %.2fs", query.name, self.reps, query.t_client)

        self.ts_stop = timestamp()
        logger.info("finished observation no: %i, id: %s, time: %.3f",
            self.observation_sequence_no, self.observation_id, time.time()-t1)


    def _segments(self, segments_f=esbench.api.index_get_segments):

        resp = segments_f(self.conn, self.doc_index_name)
        _s = json.loads(resp.data)

        segments = {
            "num_search_segments": _s['indices'][self.doc_index_name]['shards']['0'][0]['num_search_segments'],
            "num_committed_segments": _s['indices'][self.doc_index_name]['shards']['0'][0]['num_committed_segments'],
            "t_optimize": "%.2fs" % (self.t_optimize, ) if self.t_optimize else None,
            "t_optimize_in_millis": int(self.t_optimize * 1000) if self.t_optimize else None,
            "segments": _s['indices'][self.doc_index_name]['shards']['0'][0]['segments'],
        }

        return segments


    def _stats(self, stats_f=esbench.api.index_get_stats):

        # we need to specifically ask for the stats groups we want, by name.
        stats_group_names = [q.stats_group_name for q in self.queries]
        resp = stats_f(self.conn, self.doc_index_name, ",".join(stats_group_names))
#         logger.debug("stats call: %s", resp.curl)
        stats = json.loads(resp.data)['indices'][self.doc_index_name]['primaries']

        def _remove_obs_id(s):
            return "_".join(s.split("_")[1:])

        stats['search']['groups'] = {
            _remove_obs_id(k): v for
            k, v in stats['search']['groups'].items()
        }

        for query in self.queries:
            stats['search']['groups'][query.name]['client_total'] = query.execution_count
            stats['search']['groups'][query.name]['client_time'] = "%.2fs" % (query.t_client, ) if query.t_client else None
            stats['search']['groups'][query.name]['client_time_in_millis'] = int(query.t_client * 1000.0) if query.t_client else None

        return stats


    def record(self):

        obs = {
            'meta': {
                'benchmark_id': self.benchmark_id,
                'observation_id': self.observation_id,
                'observation_sequence_no': self.observation_sequence_no,
                'observation_start': self.ts_start,
                'observation_stop': self.ts_stop,
            },
            'segments': self._segments(),
            'stats': self._stats(),
        }

#         print(json.dumps(obs, indent=4, sort_keys=True))
        data = json.dumps(obs, sort_keys=True)
        path = '%s/obs/%s' % (self.stats_index_name, self.observation_id, )
        resp = self.conn.put(path, data)
        if resp.status not in [200, 201]:
            logger.error(resp)
        logger.info("recorded observation into: %s", path)
        return resp


class Benchmark(object):

    def __init__(self, cmnd=None, argv=None, conn=None, stats_index_name=None):

        self.benchmark_id = uuid()
        self.cmnd = cmnd
        self.argv = argv
        self.conn = conn

        self.config = None
        with open(argv.config_file_path, 'rU') as f:
            self.config = json.loads(f.read())

        self.doc_index_name = self.config['name_index']
        self.doctype = self.config['name_doctype']

        self.stats_index_name = stats_index_name

        self.ts_start = None
        self.ts_stop = None
        self.t_total = None
        self.t1 = None


    def __str__(self):
        return str(self.benchmark_id)


    def observe(self, obs_cls=Observation):

        observation = obs_cls(
                        conn = self.conn,
                        stats_index_name = self.stats_index_name,
                        benchmark_id = self.benchmark_id,
                        queries = self.config['queries'],
                        reps = self.argv.repetitions,
                        doc_index_name = self.doc_index_name,
                        doctype = self.doctype
        )

        if not self.argv.no_optimize_calls:
            t1 = time.time()
            resp = esbench.api.index_optimize(self.conn, self.doc_index_name, self.argv.segments)
            observation.t_optimize = time.time() - t1
            logger.info("optimize call: %.2fs", observation.t_optimize)

        observation.run()
        observation.record()

        return observation


    def prepare(self):

        self.ts_start = timestamp()
        self.t1 = time.time()


    def load(self, lines):

        count = 0
        for line in lines:
            resp = esbench.api.document_post(self.conn, self.doc_index_name, self.doctype, line)
            count += 1
        logger.info("loaded %i lines into index '%s'", count, self.doc_index_name)
        return count


    def run(self, lines):

        index_settings = {"settings" : {"index" : {"number_of_shards" : 1, "number_of_replicas" : 0}}}
        esbench.api.index_create(self.conn, self.stats_index_name, index_settings)

        if not self.argv.append:
            esbench.api.index_delete(self.conn, self.doc_index_name)
            esbench.api.index_create(self.conn, self.doc_index_name, self.config['index'])

        observation_period = self.argv.n // self.argv.observations
        if observation_period < 10:
            observation_period = 10

        while True:
            batch = itertools.islice(lines, observation_period)
            if not self.load(batch):
                break
            self.observe()


    def record(self):

        self.ts_stop = timestamp()
        self.t_total = time.time() - self.t1

        stat = {
            'benchmark_id': self.benchmark_id,
            'benchmark_name': self.argv.name,
            'benchmark_start': self.ts_start,
            'benchmark_stop': self.ts_stop,
            't_total': "%.2fm" % (self.t_total / 60.0),
            't_total_in_millis': int(self.t_total * 1000),
            'argv': self.argv.__dict__,
            'cmnd': self.cmnd,
            'config': json.dumps(self.config, sort_keys=True),
        }

        data = json.dumps(stat, sort_keys=True)
        path = '%s/bench/%s' % (self.stats_index_name, self,)
        resp = self.conn.put(path, data)
        logger.info("recorded benchmark into: %s", path)
        return resp

