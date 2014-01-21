# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


"""A benchmark alternatively loads data and runs observations.

A benchmark consists of a set number of observations. Each observation
executes a set number of queries, and records execution stats. In between
observations, set amounts of data are inserted into the index. Load some data,
run some queries, record stats, repeat. All stats are stored in a separate ES
index ('esbench_stats') as doctypes 'obs' and 'bench'.

Classes:

    - SearchQuery: per-observation query wrapper
    - Observation: repeated n-times constitutes a benchmark
    - Benchmark: orchestrates data loading and observations

"""


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
    """Each observation has a SearchQuery object for each bench query.

    The 'magic' is that each SearchQuery has a 'stats' field which is unique
    to this query / observation combination. This is what allows for tracking
    query execution stats with the 'stats group' unique to this particular
    observation.

    In addition, the execute() method will do basic templating, replacing the
    'variable' element in query template with a random string.

    """

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
    """Runs specified queries and records the results.

    Even though the queries are the same for each observation in a benchmark,
    each observation creates unique stats groups for each of its queries -
    this is what allows to pull stats specific to an observation from ES stats
    api.

    Methods:

        _segments(): segment stats
        _stats(): 'stats group' (query exec time), memory, and fielddata stats
        _cluster_stats(): cluster stats snapshot

        run(): run an observation comprising of a set of queries
        record(): pull in stats data and record it

    """

    _count = 0

    def __init__(
            self,
            conn=None,
            benchmark_id=None,
            queries=None,
            reps=None, ):

        self.conn = conn
        self.benchmark_id = benchmark_id
        self.reps = reps # how many times each query will be executed

        Observation._count += 1
        self.observation_sequence_no = Observation._count
        self.observation_id = uuid()

        self.queries = []
        for name, body in queries.items():
            self.queries.append(
                SearchQuery(name, body, self.observation_id, esbench.TEST_INDEX_NAME, esbench.TEST_DOCTYPE_NAME)
            )

        self.ts_start = None
        self.ts_stop = None
        self.t1 = time.time()
        self.t_optimize = 0


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
        """Get and massage segment stats data.

        By default, the "/[index]/_segments" api end point is used. This
        returns a lot of per-shard data, which gets aggregated, returning a
        dictionary with following keys:

            - "num_search_segments": sum for all primaries and replicas
            - "num_committed_segments": sum for all primaries and replicas
            - "t_optimize": time spent on the explicit optimize call (0 if call was not made)
            - "t_optimize_in_millis"
            - "shards": total number of primaries and replicas

        """

        resp = segments_f(self.conn, esbench.TEST_INDEX_NAME)
        _s = json.loads(resp.data)

        segments = {
            "num_search_segments": sum([s['num_search_segments'] for shard in _s['indices'][esbench.TEST_INDEX_NAME]['shards'].values() for s in shard]),
            "num_committed_segments": sum([s['num_committed_segments'] for shard in _s['indices'][esbench.TEST_INDEX_NAME]['shards'].values() for s in shard]),
            "t_optimize": "%.2fs" % (self.t_optimize, ),
            "t_optimize_in_millis": int(self.t_optimize * 1000),
            "shards": sum([len(shard) for shard in _s['indices'][esbench.TEST_INDEX_NAME]['shards'].values()]),
        }

        return segments


    def _stats(self, stats_f=esbench.api.index_get_stats):
        """Pull in stats group data.

        ES keeps track of stats groups (exec time etc) defined in the 'stats'
        field of each query. Each observation will create unique 'stats'
        values for its queries (see SearchQuery class) and so be able to see
        stats just for the queries run as part of this specific observation.
        This mathod retrieves the stats and parses them.

        """

        # we need to specifically ask for the stats groups we want, by name.
        stats_group_names = [q.stats_group_name for q in self.queries]
        resp = stats_f(self.conn, esbench.TEST_INDEX_NAME, ",".join(stats_group_names))
        logger.debug("stats call: %s", resp.curl)
        try:
            stats = json.loads(resp.data)['indices'][esbench.TEST_INDEX_NAME]['primaries']
        except KeyError: # compatibility with 19.9
            stats = json.loads(resp.data)['_all']['indices'][esbench.TEST_INDEX_NAME]['primaries']

        def _remove_obs_id(s):
            return "_".join(s.split("_")[1:])

        stats['search']['groups'] = {
            _remove_obs_id(k): v for
            k, v in stats['search']['groups'].items()
        }

        for query in self.queries:
            logger.debug("query %s execution count: %i", query.name, query.execution_count)
            stats['search']['groups'][query.name]['client_total'] = query.execution_count
            stats['search']['groups'][query.name]['client_time'] = "%.2fs" % (query.t_client, ) if query.t_client else None
            stats['search']['groups'][query.name]['client_time_in_millis'] = int(query.t_client * 1000.0) if query.t_client else None
            stats['search']['groups'][query.name]['client_time_in_millis_per_query'] = float(stats['search']['groups'][query.name]['client_time_in_millis']) / query.execution_count if query.execution_count else None
            stats['search']['groups'][query.name]['fetch_time_in_millis_per_query'] = float(stats['search']['groups'][query.name]['fetch_time_in_millis']) / query.execution_count if query.execution_count else None
            stats['search']['groups'][query.name]['query_time_in_millis_per_query'] = float(stats['search']['groups'][query.name]['query_time_in_millis']) / query.execution_count if query.execution_count else None

        return stats


    def _cluster_stats(self, cluster_f=esbench.api.cluster_get_stats, fielddata_f=esbench.api.cluster_get_fielddata_stats):

        try:
            resp = cluster_f(self.conn)
            cluster_stats = json.loads(resp.data)
            cluster_stats['node_count'] = len(cluster_stats['nodes'].keys())

            # the reason why getting fielddata here is not redundant with the
            # fielddata gathered in _stats is that here information is
            # gathered on per-node basis, which, in a multi-node setup, may be
            # interesting.
            #
            try:
                resp = fielddata_f(self.conn)
                fielddata_stats = json.loads(resp.data)

                for node in cluster_stats['nodes']:
                    try:
                        cluster_stats['nodes'][node]['indices']['fielddata']['fields'] = fielddata_stats['nodes'][node]['indices']['fielddata']['fields']
                    except KeyError:
                        logger.warning("couldn't get fielddata stats for node: %s", node)

            except (TypeError, IOError) as exc:
                logger.warning("couldn't get cluster fielddata stats: %s", exc)

        except (TypeError, IOError) as exc:
            logger.warning("couldn't get cluster stats: %s", exc)
            cluster_stats = None

        return cluster_stats


    def record(self):

        t_total = time.time() - self.t1
        obs = {
            'meta': {
                'benchmark_id': self.benchmark_id,
                'observation_id': self.observation_id,
                'observation_sequence_no': self.observation_sequence_no,
                'observation_start': self.ts_start,
                'observation_stop': self.ts_stop,
                't_total': "%.2fm" % (t_total / 60.0),
                't_total_in_millis': int(t_total * 1000),
            },
            'segments': self._segments(),
            'stats': self._stats(),
            'cluster': self._cluster_stats(),
        }

        data = json.dumps(obs, sort_keys=True)
        path = '%s/obs/%s' % (esbench.STATS_INDEX_NAME, self.observation_id, )
        resp = self.conn.put(path, data)
        if resp.status not in [200, 201]:
            logger.error(resp)
        logger.info("recorded observation into: http://%s:%i/%s", self.conn.host, self.conn.port, path)
        return resp



class Benchmark(object):
    """Orchestrates the loading of data and running of observations. """

    def __init__(self, config=None, conn=None):

        self.benchmark_id = uuid()

        self.config = config
        self.conn = conn

        self.ts_start = None
        self.ts_stop = None
        self.t_total = None
        self.t1 = None


    def __str__(self):
        return str(self.benchmark_id)


    def observe(self, obs_cls=Observation):

        observation = obs_cls(
                        conn=self.conn,
                        benchmark_id=self.benchmark_id,
                        queries=self.config['queries'],
                        reps=self.config['config']['reps'],
        )

        if self.config['config']['segments']:
            t1 = time.time()
            logger.info("starting optimize call...")
            resp = esbench.api.index_optimize(self.conn, esbench.TEST_INDEX_NAME, self.config['config']['segments'])
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
        size_b = 0
        logger.debug("begining data load...")
        for line in lines:
            size_b += len(line)
            resp = esbench.api.document_post(self.conn, esbench.TEST_INDEX_NAME, esbench.TEST_DOCTYPE_NAME, line)
            count += 1
        logger.info("loaded %i lines into index '%s', size: %i (%.2fMB)", count, esbench.TEST_INDEX_NAME, size_b, size_b/(1<<20))
        return (count, size_b)


    def run(self, batches):

        index_settings = {"settings" : {"index" : {"number_of_shards" : 1, "number_of_replicas" : 0}}}
        esbench.api.index_create(self.conn, esbench.STATS_INDEX_NAME, index_settings)

        if not self.config['config']['append']:
            esbench.api.index_delete(self.conn, esbench.TEST_INDEX_NAME)
            esbench.api.index_create(self.conn, esbench.TEST_INDEX_NAME, self.config['index'])

        total_count = 0
        total_size_b = 0
        for batch in batches:
            count, size_b = self.load(batch)
            if not count:
                break
            total_count += count
            total_size_b += size_b
            self.observe()

        logger.info("load complete; loaded total %i lines into index '%s', total size: %i (%.2fmb)", total_count, esbench.TEST_INDEX_NAME, total_size_b, total_size_b/(1<<20))


    def _get_cluster_info(self, cluster_f=esbench.api.cluster_get_info):

        try:
            resp = cluster_f(self.conn)
            cluster_info = json.loads(resp.data)
            cluster_info['node_count'] = len(cluster_info['nodes'].keys())

        except (TypeError, IOError) as exc:
            logger.warning("couldn't get cluster info: %s", exc)
            cluster_info = None

        return cluster_info


    def record(self):

        self.ts_stop = timestamp()
        self.t_total = time.time() - self.t1

        stat = {
            'meta': {
                'benchmark_id': self.benchmark_id,
                'benchmark_name': self.config['config']['name'],
                'benchmark_start': self.ts_start,
                'benchmark_stop': self.ts_stop,
                't_total': "%.2fm" % (self.t_total / 60.0),
                't_total_in_millis': int(self.t_total * 1000),
                'config': json.dumps(self.config, sort_keys=True),
            },

            'cluster': self._get_cluster_info(),
        }

        data = json.dumps(stat, sort_keys=True)
        path = '%s/bench/%s' % (esbench.STATS_INDEX_NAME, self,)
        resp = self.conn.put(path, data)
        if resp.status not in [200, 201]:
            raise IOError("failed to record benchmark")
        logger.info("recorded benchmark into: http://%s:%i/%s", self.conn.host, self.conn.port, path)
        return resp

