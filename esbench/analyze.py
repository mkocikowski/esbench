# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

"""Code for retrieving, analyzing, and displaying recorded benchmark data. """

import itertools
import logging
import json
import collections
import re

import tabulate

import esbench


logger = logging.getLogger(__name__)


def _get_benchmarks(conn=None, stats_index_name=esbench.STATS_INDEX_NAME):
    path = "%s/bench/_search?sort=benchmark_start:asc&size=100" % (stats_index_name, )
    resp = conn.get(path)
    return resp

def benchmarks(resp, benchmark_ids=None):
    data = json.loads(resp.data)
    try:
        for benchmark in data['hits']['hits']:
            if benchmark_ids and not benchmark['_id'] in benchmark_ids:
                continue
            else:
                yield benchmark
    except KeyError:
        logger.warning("no benchmarks found", exc_info=False)
    return


def _get_observations(conn, benchmark_id, stats_index_name=esbench.STATS_INDEX_NAME):
    path = "%s/obs/_search?q=meta.benchmark_id:%s&sort=meta.observation_start:asc&size=10000" % (stats_index_name, benchmark_id, )
    resp = conn.get(path)
    return resp

def observations(resp):
    data = json.loads(resp.data)
    for observation in data['hits']['hits']:
        yield observation


def get(conn, benchmark_ids=None):
    for benchmark in benchmarks(_get_benchmarks(conn), benchmark_ids=benchmark_ids):
        for observation in observations(_get_observations(conn, benchmark['_id'])):
            data = {
                "benchmark": benchmark['_source'],
                "observation": observation['_source'],
            }
            data[u'benchmark'][u'_id'] = benchmark['_id']
            data[u'observation'][u'_id'] = observation['_id']
            yield data


def flat(data, matches=None):

    if matches is None:
        matches = ['.*']

    if type(matches) is not list:
        raise TypeError('matches must be a list of regular expression strings')

    matches = [re.compile(m, re.IGNORECASE) for m in matches]
    def passes(s):
        for m in matches:
            if not m.match(s):
                return False
        return True

    return sorted([l for l in data if passes(l[0])])


def flatten(data=None, flat=None, prefix=None):

    if flat is None:
        flat = list()

    if type(data) in [str, unicode, int, float, bool, None]:
        flat.append((prefix, data))

    elif type(data) is dict:
        for key in data:
            flatten(data=data[key], flat=flat, prefix=("%s.%s" % (prefix, key)) if prefix else key)

    elif type(data) is list:
        for n, v in enumerate(data):
            flatten(data=v, flat=flat, prefix=("%s.%i" % (prefix, n)) if prefix else str(n))

    return flat







def groups(conn, benchmark_ids=None):
    for data in get(conn=conn, benchmark_ids=benchmark_ids):
        # each observation contains stats groups - here referred to as
        # 'groups' which record information on each of the queries which
        # form part of the benchmark. a stats group in context (number of
        # doucments in the index, benchmark info) forms the basic unit of
        # measured data
        gs = data['observation']['stats']['search']['groups']
        for name, group in gs.items():
            yield data['benchmark'], data['observation'], (name, group)


StatRecord = collections.namedtuple('StatRecord', [
        'bench_id',
        'bench_name',
        'obs_id',
        'obs_no',
        'doc_cnt',
        'seg_cnt',
        'size_b',
        'field_data_b',
        'heap_used_b',
        'heap_used_pct',
        'open_fd',
        't_index_ms',
        'query_name',
        'n_query',
        't_query_ms',
        't_fetch_ms',
        't_client_ms',
    ]
)


def stat_tuple(benchmark, observation, stat):
    stat_name, stat_data = stat
    record = StatRecord(
            bench_id=benchmark['_id'],
            bench_name=benchmark.get('benchmark_name', 'unknown'),
            obs_id=observation['_id'],
            obs_no=observation['meta']['observation_sequence_no'],
            doc_cnt=observation['stats']['docs']['count'],
            seg_cnt=observation['segments']['num_search_segments'],
            size_b=observation['stats']['store']['size_in_bytes'],
            field_data_b=observation['stats']['fielddata']['memory_size_in_bytes'],
            heap_used_b=[v['jvm']['mem']['heap_used_in_bytes'] for _, v in observation['cluster']['nodes'].items()][0],
            heap_used_pct=[v['jvm']['mem']['heap_used_percent'] for _, v in observation['cluster']['nodes'].items()][0],
            open_fd=[v['process']['open_file_descriptors'] for _, v in observation['cluster']['nodes'].items()][0],
            t_index_ms=observation['stats']['indexing']['index_time_in_millis'],
            query_name=stat_name,
            n_query=stat_data['query_total'],
            t_query_ms=stat_data['query_time_in_millis'],
            t_fetch_ms=stat_data['fetch_time_in_millis'],
            t_client_ms=stat_data['client_time_in_millis'],
    )
    return record


def get_group_tuples(conn, benchmark_ids=None, sort_f=lambda stat: (stat.bench_id, stat.query_name, stat.obs_no)):
    # set sort_f to None to not sort
    data = [stat_tuple(benchmark, observation, stat) for benchmark, observation, stat in groups(conn, benchmark_ids)]
    data = sorted(data, key=sort_f)
    return data




def show_benchmarks(conn, benchmark_ids=None, sample=1, fmt='tab', indent=4):
    data = get_group_tuples(conn, benchmark_ids)
    if data:
        legend = """
------------------------------------------------------------------------------
All times recorded aggregate, look at the related n_ value. So if 'n_query' == 100, and 't_query_ms' == 1000, it means
that it took 1000ms to run the query 100 times, so 10ms per query.
------------------------------------------------------------------------------
""".strip()
        print(legend)
        print(tabulate.tabulate(data, headers=data[0]._fields))
        print(legend)


def dump_benchmarks(conn=None, ids=None, stats_index_name=esbench.STATS_INDEX_NAME):
    """Dump benchmark data as a sequence of curl calls.

    You can save these calls to a file, and then replay them somewhere else.
    """

    for benchmark in benchmarks(_get_benchmarks(conn=conn, stats_index_name=stats_index_name), ids):
        curl = """curl -XPUT 'http://localhost:9200/%s/bench/%s' -d '%s'""" % (stats_index_name, benchmark['_id'], json.dumps(benchmark['_source']))
        print(curl)
        for o in observations(_get_observations(conn, benchmark['_id'], stats_index_name=stats_index_name)):
            curl = """curl -XPUT 'http://localhost:9200/%s/obs/%s' -d '%s'""" % (stats_index_name, o['_id'], json.dumps(o['_source']))
            print(curl)
    return


def delete_benchmarks(conn=None, benchmark_ids=None, stats_index_name=esbench.STATS_INDEX_NAME):

    if not benchmark_ids:
        resp = conn.delete(stats_index_name)
        logger.info(resp.curl)

    else:
        for benchmark in benchmarks(_get_benchmarks(conn, stats_index_name=stats_index_name), benchmark_ids=benchmark_ids):
            for observation in observations(_get_observations(conn, benchmark_id=benchmark['_id'], stats_index_name=stats_index_name)):
                path = "%s/obs/%s" % (stats_index_name, observation['_id'], )
                resp = conn.delete(path)
                logger.info(resp.curl)
            path = "%s/bench/%s" % (stats_index_name, benchmark['_id'], )
            resp = conn.delete(path)
            logger.info(resp.curl)

    return

