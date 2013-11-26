# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import itertools
import logging
import json
import collections

import tabulate

import esbench


logger = logging.getLogger(__name__)


def _get_benchmarks(conn, stats_index_name=esbench.STATS_INDEX_NAME):
    path = "%s/bench/_search?sort=benchmark_start:asc&size=100" % (stats_index_name, )
    resp = conn.get(path)
    return resp

def benchmarks(resp, ids=None):
    data = json.loads(resp.data)
    try:
        for benchmark in data['hits']['hits']:
            if ids and not benchmark['_id'] in ids:
                continue
            else:
                yield benchmark
    except KeyError:
        logger.error("no benchmarks found", exc_info=False)
    return


def _get_observations(conn, benchmark_id, stats_index_name=esbench.STATS_INDEX_NAME):
    path = "%s/obs/_search?q=meta.benchmark_id:%s&sort=meta.observation_start:asc&size=10000" % (stats_index_name, benchmark_id, )
    resp = conn.get(path)
    return resp

def observations(resp):
    data = json.loads(resp.data)
    for observation in data['hits']['hits']:
        yield observation


def stats(conn, benchmark_ids=None):
    for benchmark in benchmarks(_get_benchmarks(conn), ids=benchmark_ids):
        for observation in observations(_get_observations(conn, benchmark['_id'])):
            # each observation contains stats groups - here referred to as
            # 'groups' which record information on each of the queries which
            # form part of the benchmark. a stats group in context (number of
            # doucments in the index, benchmark info) forms the basic unit of
            # measured data
            groups = observation['_source']['stats']['search']['groups']
            for name, group in groups.items():
                yield benchmark, observation, (name, group)


StatRecord = collections.namedtuple('StatRecord', [
        'bench_id',
        'bench_name',
        'obs_id',
        'obs_no',
        'doc_cnt',
        'seg_cnt',
        'size_b',
        't_index_ms',
        'query_name',
        'n_query',
        't_query_ms',
        'n_fetch',
        't_fetch_ms',
        'n_client',
        't_client_ms',
    ]
)


def stat_tuple(benchmark, observation, stat):
    stat_name, stat_data = stat
    record = StatRecord(
            bench_id=benchmark['_id'],
            bench_name=benchmark['_source'].get('benchmark_name', 'unknown'),
            obs_id=observation['_id'],
            obs_no=observation['_source']['meta']['observation_sequence_no'],
            doc_cnt=observation['_source']['stats']['docs']['count'],
            seg_cnt=observation['_source']['segments']['num_search_segments'],
            size_b=observation['_source']['stats']['store']['size_in_bytes'],
            t_index_ms=observation['_source']['stats']['indexing']['index_time_in_millis'],
            query_name=stat_name,
            n_query=stat_data['query_total'],
            n_fetch=stat_data['fetch_total'],
            n_client=stat_data['client_total'],
            t_query_ms=stat_data['query_time_in_millis'],
            t_fetch_ms=stat_data['fetch_time_in_millis'],
            t_client_ms=stat_data['client_time_in_millis'],
    )
    return record


def get_stat_tuples(conn, benchmark_ids=None, sort_f=lambda stat: (stat.bench_id, stat.query_name, stat.obs_no)):
    # set sort_f to None to not sort
    data = [stat_tuple(benchmark, observation, stat) for benchmark, observation, stat in stats(conn, benchmark_ids)]
    data = sorted(data, key=sort_f)
    return data



def show_benchmarks(conn, benchmark_ids=None, sample=1, format='tab', indent=4):
    data = get_stat_tuples(conn, benchmark_ids)
    if data:
        print(tabulate.tabulate(data, headers=data[0]._fields))


def dump_benchmarks(conn, ids=None):
    """Dump benchmark data as a sequence of curl calls.

    You can save these calls to a file, and then replay them somewhere else.
    """

    for benchmark in benchmarks(conn, ids):
        curl = """curl -XPUT 'http://localhost:9200/stats/bench/%s' -d '%s'""" % (benchmark['_id'], json.dumps(benchmark['_source']))
        print(curl)
        for o in observations(conn, benchmark['_id']):
            curl = """curl -XPUT 'http://localhost:9200/stats/obs/%s' -d '%s'""" % (o['_id'], json.dumps(o['_source']))
            print(curl)
    return


def delete_benchmarks(conn, ids=None):
    if not ids:
        path = "stats"
        resp = conn.delete(path)
        logger.info(resp.curl)
    else:
        for benchmark in benchmarks(conn, ids):
            for o in observations(conn, benchmark['_id']):
                path = "stats/obs/%s" % (o['_id'], )
                conn.delete(path)
            path = "stats/bench/%s" % (benchmark['_id'], )
            conn.delete(path)
    return

