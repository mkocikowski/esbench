# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import itertools
import logging
import json
import collections

import tabulate


logger = logging.getLogger(__name__)

def benchmarks(conn, ids=None): 
    path = "stats/bench/_search?sort=benchmark_start:asc&size=100"
    resp = conn.get(path)
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
    

def observations(conn, benchmark_id): 
    path = "stats/obs/_search?q=meta.benchmark_id:%s&sort=meta.observation_start:asc&size=10000" % (benchmark_id, )
    resp = conn.get(path)
    data = json.loads(resp.data)
    for observation in data['hits']['hits']: 
        yield observation


def stats(conn, benchmark_ids=None):
    for benchmark in benchmarks(conn, benchmark_ids): 
        for observation in observations(conn, benchmark['_id']): 
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
        't_index', 
        'stat_name', 
        't_query', 
        't_fetch', 
        't_client', 
    ]
)
    
def stat_tuple(benchmark, observation, stat): 
    record = StatRecord(
            bench_id=benchmark['_id'], 
            bench_name=benchmark['_source'].get('benchmark_name', 'unknown'), 
            obs_id=observation['_id'], 
            obs_no=observation['_source']['meta']['observation_sequence_no'],
            doc_cnt=observation['_source']['stats']['docs']['count'], 
            seg_cnt=observation['_source']['segments']['num_search_segments'], 
            t_index=observation['_source']['stats']['indexing']['index_time_in_millis'],
            stat_name=stat[0], 
            t_query=float(stat[1]['query_time_in_millis'])/float(stat[1]['query_total']), 
            t_fetch=float(stat[1]['fetch_time_in_millis'])/float(stat[1]['fetch_total']),
            t_client=float(stat[1]['client_time_in_millis'])/float(stat[1]['client_total']),
    )
    return record

def show_benchmarks(conn, ids=None, sample=1, format='JSON', indent=4):
    data = [stat_tuple(benchmark, observation, stat) for benchmark, observation, stat in stats(conn, ids)]
    data = sorted(data, key=lambda stat: (stat.bench_id, stat.stat_name, stat.obs_no))
    if data: 
        print(tabulate.tabulate(data, headers=data[0]._fields))
#     print(tabulate.tabulate(data, headers='keys'))
    


def dump_benchmarks(conn, ids=None): 
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

