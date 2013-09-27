# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import itertools
import logging
import json


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
        logger.error("no benchmarks found", exc_info=True)
    return
    

def observations(conn, benchmark_id): 
    path = "stats/obs/_search?q=meta.benchmark_id:%s&sort=meta.observation_start:asc&size=10000" % (benchmark_id, )
    resp = conn.get(path)
    data = json.loads(resp.data)
    for observation in data['hits']['hits']: 
        yield observation


def analyze_benchmarks(conn, ids=None, step=1): 
    for benchmark in benchmarks(conn, ids): 
        seg_max = benchmark['_source']['argv']['segments'] if benchmark['_source']['argv']['segments'] else 'inf'
        obs_i = itertools.islice(observations(conn, benchmark['_id']), 0, None, step)
        r = [(
            d['_source']['stats']['docs']['count'],
            " ".join(["%s:%6sq%6sf%6sc" % (k, v['query_time_in_millis'], v['fetch_time_in_millis'], v['client_time_in_millis']) for k, v in sorted(d['_source']['stats']['search']['groups'].items())]), 
            d['_source']['segments']['num_search_segments'],
            seg_max,
            d['_source']['stats']['store']['size'],  
            d['_source']['segments']['t_optimize_in_millis'] / 1000.0 if d['_source']['segments']['t_optimize_in_millis'] else -1.0, 
            ) for d in obs_i]
        print("\nBenchmark: %s, start: %s, total: %s \n" % (benchmark['_id'], benchmark['_source']['benchmark_start'], benchmark['_source']['t_total'],))
        for t in r:
            print("N: %-6i %s SEG/MAX: %3i/%s SIZE: %8s OPT: %6.2f" % t)
        print("")
    return


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
    
