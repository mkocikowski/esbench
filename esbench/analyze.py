# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)

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

import esbench.bench

logger = logging.getLogger(__name__)

conn = None
DEFAULT_TIMEOUT = 10


def benchmarks(conn, ids=None): 
    path = "stats/bench/_search?sort=time_start:asc&size=100"
    status, reason, data = conn.get(path)
    data = json.loads(data)
    for benchmark in data['hits']['hits']: 
        if ids and not benchmark['_id'] in ids: 
            continue
        else:
            yield benchmark
    return
    

def observations(conn, benchmark_id): 
    path = "stats/obs/_search?q=meta.benchmark_id:%s&sort=meta.observation_start:asc&size=10000" % (benchmark_id, )
    status, reason, data = conn.get(path)
    data = json.loads(data)
    for observation in data['hits']['hits']: 
        yield observation



def analyze_benchmarks(conn, ids=None, step=1): 
    for benchmark in benchmarks(conn, ids): 
        seg_max = benchmark['_source']['argv']['segments'] if benchmark['_source']['argv']['segments'] else 'inf'
        obs_i = itertools.islice(observations(conn, benchmark['_id']), 0, None, step)
        r = [(
            d['_source']['stats']['docs']['count'],
            d['_source']['stats']['search']['groups']['mlt']['query_time_in_millis'], 
            d['_source']['stats']['search']['groups']['match']['query_time_in_millis'], 
            d['_source']['stats']['search']['groups']['match_sorted']['query_time_in_millis'], 
            d['_source']['segments']['num_search_segments'],
            seg_max,
            d['_source']['stats']['store']['size'],  
            d['_source']['segments']['t_optimize_in_millis'] / 1000.0, 
            ) for d in obs_i]
        print("\nBenchmark: %s, start: %s, total: %s \n" % (benchmark['_id'], benchmark['_source']['time_start'], benchmark['_source']['time_total'],))
        print("%8s %7s %7s %7s %8s %12s %12s" % ('COUNT', 'MLT', 'MATCH', 'MS', 'SEG/MAX', 'SIZE', 'OPTIMIZE'))
        for t in r:
            print("%8i %7i %7i %7i %4i/%s %12s %9.2f" % t)
        print("")
    return


def list_benchmarks(conn, ids=None): 
    for benchmark in benchmarks(conn, ids): 
        print("http://localhost:9200/stats/bench/%s %s \n%s" % (benchmark['_id'], benchmark['_source']['time_start'], json.dumps(benchmark['_source']['argv'], indent=4)))
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
    for benchmark in benchmarks(conn, ids): 
        for o in observations(conn, benchmark['_id']): 
            path = "stats/obs/%s" % (o['_id'], )
            conn.delete(path)
        path = "stats/bench/%s" % (benchmark['_id'], )
        conn.delete(path)
    return
    
