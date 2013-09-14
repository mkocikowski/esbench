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

import bench

__version__ = "0.0.2"

logger = logging.getLogger(__name__)

conn = None
DEFAULT_TIMEOUT = 10


def benchmarks(conn): 
    path = "stats/bench/_search?sort=time_start:asc&size=100"
    status, reason, data = conn.get(path)
    data = json.loads(data)
    for benchmark in data['hits']['hits']: 
        yield benchmark
        

def observations(conn, benchmark_id): 

#     status, reason, data = conn.get("stats/obs/_count")
#     count = json.loads(data)['count']
#     for i in range(1, count+1): 
#         status, reason, data = conn.get("stats/obs/%s_%i" % (benchmark_id, i))
#         if status == 200: 
#             print(data)
#             yield json.loads(data)
#         else:
#             continue

    path = "stats/obs/_search?q=meta.benchmark_id:%s&sort=meta.observation_start:asc&size=10000" % (benchmark_id, )
    status, reason, data = conn.get(path)
    data = json.loads(data)
    for observation in data['hits']['hits']: 
        yield observation




def args_parser():
    parser = argparse.ArgumentParser(description="esbench runner.")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    return parser


def main():

    logging.basicConfig(level=logging.DEBUG)
    args = args_parser().parse_args()

    with bench.connect() as conn: 

        for benchmark in benchmarks(conn): 
            seg_max = benchmark['_source']['argv']['segments'] if benchmark['_source']['argv']['segments'] else 'inf'
            r = [(
                d['_source']['stats']['docs']['count'],
                d['_source']['stats']['search']['groups']['mlt']['query_time_in_millis'], 
                d['_source']['stats']['search']['groups']['match']['query_time_in_millis'], 
                d['_source']['segments']['num_search_segments'],
                seg_max, 
                d['_source']['segments']['t_optimize_in_millis'] / 1000.0, 
                ) for d in observations(conn, benchmark['_id'])]

            print("\nBenchmark: %s, start: %s, total: %s \n" % (benchmark['_id'], benchmark['_source']['time_start'], benchmark['_source']['time_total'],))
            print("%8s %7s %7s %8s %12s" % ('COUNT', 'MLT', 'MATCH', 'SEG/MAX', 'OPTIMIZE'))
            for t in r:
                print("%8i %7i %7i %4i/%s %9.2f" % t)
            print("")
        


if __name__ == "__main__":
    main()

