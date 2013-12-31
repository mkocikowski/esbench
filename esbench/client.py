# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


import argparse
import itertools
import logging
import contextlib
import sys
import os.path
import socket

import esbench.api
import esbench.analyze
import esbench.bench


logger = logging.getLogger(__name__)

def args_parser():

    epilog = ""

    parser = argparse.ArgumentParser(description="Elasticsearch benchmark runner (%s)" % (esbench.__version__, ), epilog=epilog)
    parser.add_argument('-v', '--version', action='version', version=esbench.__version__)
    subparsers = parser.add_subparsers(dest='command', title='commands')

    parser_run = subparsers.add_parser('run', help='run a benchmark')
    parser_run.add_argument('-v', '--verbose', action='store_true')
    parser_run.add_argument('--observations', metavar='N', type=int, default=10, help='run n observations; (%(default)i)')
    parser_run.add_argument('--segments', type=int, metavar='N', default=None, help='max_num_segments for optimize calls; (%(default)s)')
    parser_run.add_argument('--repetitions', metavar='N', type=int, default=100, help='run each query n times per observation; (%(default)i)')
    parser_run.add_argument('--no-load', action='store_true', help="if set, do not load data, just run observations")
    parser_run.add_argument('--no-optimize-calls', action='store_true', help="if set, do not optimize before observations")
    parser_run.add_argument('--record-segments', action='store_true', help="if set, record detailed per-segment stats")
    parser_run.add_argument('--config-file-path', metavar='', type=str, default='%s/config.json' % (os.path.dirname(os.path.abspath(__file__)), ), help="path to json config file; (%(default)s)")
    parser_run.add_argument('--name', type=str, action='store', default="%s::%s" % (socket.gethostname(), esbench.bench.timestamp()), help="human readable name of the benchmark; (%(default)s)")
    parser_run.add_argument('--append', action='store_true', help="if set, append data to the index; (%(default)s)")
    parser_run.add_argument('--data', metavar='PATH', type=str, action='store', default=None, help="read data from PATH; set to /dev/stdin to read from stdin. Set this only if you want to provide your own data, by default US Patent Application data will be used; (%(default)s)")
    parser_run.add_argument('maxsize', nargs="?", type=str, default='1mb', help="max size of the index, as either the number of documents or byte size. To index 100 documents, set it to 100; to index 1gb of documents, set it to 1gb. When setting the byte size of data, best effort will be made to run observations at even intervals, and the index bytesize will be ballpark, not the exact figure you specified. The default USPTO Patent Application data set has 123GB of data / 2.5m documents, so if you want more, you'll need to look elsewhere (or feed the same data in more than once); (%(default)s)")

    parser_show = subparsers.add_parser('show', help='show data from recorded benchmarks')
    parser_show.add_argument('-v', '--verbose', action='store_true')
    parser_show.add_argument('--sample', metavar='N', type=int, default=1, help='sample every Nth observation; (%(default)i)')
    parser_show.add_argument('--format', choices=['tab', 'json', 'csv', 'svg'], default='tab', help='(%(default)s)')
    parser_show.add_argument('ids', nargs='*')

    parser_clear = subparsers.add_parser('clear', help='clear recorded benchmarks')
    parser_clear.add_argument('-v', '--verbose', action='store_true')
    parser_clear.add_argument('ids', nargs='*')

    parser_dump = subparsers.add_parser('dump', help='curl dump recorded benchmarks')
    parser_dump.add_argument('-v', '--verbose', action='store_true')
    parser_dump.add_argument('ids', nargs='*')

    return parser


def parse_maxsize(value):

    max_n = 0
    max_byte_size = 1 << 20
    try:
        max_n = int(value.strip())
        max_byte_size = 0
    except ValueError:
        max_n = 0
        orders = {'kb': 10, 'mb': 20, 'gb': 30, 'tb': 40}
        max_byte_size = int(value[:-2]) << orders[value[-2:].lower()]
    logger.debug("Parsed maxsize; max_n: %i, max_byte_size: %i", max_n, max_byte_size)
    return max_n, max_byte_size


def main():

    args = args_parser().parse_args()
    cmnd = " ".join(sys.argv[1:])

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(process)d %(name)s %(funcName)s:%(lineno)d %(levelname)s %(message)s')
    else:
        logging.basicConfig(level=logging.INFO)

    with esbench.api.connect() as conn:

        if args.command == 'run':
            benchmark = esbench.bench.Benchmark(cmnd=cmnd, argv=args, conn=conn, stats_index_name=esbench.STATS_INDEX_NAME)
            benchmark.prepare()
            if args.no_load:
                for _ in range(args.observations):
                    benchmark.observe()
            else:
                max_n, max_byte_size = parse_maxsize(args.maxsize)
                with esbench.data.feed(path=args.data) as feed:
                    batches = esbench.data.batches_iterator(lines=feed, batch_count=args.observations, max_n=max_n, max_byte_size=max_byte_size)
                    benchmark.run(batches)

            benchmark.record()

        elif args.command == 'show':
            esbench.analyze.show_benchmarks(conn, benchmark_ids=args.ids, sample=args.sample, format=args.format)

        elif args.command == 'dump':
            esbench.analyze.dump_benchmarks(conn, args.ids)

        elif args.command == 'clear':
            esbench.analyze.delete_benchmarks(conn, args.ids)


if __name__ == "__main__":
    main()

