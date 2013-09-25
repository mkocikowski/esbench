# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


import argparse
import itertools
import logging
import sys
import contextlib

import esbench.analyze
import esbench.bench


logger = logging.getLogger(__name__)

@contextlib.contextmanager
def get_lines_iterator(path=None, count=None):

    infile = None
    if path: 
        infile = open(path, 'rU')
        # if count is None, iterate through all elements
        lines = itertools.islice(infile, count)
    else:
        lines = itertools.islice(esbench.data.feed(), count)
    
    yield lines
    
    if infile:
        infile.close()



def args_parser():
    parser = argparse.ArgumentParser(description="Elasticsearch benchmark runner")
    parser.add_argument('--version', action='version', version=esbench.__version__)
    parser.add_argument('-v', '--verbose', action='store_true')
    subparsers = parser.add_subparsers(dest='command', title='commands')

    parser_run = subparsers.add_parser('run', help='run a benchmark')
    parser_run.add_argument('-v', '--verbose', action='store_true')
    parser_run.add_argument('--observations', metavar='N', type=int, default=10, help='run n observations; (%(default)i)')
    parser_run.add_argument('--segments', type=int, metavar='N', default=None, help='max_num_segments for optimize calls; (%(default)s)')
    parser_run.add_argument('--refresh', type=str, metavar='T', default='1s', help="'refresh_interval' for the index, '-1' for none; (%(default)s)")
    parser_run.add_argument('--no-optimize-calls', action='store_true', help="if set, do not optimize before observations")
    parser_run.add_argument('--config-file-path', metavar='', type=str, default='./config.json', help="path to json config file; (%(default)s)")
    parser_run.add_argument('--data', metavar='PATH', type=str, action='store', default=None, help="read data from PATH; set to /dev/stdin to read from stdin. Set this only if you want to provide your own data, by default US Patent Application data will be used; (%(default)s)")
    parser_run.add_argument('n', nargs="?", type=int, default=100, help='number of documents; (%(default)i)')

    parser_show = subparsers.add_parser('show', help='show data from recorded benchmarks')
    parser_show.add_argument('-v', '--verbose', action='store_true')
    parser_show.add_argument('--sample', metavar='N', type=int, default=1, help='sample every Nth observation; (%(default)i)')
    parser_show.add_argument('ids', nargs='*')

#     parser_list = subparsers.add_parser('list', help='list recorded benchmarks')
#     parser_list.add_argument('-v', '--verbose', action='store_true')
#     parser_list.add_argument('ids', nargs='*', help='optional list of benchmark ids, empty list means all; (%(default)s)')

    parser_clear = subparsers.add_parser('clear', help='clear recorded benchmarks')
    parser_clear.add_argument('-v', '--verbose', action='store_true')
    parser_clear.add_argument('ids', nargs='*')

    parser_dump = subparsers.add_parser('dump', help='curl dump recorded benchmarks')
    parser_dump.add_argument('-v', '--verbose', action='store_true')
    parser_dump.add_argument('ids', nargs='*')

    return parser


def main():

    args = args_parser().parse_args()
    
    loglevel = logging.INFO if not args.verbose else logging.DEBUG
    logging.basicConfig(level=loglevel)

    with esbench.bench.connect() as conn: 

        if args.command == 'run':         
            benchmark = esbench.bench.Benchmark(args)
            try: 
                benchmark.prepare(conn)
                with get_lines_iterator(args.data, args.n) as lines: 
                    benchmark.run(conn, lines)            
            except Exception as exc:
                logger.error(exc, exc_info=True)
                raise
            finally:
                benchmark.record(conn)

#         elif args.command == 'list': 
#             esbench.analyze.list_benchmarks(conn, args.ids)
            
        elif args.command == 'show': 
            esbench.analyze.analyze_benchmarks(conn, ids=args.ids, step=args.sample)
            
        elif args.command == 'dump':
            esbench.analyze.dump_benchmarks(conn, args.ids)
            
        elif args.command == 'clear': 
            esbench.analyze.delete_benchmarks(conn, args.ids)


if __name__ == "__main__":
    main()

