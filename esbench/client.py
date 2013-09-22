import argparse
import itertools
import logging
import sys

import esbench.analyze
import esbench.bench


__version__ = "0.0.4"

def args_parser():
    parser = argparse.ArgumentParser(description="Elasticsearch benchmark runner")
    parser.add_argument('-v', '--version', action='version', version=__version__)
#     parser.add_argument('--verbose', action='store_true')
    subparsers = parser.add_subparsers(dest='command', title='commands')

    parser_run = subparsers.add_parser('run', help='run a benchmark')
    parser_run.add_argument('--observations', metavar='N', type=int, default=10, help='run n observations; (%(default)i)')
    parser_run.add_argument('--segments', type=int, metavar='N', default=None, help='max_num_segments for optimize calls; (%(default)s)')
    parser_run.add_argument('--refresh', type=str, metavar='T', default='1s', help="'refresh_interval' for the index, '-1' for none; (%(default)s)")
    parser_run.add_argument('--no-optimize-calls', action='store_true', help="if set, do not optimize before observations")
#     parser.add_argument('--clear-all-results', action='store_true', help="if set, clear all benchmark data from the index")
    parser_run.add_argument('--config-file-path', metavar='', type=str, default='./config.json', help="path to json config file; (%(default)s)")
    parser_run.add_argument('n', nargs="?", type=int, default=100, help='number of documents; (%(default)i)')

    parser_dump = subparsers.add_parser('show', help='show data from recorded benchmarks')
    parser_dump.add_argument('ids', nargs='*')

    parser_list = subparsers.add_parser('list', help='list recorded benchmarks')
    parser_list.add_argument('ids', nargs='*')

    parser_clear = subparsers.add_parser('clear', help='clear recorded benchmarks')
    parser_clear.add_argument('ids', nargs='*')

    parser_dump = subparsers.add_parser('dump', help='curl dump recorded benchmarks')
    parser_dump.add_argument('ids', nargs='*')



    return parser


def main():

    logging.basicConfig(level=logging.DEBUG)
    args = args_parser().parse_args()

#     print(args)
#     sys.exit(0)

    with esbench.bench.connect() as conn: 

        if args.command == 'run': 

            lines = itertools.islice(esbench.data.feed(), args.n)
            benchmark = esbench.bench.Benchmark(args)

            try: 
                benchmark.prepare(conn)
                benchmark.run(conn, lines)
            
            except Exception as exc:
                logger.error(exc, exc_info=True)
                raise
            
            finally:
                benchmark.record(conn)

        elif args.command == 'list': 
            esbench.analyze.list_benchmarks(conn, args.ids)
        elif args.command == 'show': 
            esbench.analyze.analyze_benchmarks(conn, args.ids)
        elif args.command == 'dump':
            esbench.analyze.dump_benchmarks(conn, args.ids)
        elif args.command == 'clear': 
            esbench.analyze.delete_benchmarks(conn, args.ids)


if __name__ == "__main__":
    main()

