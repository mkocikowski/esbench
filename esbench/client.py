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
import json

import esbench.api
import esbench.analyze
import esbench.bench


logger = logging.getLogger(__name__)

def args_parser():

    epilog = """
To get help for a command, do 'esbench command -h'

Other commands:
curl -XDELETE localhost:9200/esbench_* # delete existing benchmarks
\t
"""

    parser = argparse.ArgumentParser(description="Elasticsearch benchmark runner (%s)" % (esbench.__version__, ), epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-v', '--version', action='version', version=esbench.__version__)
    subparsers = parser.add_subparsers(dest='command', title='commands')

    epilog_run = """
"""

    parser_run = subparsers.add_parser('run', help='run a benchmark', epilog=epilog_run, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser_run.add_argument('-v', '--verbose', action='store_true')
    parser_run.add_argument('--host', type=str, default='localhost', help='elasticsearch host; (%(default)s)')
    parser_run.add_argument('--port', type=int, default=9200, help='elasticsearch port; (%(default)s)')

    parser_run.add_argument('--segments', type=int, metavar='N', default=None, help='if set, run optimize before each observation')
    parser_run.add_argument('--shards', metavar='N', action='store', type=int, help="create test index with N primaries")
    parser_run.add_argument('--observations', metavar='N', type=int, default=None, help='run n observations')
    parser_run.add_argument('--reps', metavar='N', type=int, default=None, help='run each query n times per observation')

    parser_run.add_argument('--no-load', action='store_true', help="if set, do not load data, just run observations")
    parser_run.add_argument('--append', action='store_true', help="if set, append data to the index; (%(default)s)")
    parser_run.add_argument('--data', metavar='PATH', type=str, action='store', default=None, help="read data from PATH; set to /dev/stdin to read from stdin. Set this only if you want to provide your own data, by default US Patent Application data will be used; (%(default)s)")

    parser_run.add_argument('--config-file-path', metavar='', type=str, default='%s/config.json' % (os.path.dirname(os.path.abspath(__file__)), ), help="path to json config file; (%(default)s)")
    parser_run.add_argument('--name', type=str, action='store', default="%s::%s" % (socket.gethostname(), esbench.bench.timestamp()), help="human readable name of the benchmark; (%(default)s)")
    parser_run.add_argument('maxsize', nargs="?", type=str, default='1mb', help="max size of the index, as either the number of documents or byte size. To index 100 documents, set it to 100; to index 1gb of documents, set it to 1gb. When setting the byte size of data, best effort will be made to run observations at even intervals, and the index bytesize will be ballpark, not the exact figure you specified. The default USPTO Patent Application data set has 123GB of data / 2.5m documents, so if you want more, you'll need to look elsewhere (or feed the same data in more than once); (%(default)s)")

    epilog_show = """
Sample use:

# output tabulated, all benchmarks, default fields, to stdout:
esbench show

# write csv-formatted data for benchmark bd97da35 to file foo.csv:
esbench show bd97da35 > foo.csv

# see fieldnames for default setting of '--fields':
esbench show | head -1 | tr '\\t' '\\n'

# see all possible fieldnames:
esbench show --fields '.*' | head -1 | tr '\\t' '\\n'

# plot optimize time vs data size in gnuplot and open resulting graph in google chrome (on osx):
esbench show > /tmp/esbench.csv && gnuplot -e "set terminal svg size 1000, 1000; set xlabel 'observation number'; plot '/tmp/esbench.csv' using 4:5 with fsteps title columnheader, '' using 4:(column(7)/(1000)) with fsteps title 'observation.segments.t_optimize_in_millis (SECONDS)'" > /tmp/esbench.svg && open -a 'Google Chrome' '/tmp/esbench.svg'

# plot basic data in gnuplot and open resulting graph in google chrome (on osx):
esbench show > /tmp/esbench.csv && gnuplot -e "set terminal svg size 1000, 1000; set xlabel 'observation number'; plot for [col=10:12] '/tmp/esbench.csv' using 4:col with lines lw 3 title columnheader, '' using 4:5 with fsteps title columnheader, '' using 4:6 with fsteps title columnheader, '' using 4:(column(9)/(2**20)) with fsteps title 'observation.stats.fielddata.memory_size_in_bytes (MB)', '' using 4:(column(13)/(2**30)) with fsteps title 'observation.stats.store.size_in_bytes (GB)'" > /tmp/esbench.svg && open -a 'Google Chrome' '/tmp/esbench.svg'

In general, if the 'canned' fields / graph do not meet your needs, dump all
the fields into a csv and graph /analyze it with whatever you have.
\t
"""


    parser_show = subparsers.add_parser('show', help='show data from recorded benchmarks', epilog=epilog_show, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser_show.add_argument('-v', '--verbose', action='store_true')
    parser_show.add_argument('--host', type=str, default='localhost', help='elasticsearch host; (%(default)s)')
    parser_show.add_argument('--port', type=int, default=9200, help='elasticsearch port; (%(default)s)')
    parser_show.add_argument('--format', choices=['csv', 'tab'], default='csv', help="output format; (%(default)s)")
    parser_show.add_argument('--fields', metavar='REGEX', type=str, action='store', default=esbench.analyze.FIELDS, help='default: %(default)s')
    parser_show.add_argument('ids', nargs='*', default=['all'], help='benchmark ids; (default: show all benchmarks)')

    parser_dump = subparsers.add_parser('dump', help='curl dump recorded benchmarks')
    parser_dump.add_argument('-v', '--verbose', action='store_true')
    parser_dump.add_argument('--host', type=str, default='localhost', help='elasticsearch host; (%(default)s)')
    parser_dump.add_argument('--port', type=int, default=9200, help='elasticsearch port; (%(default)s)')
    parser_dump.add_argument('ids', nargs='*', default=['all'], help='benchmark ids; (default: show all benchmarks)')

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


def load_config(path):

    c = None
    with open(path, 'rU') as f:
        c = json.loads(f.read())

    return c


def merge_config(argv, config):
    """Merge the config file with the command line arguments.

    Command line arguments override the config file parameters, unless they
    are None. If a command line argument exists which isn't defined in the
    'config' element of the config, it gets set regradless of its value (this
    includes None).

    """

    c = config['config']
    for k, v in argv.__dict__.items():
        if k in c and v is not None:
            c[k] = v
        elif k not in c:
            c[k] = v

    c['max_n'], c['max_byte_size'] = parse_maxsize(c['maxsize'])

    if argv.shards:
        config['index']['settings']['index']['number_of_shards'] = argv.shards

    return config


def main():

    args = args_parser().parse_args()

    if args.verbose: logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(name)s.%(funcName)s:%(lineno)d %(levelname)s %(message)s')
    else: logging.basicConfig(level=logging.INFO)

    with esbench.api.connect(host=args.host, port=args.port) as conn:

        try:

            if args.command == 'run':

                config = merge_config(args, load_config(args.config_file_path))
                benchmark = esbench.bench.Benchmark(config=config, conn=conn)
                benchmark.prepare()
                if config['config']['no_load']:
                    for _ in range(config['config']['observations']):
                        benchmark.observe()
                else:
                    with esbench.data.feed(path=config['config']['data']) as feed:
                        batches = esbench.data.batches_iterator(lines=feed, batch_count=config['config']['observations'], max_n=config['config']['max_n'], max_byte_size=config['config']['max_byte_size'])
                        benchmark.run(batches)

                benchmark.record()

            elif args.command == 'show':
                esbench.analyze.show_benchmarks(conn=conn, benchmark_ids=args.ids, fields=args.fields, fmt=args.format, fh=sys.stdout)

            elif args.command == 'dump':
                esbench.analyze.dump_benchmarks(conn=conn, benchmark_ids=args.ids)

        except IOError as exc:
            logger.debug(exc, exc_info=False)
        except Exception as exc:
            logger.error(exc, exc_info=True)


if __name__ == "__main__":
    main()

