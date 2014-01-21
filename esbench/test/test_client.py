# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import datetime
import os.path
import unittest
import json
import contextlib
import types
import collections
import sys
import StringIO

import esbench.client


class ClientTest(unittest.TestCase):

    def setUp(self):
        # this is needed to supress output from argparse '-h'
        self.tmp = sys.stdout
        sys.stdout = StringIO.StringIO()

    def tearDown(self):
        sys.stdout = self.tmp

    def test_args_run(self):

        parser = esbench.client.args_parser()
        args = parser.parse_args("run".split())
        self.assertEqual(args.__dict__,
            {
                'verbose': False,
                'segments': None,
                'reps': None,
                'maxsize': '1mb',
                'name': args.name, # cheating, but no clean way around it as it contains timestamp
                'no_load': False,
                'command': 'run',
                'observations': None,
                'data': None,
                'append': False,
                'config_file_path': os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../", "config.json")),
                'host': 'localhost',
                'port': 9200,
                'shards': None,
            }
        )

        # running with -h flag will catch some errors
        self.assertRaises(SystemExit, parser.parse_args, "run -h".split())


    def test_args_show(self):

        parser = esbench.client.args_parser()
        args = parser.parse_args("show".split())
        self.assertEqual(args.__dict__,
            {
                'command': 'show',
                'fields': '(?!observation.segments.segments)((benchmark.meta.benchmark_start)|(observation.meta.benchmark_id)|(observation.meta.observation_id)|(observation.meta.observation_sequence_no)|(observation.segments.num_committed_segments)|(observation.segments.num_search_segments)|(observation.segments.t_optimize_in_millis)|(observation.stats.docs.count)|(observation.stats.store.size_in_bytes)|(observation.stats.fielddata.memory_size_in_bytes)|(observation.stats.search.groups.*query_time_in_millis_per_query$))',
                'host': 'localhost',
                'port': 9200,
                'format': 'csv',
                'verbose': False,
                'ids': ['all'],
            }
        )

        # running with -h flag will catch some errors
        self.assertRaises(SystemExit, parser.parse_args, "show -h".split())



    def test_parse_maxsize(self):

        self.assertRaises(AttributeError, esbench.client.parse_maxsize, (10,))
        self.assertEqual((10, 0), esbench.client.parse_maxsize('10'))
        self.assertEqual((0, 1<<10), esbench.client.parse_maxsize('1kb'))
        self.assertEqual((0, 1<<20), esbench.client.parse_maxsize('1mb'))


if __name__ == "__main__":
    unittest.main()

