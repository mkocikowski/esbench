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
import tempfile
import time
import copy

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


class TestConfig(unittest.TestCase):

    def setUp(self):
        self.tf = tempfile.NamedTemporaryFile()
        self.base_config = {
            "index": {
                "settings": {
                    "index": {
                        "number_of_replicas": 0,
                        "number_of_shards": 1,
                    }
                }
            },
            "config": {
                "observations": 10,
                "segments": None,
                "reps": 100
            }
        }
        self.tf.write(json.dumps(self.base_config))
        self.tf.flush()
#         self.argv = esbench.client.args_parser().parse_args("run")
        self.maxDiff = None

    def tearDown(self):
        self.tf.close()

    def test_load_config(self):
        c = esbench.client.load_config(self.tf.name)
        self.assertEqual(c, self.base_config)

    def test_merge_config(self):

        argv = esbench.client.args_parser().parse_args("run".split())
        c = esbench.client.merge_config(argv=argv, config=copy.deepcopy(self.base_config))
        c['config']['name'] = None # need to clear it as it is a timestamp, will fail tests, changes at every test run
        c['config']['config_file_path'] = None # ditto
        self.assertEqual(c, {
                'index': {
                    'settings': {
                        'index': {
                            'number_of_replicas': 0,
                            'number_of_shards': 1
                        }
                    }
                },
                'config': {
                    'verbose': False,
                    'segments': None,
                    'reps': 100,
                    'shards': None,
                    'maxsize': '1mb',
                    'no_load': False,
                    'command': 'run',
                    'observations': 10,
                    'host': 'localhost',
                    'config_file_path': None,
                    'data': None,
                    'port': 9200,
                    'append': False,
                    'name': None,
                    'max_byte_size': 1048576,
                    'max_n': 0
                }
            }
        )

        argv = esbench.client.args_parser().parse_args("run --observations 100 --shards 2 100mb".split())
        c = esbench.client.merge_config(argv=argv, config=copy.deepcopy(self.base_config))
        self.assertEqual(c['config']['observations'], 100)
        self.assertEqual(c['config']['shards'], 2)
        self.assertEqual(c['index']['settings']['index']['number_of_shards'], 2)
        self.assertEqual(c['config']['maxsize'], '100mb')
        self.assertEqual(c['config']['max_byte_size'], 104857600)
        self.assertEqual(c['config']['max_n'], 0)

        argv = esbench.client.args_parser().parse_args("run --observations 100 --no-load --shards 2 100".split())
        c = esbench.client.merge_config(argv=argv, config=copy.deepcopy(self.base_config))
        self.assertEqual(c['config']['max_byte_size'], 0)
        self.assertEqual(c['config']['max_n'], 100)
        self.assertEqual(c['config']['no_load'], True)


if __name__ == "__main__":
    unittest.main()

