# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import datetime
import os.path
import unittest
import json

import esbench.client


class ClientTest(unittest.TestCase):

    def test_args(self):
        parser = esbench.client.args_parser()
        self.assertRaises(SystemExit, parser.parse_args, "".split())
        args = parser.parse_args("run".split())
        self.assertEqual(args.__dict__, 
            {
                'no_optimize_calls': False, 
                'verbose': False, 
                'segments': None, 
                'repetitions': 1000, 
                'n': 100, 
                'command': 'run', 
                'observations': 10, 
                'data': None, 
                'append': False, 
                'config_file_path': os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../", "config.json")),
            }
        )
    

        
if __name__ == "__main__":
    unittest.main()     

