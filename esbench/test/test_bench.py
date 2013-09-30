# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import datetime
import os.path
import unittest
import json

import esbench.bench

class BenchTest(unittest.TestCase):

    def test_rands(self):
        s = esbench.bench.rands()
        self.assertIsInstance(s, str)
        self.assertEqual(len(s), 6)
        self.assertNotEqual(esbench.bench.rands(), esbench.bench.rands())
    

        
if __name__ == "__main__":
    unittest.main()     

