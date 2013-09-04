# -*- coding: UTF-8 -*-

import datetime
import os.path
import unittest
import json

import esbench.data


class DataTest(unittest.TestCase):

    def test__aa(self):
        l = list(esbench.data._aa())
        self.assertEqual(len(l), 676)
        self.assertEqual(l[0], 'aa')
        self.assertEqual(l[-1], 'zz')
        l = list(esbench.data._aa(10))
        self.assertEqual(len(l), 10)
        self.assertEqual(l[0], 'aa')
        self.assertEqual(l[-1], 'aj')
        

        
if __name__ == "__main__":
    unittest.main()     

