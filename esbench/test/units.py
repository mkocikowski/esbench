# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


import sys
import os.path
import unittest


if __name__ == "__main__":

    suite = unittest.defaultTestLoader.discover(os.path.dirname(__file__), top_level_dir=os.path.abspath("../../"))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # doing sys.exit(1) on test failure will signal test failure to other
    # processes (this is for when the suite is run automatically, not by hand
    # from the command line)
    #
    if not result.wasSuccessful():
        sys.exit(1)


