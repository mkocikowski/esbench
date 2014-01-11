# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


# python setup.py -r http://testpypi.python.org/pypi register
# python setup.py sdist upload -r http://pypi.python.org/pypi

# import ez_setup
# ez_setup.use_setuptools()
#
from setuptools import setup

ld = """Tool for benchmarking performance of Elasticsearch nodes.

The two primary uses are for capacity planning (guessing how much oomph you
need to do what what you think you need to do), and for performance tuning
(trying out various index, mapping, and query settings in a consistent and
reproducible manner).

An Elasticsearch index is composed of a set of 1 or more Lucene indexes
(designated as primary and replica 'shards' by ES). A single Lucene index is
the basic unit on which indexing and search operations are executed, and so
the performance of individual Lucene indexes largely determines the
performance of a cluster.

The basic approach is to create an index with 1 primary and no replica shards
(a single Lucene index), load it with data, and periodically run
representative use patterns against it, recording observations, adding more
data until the performance drops below acceptable levels.

This tool comes with 'batteries included' (ie large sample data set,
downloaded on demand). See the README.md file, or even better, the project's
github page.

"""

setup(
    name = 'esbench',
    version = '0.1.1',
    author = 'Mik Kocikowski',
    author_email = 'mkocikowski@gmail.com',
    url = 'https://github.com/mkocikowski/esbench',
    description = 'Elasticsearch performance benchmark tool',
    long_description = ld,
    install_requires = ['tabulate >= 0.6', ],
    packages = ['esbench', 'esbench.test'],
    package_data = {
        '': ['README.md'],
        'esbench': ['config.json'],
    },
    entry_points = {
        'console_scripts': [
            'esbench = esbench.client:main', 
#             'elasticsearch_bench = esbench.client:main',
        ]
    },
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: POSIX",
        "Topic :: System :: Benchmark",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
        "Topic :: Utilities",
    ],
    license = 'MIT',
    test_suite = "esbench.test.units.suite",
)
