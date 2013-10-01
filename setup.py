# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench


# python setup.py -r http://testpypi.python.org/pypi register
# python setup.py sdist upload -r http://pypi.python.org/pypi

# import ez_setup
# ez_setup.use_setuptools()
# 
from setuptools import setup

ld = """Tool for benchmarking Elasticsearch nodes. 
"""

setup(
    name = 'esbench', 
    version = '0.0.5', 
    author = 'Mik Kocikowski', 
    author_email = 'mkocikowski@gmail.com', 
    url = 'https://github.com/mkocikowski/esbench', 
    description = 'Elasticsearch benchmarking tool', 
    long_description = ld,
    packages = ['esbench', 'esbench.test'], 
    package_data = {
        '': ['README.md'], 
        'esbench': ['config.json'], 
    },
    entry_points = {
        'console_scripts': [
            'esbench = esbench.client:main', 
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
