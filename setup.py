# -*- coding: utf-8 -*-

# python setup.py -r http://testpypi.python.org/pypi register
# python setup.py sdist upload -r http://pypi.python.org/pypi

from setuptools import setup

ld = """
"""

setup(
    name = 'esbench', 
    version = '0.0.2', 
    author = 'Mik Kocikowski', 
    author_email = 'mkocikowski@gmail.com', 
    url = 'https://github.com/mkocikowski/esbench', 
    description = 'Elasticsearch benchmarking tool', 
    long_description=ld,
    packages = ['esbench', 'esbench.test'], 
    package_data={'': ['README.md',],},
#     entry_points = {
#         'console_scripts': [
#             'elsec = elsec.client:main', 
#         ]
#     }, 
#     classifiers = [
#         "Development Status :: 5 - Production/Stable", 
#         "Environment :: Console", 
#         "Intended Audience :: Developers", 
#         "Intended Audience :: End Users/Desktop", 
#         "License :: OSI Approved :: MIT License", 
#         "Natural Language :: English", 
#         "Operating System :: POSIX",
#         "Topic :: Internet :: WWW/HTTP :: Indexing/Search",  
#         "Topic :: Utilities",
#     ], 
    license = 'MIT',
#     test_suite = "elsec.test.units.suite", 
)
