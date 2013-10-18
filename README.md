WARNING
=======
This is not fit for public consumption yet! I'm putting this on github for my
own convenience, not to share with people yet.


Elasticsearch benchmarking tools
--------------------------------
'esbench' is a set of python 2.7 scripts for benchmarking Elasticsearch. The
two primary uses are for capacity planning (guessing how much oomph you need
to do what what you think you need to do), and for performance tuning (trying
out various index, mapping, and query settings in a consistent and
reproducible manner). 

Installation
------------

    pip install https://github.com/mkocikowski/esbench/archive/master.zip

Tests
-----
Once 'esbench' has been installed (no need for local Elasticsearch instance):

	python -m esbench.test.units

Quick start
-----------
With an instance of elasticsearch running on localhost:9200, do: 

	esbench run
	
When the bench run has finished, you can review the results with: 

	esbench show

To get help: 
	
	esbench --help
	esbench <command> --help

Overview
--------
An Elasticsearch index is composed of a set of 1 or more Lucene indexes
(designated as primary and replica 'shards' by ES). The Elasticsearch cluster
routes incoming documents and search requests to individual Lucene indexes /
shards as needed. A single Lucene index is the basic unit on which indexing
and search operations are executed, and so the performance of individual
Lucene indexes largely determines the performance of a cluster. Lucene indexes
store data on disk in append-only 'segments', which can be periodically
cleaned up and merged. The number and size (bytes) of these segments, and the
disk IO, ultimately determine the speed at which data can be retrieved. 

The basic approach to capacity planning then is to create an Elasticsearch
index with 1 primary and no replica shards (a single Lucene index), load it
with data, and run representative use patterns against it, adding more data
until the performance drops below acceptable levels. That will determine the
maximum viable size of a single shard given the data, use patterns, and
hardware. Total expected data size divided by the viable single index size
gives then the number of required shards. With a rule of thumb of no more than
2 shards per core, this allows for simple capacity planning. 

The 'run' command
-----------------
When the 'run' command is executed, a 'test' index is created on the target ES
host, populated with provided data, and queried at specified intervals. The
queries which will be executed against the index are provided in the
'config.json' file. Observations taken while the benchmark is running are
recorded in the 'stats' index on the ES host. Each benchmark results in one
document of type 'bench' and one or more documents of type 'obs' (observation)
added to the 'stats' index. 

When executed with default arguments, the 'run' command will result in a quick
(too small to be meaningful) benchmark run against the local ES instance,
using US Patent Application data downloaded from S3 (into /tmp, size 99MB).
The intention is to provide sample data to verify that the 'rig is working'
and to familiarize the user with observation data.

The first argument to play with is 'n', the only non-keyword parameter, which
specifies the total number of documents to be inserted into the test index.
Default value of 100, which results in an index of just 2.9MB, but allows the
benchmark to run in just few seconds. Running: 

	esbench run 100000 

will result in an index of 4.7GB, which starts to yield meaningful performance
information. Experimenting with queries, and the various parameters (try
adjusting the number of segments or optimize calls) will yield interesting
changes. Depending on the specified logging level, you will see information
somewhat like that when running the benchmark: 

	[...]
	INFO:esbench.bench:beginning observation no: 9, 2013-10-17T22:50:59Z
	INFO:esbench.bench:ran query 'match_abs' 100 times in 1.27s
	INFO:esbench.bench:ran query 'match' 100 times in 1.32s
	INFO:esbench.bench:ran query 'mlt' 100 times in 0.28s
	INFO:esbench.bench:ran query 'match_srt' 100 times in 1.39s
	INFO:esbench.bench:finished observation no: 9, id: 3f519b57, time: 4.260
	INFO:esbench.bench:recorded observation into: stats/obs/3f519b57
	[...]

As data is stored into the 'stats' index, you can access it raw (see the last
log line for the URL). Once you've done that, you'll see why you want to use
the 'show' command, described in the next section: 

	curl -XGET localhost:9200/stats/obs/3f519b57


The 'show' command
------------------
The 'show' command will retrieve previously recorded benchmark information
from an ES instance, and format it and display it as requested. Running it
without parameters will output tabular data for all benchmarks recorded on
localhost. 

The 'dump' command
------------------
Dump previously recorded benchmark data as a series of curl calls. These can
be replayed to populate a new index. 

The 'clear' command
-------------------
Deletes specified benchmarks and related observations. 

License
-------

The project uses [the MIT license](http://opensource.org/licenses/MIT):

    Copyright (c) 2013 Mikolaj Kocikowski
    
    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
    THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.
    
