Elasticsearch benchmarking tools
--------------------------------

This is not fit for public consumption yet! I'm putting this on github for my
own convenience, not to share with people yet. 

Installation
------------

    pip install https://github.com/mkocikowski/esbench/archive/dev.zip

If you don't want to use pip (which is a nice python package manager /
installer), you can download the source, cd to the directory which contains
the 'setup.py' file, and run 'python setup.py install'. But I recommend using
'pip'.

Running
-------
With an instance of elasticsearch running on localhost, do: 

	esbench run
	
When the bench run has finished, you can review the results with: 

	esbench show


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
    
