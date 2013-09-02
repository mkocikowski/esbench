# -*- coding: UTF-8 -*-

import os.path
import logging
import argparse
import sys
# import socket

# import urlparse
# import httplib
import urllib2
# import tempfile
import gzip

# import requests


__version__ = "0.0.1"

logger = logging.getLogger(__name__)


URL = "https://s3-us-west-1.amazonaws.com/esbench/assn_%02i.json.gz"

def urls(count=1):
    for n in range(count):
        yield (URL % (n+1))


def download(url): 
    
    fn = os.path.basename(url)
    fn = os.path.abspath(fn)
    if os.path.exists(fn): 
        return fn
    
    resp = urllib2.urlopen(url)
    
    with open(fn, 'w') as f:
        chunk = resp.read(2**16)
        while chunk:
            f.write(chunk)
            chunk = resp.read(2**16)
            sys.stderr.write(".")

    resp.close()
    return fn


def unzip(fn): 

    with gzip.open(fn, 'rb') as f: 
        for line in f:
            yield(line.strip())
            

def feed(nocache=False): 

    for url in urls():
        fn = download(url)
        try: 
            for line in unzip(fn): 
                yield line 
        finally:
            if nocache:
                os.remove(fn)


def args_parser():
    parser = argparse.ArgumentParser(description="esbench USPTO patent assignment downloader.")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('--nocache', action='store_true', help="if set, delete downloaded data (default: %(default)s)")
    return parser


def main():

    logging.basicConfig(level=logging.WARNING)
    args = args_parser().parse_args()

    try: 
        for line in feed(nocache=args.nocache):
            print(line) 
        sys.exit(0)
    
    except IOError:
        pass
    

if __name__ == "__main__":
    main()

