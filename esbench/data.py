# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import os.path
import logging
import argparse
import sys
import urllib2
import gzip
import itertools
import string
import contextlib
import collections

import esbench

logger = logging.getLogger(__name__)

# URL = "https://s3-us-west-1.amazonaws.com/esbench/assn_%s.gz"
URL = "https://s3-us-west-1.amazonaws.com/esbench/appl_%s.gz"

def _aa(count=None):
    i = ("".join(i) for i in itertools.product(string.lowercase, repeat=2))
    if count:
        i = itertools.islice(i, count)
    return i


def urls(count=None):
    for s in _aa(count):
        yield (URL % s)


def download(url, tmpd="/tmp"):

    fn = os.path.basename(url)
    fn = os.path.abspath(os.path.join(tmpd, fn))

    logger.info("Downloading '%s' to '%s'", url, fn)

    # if the file already exists, don't download it again
    if os.path.exists(fn):
        logger.info("Using cached file '%s'", fn)
        return fn

    resp = urllib2.urlopen(url)

    with open(fn, 'w') as f:
        chunk = resp.read(2**16)
        while chunk:
            f.write(chunk)
            chunk = resp.read(2**16)
            sys.stderr.write(".")

    logger.info("finished downloading '%s'", fn)

    resp.close()
    return fn


def unzip(fn):

    with gzip.open(fn, 'rb') as f:
        for line in f:
            yield(line.strip())


def get_data(nocache=False):

    for url in urls():
        fn = download(url)
        try:
            for line in unzip(fn):
                yield line
        except IOError:
            logger.error("IOError reading file: '%s'. Looks like the cached data file is corrupted, it will now be removed, and downloaded again on the next test run. Moving on to the next data file - this error will not affect the test run.", fn)
            nocache = True # this will remove the file in finally clause
        finally:
            if nocache:
                os.remove(fn)
                logger.info("removed file '%s'", fn)


@contextlib.contextmanager
def feed(path=None, lines_i=None, data_f=get_data):

    if lines_i:
        if not isinstance(lines_i, collections.Iterable):
            raise TypeError("'lines_i' must be iterable")
        yield lines_i
    elif path:
        with open(path, 'rU') as lines_i:
            yield lines_i
    else:
        yield data_f()

    # no cleanup needed
    logger.debug("exit feed context manager")
    pass



def batch_iterator(lines=None, max_batch_n=0, max_batch_byte_size=0):

    curr_n = 0
    curr_byte_size = 0

    while ((max_batch_n and (curr_n < max_batch_n)) or
           (max_batch_byte_size and (curr_byte_size < max_batch_byte_size)) ):

        line = next(lines)
        curr_n += 1
        curr_byte_size += len(line)

        yield line


def batches_iterator(lines=None, batch_count=0, max_n=0, max_byte_size=0):

    if not (max_n or max_byte_size):
        raise ValueError("must specify either max_n or max_byte_size")

    for _ in range(batch_count):
        yield batch_iterator(lines=lines, max_batch_n=max_n//batch_count, max_batch_byte_size=max_byte_size//batch_count)



def args_parser():
    parser = argparse.ArgumentParser(description="esbench USPTO patent assignment downloader.")
    parser.add_argument('-v', '--version', action='version', version=esbench.__version__)
    parser.add_argument('--nocache', action='store_true', help="if set, delete downloaded data (default: %(default)s)")
    return parser


def main():

    logging.basicConfig(level=logging.WARNING)
    args = args_parser().parse_args()

    try:
        for line in feed(nocache=args.nocache):
            print(line)

        sys.exit(0)

    except IOError as exc:
        logger.warning(exc)
        pass


if __name__ == "__main__":
    main()

