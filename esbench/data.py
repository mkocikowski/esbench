# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

"""Functions for downloading sample data, and for iterating over input to
create batches of documents, based on counts or on byte sizes. """

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
URL_TEMPLATE = "https://s3-us-west-1.amazonaws.com/esbench/appl_%i_%s.gz"

def _aa(count=None):
    i = ("".join(i) for i in itertools.product(string.lowercase, repeat=2))
    if count:
        i = itertools.islice(i, count)
    return i


def urls(url_template=None, count=75):
    # default count=75 because that is the max number of yearly appl files
    for year in range(2005, 2013):
        for postfix in _aa(count):
            yield (url_template % (year, postfix))


def download(url, tmpd="/tmp"):

    fn = os.path.basename(url)
    fn = os.path.abspath(os.path.join(tmpd, fn))

    logger.info("Downloading '%s' to '%s'", url, fn)

    # if the file already exists, don't download it again
    if os.path.exists(fn):
        logger.info("Using cached file '%s'", fn)
        return fn

    try:
        resp = urllib2.urlopen(url)
        with open(fn, 'w') as f:
            chunk = resp.read(2**16)
            while chunk:
                f.write(chunk)
                chunk = resp.read(2**16)
                sys.stderr.write(".")
        logger.info("finished downloading '%s'", fn)
        resp.close()

    except (IOError,) as exc:
        logger.debug("error %s opening url: %s", exc, url)
        fn = None

    return fn



def unzip(fn):

    with gzip.open(fn, 'rb') as f:
        for line in f:
            yield(line.strip())


def get_data(nocache=False, urls_f=urls):
    """Get default data provided with the benchmark (US Patent Applications).

    Returns an iterator, where each item is a json line with a complete US
    Patent Application document which can be indexed into Elasticsearch. In
    the background it deals with chunked downloads from S3, providing what in
    essence is an 'unlimited' data source. See 'feed()' function below.

    """

    for url in urls_f(URL_TEMPLATE):
        fn = download(url)
        if not fn:
            # download() will return None if data can't be downloaded, in that
            # case just go to the next url
            continue
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
    """Return an iterator with data to be fed into the index.

    Given a source of data, return a safe iterator over that data. Data can
    come from one of 3 sources, provided in the parameters. The idea is that
    you use this function to provide the 'lines' iterator for the
    esbench.data.batches_iterator function below. This function is wrapped in
    a context manager, so you would use it like so:

        with feed() as f:
            for line in f:
                print(line)

        Args:
            path: path to a file (can be '/dev/stdin'). When provided, lines
                will be read from the file. The context manager ensures that
                the file is closed properly when done.
            lines_i: iterator, yielding lines
            data_f: generator function, when called yields lines

    """

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
    """Yields up to n lines, or up to x byte size of data.

    Given an iterator, yields an iterator which will pass through the data up
    to n lines, or until specified byte size of data has been passed.

    Args:
        lines: iterator
        max_batch_n: max number of lines to yield from the iterator
        max_batch_byte_size: max byte size of data to yield. The actual amount
            of data yielded will be greater than the amount specified - the
            iterator doesn't have a look ahead capacity, so by the time it
            knows the size of the next item it is too late to put it back in
            if it it too large. Sorry.

    Yields:
        items from the provided iterator

    """

    curr_n = 0
    curr_byte_size = 0

    while ((max_batch_n and (curr_n < max_batch_n)) or
           (max_batch_byte_size and (curr_byte_size < max_batch_byte_size)) ):

        line = next(lines)
        curr_n += 1
        curr_byte_size += len(line)

        yield line


def batches_iterator(lines=None, batch_count=0, max_n=0, max_byte_size=0):
    """Yields n batches of lines.

    Each batch is an iterator containing either n lines, or lines of certain
    combined byte size. You must provide batch_count, and either max_n or
    max_byte_size. If you provide max_n, then each batch will contain
    max_n//batch_count lines. If you provide max_byte_size, then each batch
    will contain lines whose total byte size is approximately
    max_byte_size//batch_count.

        Args:
            lines: iterator of lines, get it from esbench.data.feed()
            batch_count: int, number of batches
            max_n: total number of documents in all batches
            max_byte_size: total byte size of all documents in all batches

        Yields:
            batches, which themselves are iterators of lines.

        Raises:
            ValueError: neither max_n not max_byte_size specified

    """

    if not (max_n or max_byte_size):
        raise ValueError("must specify either max_n or max_byte_size")

    for _ in range(batch_count):
        yield batch_iterator(
                lines=lines,
                max_batch_n=max_n//batch_count,
                max_batch_byte_size=max_byte_size//batch_count,
        )



def args_parser():
    parser = argparse.ArgumentParser(description="esbench USPTO patent assignment downloader.")
    parser.add_argument('-v', '--version', action='version', version=esbench.__version__)
    parser.add_argument('--nocache', action='store_true', help="if set, delete downloaded data (default: %(default)s)")
    return parser


def main():

    logging.basicConfig(level=logging.INFO)
    args = args_parser().parse_args()

    try:
        with feed() as f:
            for line in f:
                print(line)

    except IOError as exc:
#         logger.warning(exc)
        pass


if __name__ == "__main__":
    main()

