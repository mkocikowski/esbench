# -*- coding: UTF-8 -*-

import datetime
import zipfile
import os.path
import logging
import argparse
import sys

import requests


__version__ = (0, 0, 1)

logger = logging.getLogger(__name__)


# def _retro_backside_urls():
#     for year in range(2005, 2013): 
#         for n in range(1, 16):
#             s = "ad%i1231-%02i.zip" % (year, n)
#             yield("wget http://storage.googleapis.com/patents/retro/%i/%s" % (year, s))
# 
# 
# def _retro_frontside_urls():
#     d = datetime.datetime(2004, 1, 1)
#     day = datetime.timedelta(days=1)
#     for n in range(365 * 10):
#         s = d.strftime(r'ad%Y%m%d.zip')
#         yield ("wget http://storage.googleapis.com/patents/assignments/%i/%s" % (d.year, s))
#         d += day


def urls(count=1):
    d = datetime.datetime(2009, 1, 1)
    day = datetime.timedelta(days=1)
    for _ in range(count):
        s = d.strftime(r'ad%Y%m%d.zip')
        yield ("http://storage.googleapis.com/patents/assignments/%i/%s" % (d.year, s), s)
        d += day
        if d > datetime.datetime.utcnow(): 
            break


def download(url=None, fn=None):

    if os.path.exists(os.path.abspath(fn)): 
        return os.path.abspath(fn)

    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(fn, "w") as f:
            for chunk in response.iter_content(chunk_size=2**16): 
                f.write(chunk)
        return os.path.abspath(fn)

    else:
        return False


def extract(zfn):

    if not zfn or not zfn.endswith(".zip"):
        return
    
    xfn = os.path.abspath("%s.xml" % (zfn[:-4], ))
    if os.path.exists(xfn):
        yield xfn
        return
    
    else:
        try:
            archive = None 
            archive = zipfile.ZipFile(zfn)
            for fn in archive.namelist():
                if not os.path.exists(os.path.abspath(fn)): 
                    archive.extract(fn)
                yield os.path.abspath(fn)
            return

        finally:
            if archive: archive.close()


def lines(days=None, cache=False):

    for url, fn in urls(days):
        zfn = download(url=url, fn=os.path.abspath(fn))
        for xfn in extract(zfn):
            try: 
                with open(xfn, "rU") as f:
                    for line in f:
                        if line.startswith("<patent-assignment>"):
                            yield line.strip()
                        else:
                            continue

            finally:
                if not cache: os.remove(xfn)

        if not cache and os.path.exists(fn):
            os.remove(fn)


def args_parser():

    parser = argparse.ArgumentParser(description="esbench USPTO patent assignment downloader.")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('--cache', action='store_true', help="if set, don't delete downloaded data (default: %(default)s)")
    parser.add_argument('--retro', action='store_true', help="if set, download data for 1980-2012 (default: %(default)s)")
    parser.add_argument('n', type=int, default=1, help="fetch records for how many days? (default: %(default)s)")
    return parser


def main():

    logging.basicConfig(level=logging.WARNING)
    args = args_parser().parse_args()

    try: 
        count = 0
        for line in lines(days=args.n, cache=args.cache):
            sys.stderr.write("%i," % (len(line)/2**10))
            print(line)
            count += 1
            if count == 50:
                sys.stderr.write("\n")
                count = 0

    except IOError:
        logger.warning("Exiting with IO error")
        sys.exit(1)

    finally:
        sys.stderr.write("\n")

    sys.exit(0)


if __name__ == "__main__":
    main()


# python uspto.py -d 0 --retro 2> retro.log  > retro.json
