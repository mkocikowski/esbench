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



def urls(retro=False, offset_2013=0, days_2013=1):
    if retro:
        for n in range(1, 13):
            s = "ad20121231-%02i.zip" % n
            yield("http://storage.googleapis.com/patents/retro/2012/%s" % s, os.path.abspath(s))
    d = datetime.datetime(2013, 1, 1)
    day = datetime.timedelta(days=1)
    d += day * offset_2013
    for _ in range(days_2013):
        s = d.strftime(r'ad%Y%m%d.zip')
        yield ("http://storage.googleapis.com/patents/assignments/2013/%s" % s, os.path.abspath(s))
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
        yield os.path.abspath(xfn)
        return
    
    else:
        try: 
            archive = zipfile.ZipFile(zfn)
            for fn in archive.namelist():
                if not os.path.exists(os.path.abspath(fn)): 
                    archive.extract(fn)
                yield os.path.abspath(fn)
            return

        finally:
            archive.close()


def lines(offset=0, days=1, retro=False, cache=False):

    for url, fn in urls(retro=retro, offset_2013=offset, days_2013=days):
        for xfn in extract(download(url=url, fn=fn)):
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
    parser.add_argument('-d', '--days', type=int, default=3, help="fetch records for how many days? (default: %(default)s)")
    parser.add_argument('-o', '--offset', type=int, default=0, help="start offset (default: %(default)s)")
    parser.add_argument('--cache', action='store_true', help="if set, don't delete downloaded data (default: %(default)s)")
    parser.add_argument('--retro', action='store_true', help="if set, download data for 1980-2012 (default: %(default)s)")
    return parser


def main():

    logging.basicConfig(level=logging.WARNING)
    args = args_parser().parse_args()

    try: 
        count = 0
        for line in lines(offset=args.offset, days=args.days, retro=args.retro, cache=args.cache):
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
