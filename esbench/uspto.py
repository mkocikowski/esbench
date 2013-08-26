# -*- coding: UTF-8 -*-

import datetime
import zipfile
import os.path
import logging
import argparse
import sys
import collections
from xml.etree import ElementTree

import requests


__version__ = (0,0,1)


def urls(count=1):
    d = datetime.datetime.utcnow()
    day = datetime.timedelta(days=1)
    for _ in range(count):
        s = d.strftime(r'ad%Y%m%d.zip')
        yield ("http://storage.googleapis.com/patents/assignments/2013/%s" % s, s)
        d -= day


def download(url, fn):
    response = requests.get(url)
    if response.status_code == 200:
        with open(fn, "w") as f:
            f.write(response.content)
        return os.path.abspath(fn)
    else:
        return False


def extract(zfn):

    if not zfn or not zfn.endswith(".zip"):
        return

    else: 
        try: 
            archive = zipfile.ZipFile(zfn)
            for fn in archive.namelist():
                archive.extract(fn)
                yield os.path.abspath(fn)
    
        finally:
            archive.close()


def get_assignments(days=1): 
    for url, fn in urls(days):
        for xfn in extract(download(url, fn)):
            with open(xfn, "rU") as f:
                for line in f:
                    if line.startswith("<patent-assignment>"):
                        yield line.strip()
                    else:
                        continue


def parse(s):
    root = ElementTree.fromstring(s)
    for node in root.iterfind('.//patent-property'):
        p = PatentProperty(node)
        yield p
        
    
class PatentProperty(collections.Mapping): 

    def __init__(self, root):
        self.data = {
            'invention-title': '',
            'invention-title-language': '', 
            'document-ids': []
        }
        self.from_xml(root)
        return

    def __getitem__(self, key): 
        return self.data[key]
    
    def __iter__(self):
        for k in self.data:
            yield k
    
    def __len__(self):
        return 3
    
    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return str(self.data)

    def from_xml(self, root):
#         root = ElementTree.fromstring(xml)

        i = root.find('invention-title')
        try:
            self.data['invention-title'] = i.text
            self.data['invention-title-language'] = i.attrib['lang']
        except AttributeError:
            self.data['invention-title'] = ''
            self.data['invention-title-language'] = ''
            
        for node in root.iterfind('.//document-id'):
#             docid = DocumentID(ElementTree.tostring(node))
            docid = DocumentID(node)
            self.data['document-ids'].append(docid)
    


class DocumentID(collections.Mapping):

    def __init__(self, root): 
        self.data = {
            'country': '', 
            'doc-number': '', 
            'kind': '', 
            'date': '' 
        }
        self.from_xml(root)
        return

    def __getitem__(self, key): 
        return self.data[key]
    
    def __iter__(self):
        for k in self.data:
            yield k
    
    def __len__(self):
        return 4

    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return str(self.data)
    
    def from_xml(self, root):
#         root = ElementTree.fromstring(xml)
        for k in self.data:
            try: 
                self.data[k] = root.findtext(k).strip()
            except AttributeError:
                self.data[k] = ''




def get_args_parser():
    parser = argparse.ArgumentParser(description="esbench USPTO patent assignment downloader.")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('-d', '--days', type=int, default=3, help="fetch records for how many days? (default: %(default)s)")
    return parser


def main():
    logging.basicConfig(level=logging.WARNING)
    args = get_args_parser().parse_args()

    try: 
        for a in get_assignments(args.days):
            for p in parse(a):
                print(p)

    except IOError:
        pass

    sys.exit(0)


if __name__ == "__main__":
    main()
