# -*- coding: UTF-8 -*-

import datetime
import zipfile
import os.path
import logging
import argparse
import sys
import collections
import json
from xml.etree import ElementTree

import requests


__version__ = (0,0,1)

logger = logging.getLogger(__name__)

def urls(count=1):
    d = datetime.datetime.utcnow()
    day = datetime.timedelta(days=1)
    for _ in range(count):
        s = d.strftime(r'ad%Y%m%d.zip')
        yield ("http://storage.googleapis.com/patents/assignments/2013/%s" % s, s)
        d -= day


def download(url, fn):

    if os.path.exists(os.path.abspath(fn)): 
        return os.path.abspath(fn)

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
#             os.remove(zfn)


def get_assignments(days=1): 
    for url, fn in urls(days):
        for xfn in extract(download(url, fn)):
            try: 
                with open(xfn, "rU") as f:
                    for line in f:
                        if line.startswith("<patent-assignment>"):
                            yield line.strip()
                        else:
                            continue
            finally:
#                 os.remove(xfn)
                pass


def parse(s):
#     print(s)
    root = ElementTree.fromstring(s)
    for node in root.iterfind('.//patent-property'):
        p = PatentProperty(node)
        yield p
        


class Element(dict):

    def __init__(self, root): 
        dict.__init__(self) 
        self.from_xml(root)

    def from_xml(self, root):
        raise NotImplementedError()


class PatentProperty(Element): 

    def from_xml(self, root):
    
        i = root.find('invention-title')
        try:
            self['invention-title'] = i.text
            self['invention-title-language'] = i.attrib['lang']
        except AttributeError:
            self['invention-title'] = ''
            self['invention-title-language'] = ''

        self['document-ids'] = []
        ids = list(root.iterfind('.//document-id'))
        if len(ids) == 3: 
            self['document-ids'].append(ApplicationNumber(ids[0]))
            self['document-ids'].append(PatentNumber(ids[1]))
            self['document-ids'].append(PublicationNumber(ids[2]))
        elif len(ids) == 2:
            self['document-ids'].append(ApplicationNumber(ids[0]))
            self['document-ids'].append(PublicationNumber(ids[1]))
        else:
            for docid in ids:
                self['document-ids'].append(DocumentID(docid))


class DocumentID(Element):

    APPLICATION = 'app'
    PATENT = 'pat'
    PUBLICATION = 'pub'
    
    FIELDS = ['country', 'doc-number', 'kind', 'date', '_type_expected', '_type_inferred']

    def _type(self):
        idstr = self['doc-number']
        if len(idstr) == 7: 
            try: int(idstr, 10)
            except ValueError: return False
            self['_type_inferred'] = DocumentID.PATENT
            return True
        elif len(idstr) == 8: 
            try: int(idstr, 10)
            except ValueError: return False
            self['_type_inferred'] = DocumentID.APPLICATION
            return True
        elif len(idstr) in [10, 11]: 
            try: int(idstr, 10)
            except ValueError: return False
            self['_type_inferred'] = DocumentID.PUBLICATION
            return True
        else:
            self['_type_inferred'] = ''
            return False

    
    def from_xml(self, root):
        for k in self.FIELDS:
            try: 
                text = root.findtext(k)
                self[k] = text.strip() if text else ''
            except AttributeError as exc:
                pass
        self._type()
        

class ApplicationNumber(DocumentID):

    def from_xml(self, root):
        super(ApplicationNumber, self).from_xml(root)
        self['_type_expected'] = DocumentID.APPLICATION
        

class PatentNumber(DocumentID):

    def from_xml(self, root):
        super(PatentNumber, self).from_xml(root)
        self['_type_expected'] = DocumentID.PATENT


class PublicationNumber(DocumentID):

    def from_xml(self, root):
        super(PublicationNumber, self).from_xml(root)
        self['_type_expected'] = DocumentID.PUBLICATION


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
                print(json.dumps(p))
#                 sys.exit(1)

    except IOError:
        pass

    sys.exit(0)


if __name__ == "__main__":
    main()
