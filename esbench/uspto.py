# -*- coding: UTF-8 -*-

import datetime
import zipfile
import os.path
import logging
import argparse
import sys
import collections
import json
import datetime
from xml.etree import ElementTree

import requests


__version__ = (0,0,1)

logger = logging.getLogger(__name__)

def format_date(s):
    try: 
        dt = datetime.datetime.strptime(s, r"%Y%m%d")
        return dt.strftime(r"%Y-%m-%d")
    except ValueError: 
        return ""


def urls(count=1):
    d = datetime.datetime.utcnow()
    day = datetime.timedelta(days=1)
    for _ in range(count):
        s = d.strftime(r'ad%Y%m%d.zip')
        yield ("http://storage.googleapis.com/patents/assignments/2013/%s" % s, s)
        d -= day
        if d < datetime.datetime(2013, 01, 01): 
            break


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
        


class PatentAssignmentElement(dict):

    def __init__(self, root): 
        dict.__init__(self)
#         print(type(root)) 
        self.from_xml(root)
        
    def __setitem__(self, key, value):
        if isinstance(value, str):
            value = value.upper()
        dict.__setitem__(self, key, value)

    def from_xml(self, root):
        raise NotImplementedError()


class PatentProperty(PatentAssignmentElement): 

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


class DocumentID(PatentAssignmentElement):

    APPLICATION = 'app'
    PATENT = 'pat'
    PUBLICATION = 'pub'
    
    def _type(self):
    
        idstr = self['doc_number']
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
    
        self['country'] = root.findtext('./country', default='')
        self['doc_number'] = root.findtext('./doc-number', default='')
        self['kind'] = root.findtext('./kind', default='')
        self['date'] = format_date(root.findtext('./date', default=''))
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


class Address(PatentAssignmentElement):

    def from_xml(self, root):
        self['address_1'] = root.findtext('.//address-1', default='')
        self['address_2'] = root.findtext('.//address-2', default='')
        self['address_3'] = root.findtext('.//address-3', default='')
        self['address_4'] = root.findtext('.//address-4', default='')
        self['city'] = root.findtext('.//city', default='')
        self['state'] = root.findtext('.//state', default='')
        self['country_name'] = root.findtext('.//country-name', default='')
        self['postcode'] = root.findtext('.//postcode', default='')


class Entity(PatentAssignmentElement):

    def from_xml(self, root):
        self['name'] = root.findtext('./name', default='')
        self['address'] = Address(root)


class Assignor(Entity): 

    def from_xml(self, root):
        super(Assignor, self).from_xml(root)
        self['date_execution'] = format_date(root.findtext('./execution-date/date', default=''))


class Assignee(Entity):
    pass


class Correspondent(Entity):
    pass
    

class AssignmentRecord(PatentAssignmentElement):
        
    def from_xml(self, root):
        self['reel_no'] = root.findtext('./reel-no', default='')
        self['frame_no'] = root.findtext('./frame-no', default='')
        self['date_last_update'] = format_date(root.findtext('./last-update-date/date', default=''))
        self['date_recorded'] = format_date(root.findtext('./recorded-date/date', default=''))
        self['correspondent'] = Correspondent(root.find('./correspondent'))
        self['conveyance_text'] = root.findtext('./conveyance-text', default='')



class PatentAssignment(dict):
    
    def __init__(self, xml): 
        dict.__init__(self)
        self.xml = xml
        self.tree = ElementTree.fromstring(self.xml)
        self['assignment_record'] = AssignmentRecord(self.tree.find('.//assignment-record'))
        self['patent_assignors'] = []
        for node in self.tree.iterfind('.//patent-assignor'):
            a = Assignor(node)
            self['patent_assignors'].append(a)
        self['patent_assignees'] = []
        for node in self.tree.iterfind('.//patent-assignee'):
            a = Assignee(node)
            self['patent_assignees'].append(a)
        self['patent_properties'] = []
        for node in self.tree.iterfind('.//patent-property'):
            p = PatentProperty(node)
            self['patent_properties'].append(p)


def get_args_parser():

    parser = argparse.ArgumentParser(description="esbench USPTO patent assignment downloader.")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('-d', '--days', type=int, default=3, help="fetch records for how many days? (default: %(default)s)")
    parser.add_argument('-i', metavar='INDENT', type=int, default=None, help="if set, format JSON output with indent (default: %(default)s)")
    return parser


def main():

    logging.basicConfig(level=logging.WARNING)
    args = get_args_parser().parse_args()

    try: 
        for line in get_assignments(args.days):
#             print(line)
            a = PatentAssignment(line)
            print(json.dumps(a, indent=args.i, sort_keys=True))
#             print("*********************************************************")
#             sys.exit(1)

    except IOError:
        pass

    sys.exit(0)


if __name__ == "__main__":
    main()
