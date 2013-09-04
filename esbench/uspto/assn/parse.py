# -*- coding: UTF-8 -*-

import datetime
import logging
import argparse
import sys
import json
import re
import os.path

from xml.etree import ElementTree
# from lxml import etree as ElementTreeLXML


__version__ = "0.0.1"

logger = logging.getLogger(__name__)


def format_date(s):
    try: 
        dt = datetime.datetime.strptime(s, r"%Y%m%d")
        return dt.strftime(r"%Y-%m-%d")
    except (TypeError, ValueError): 
        return None


class PatentAssignmentElement(dict):

    def __init__(self, root): 
        dict.__init__(self)
        self.from_xml(root)
        
    def __setitem__(self, key, value):
        if isinstance(value, str):
            value = value.upper()
        dict.__setitem__(self, key, value)

    def from_xml(self, root):
        raise NotImplementedError()


class PatentProperty(PatentAssignmentElement): 

    def from_xml(self, root):
        self['invention_title'] = root.findtext('./invention-title')
        self['invention_title_lang'] = 'EN'
        self['doc_ids'] = [DocumentID(node) for node in root.iterfind('./document-id')]


class DocumentID(PatentAssignmentElement):

    APPLICATION = 'APP'
    PATENT = 'PAT'
    PUBLICATION = 'PUB'
    UNKNOWN = 'XXX'
    
    def _set_type(self):

        idstr = self['doc_number']

        try:        
            if len(idstr) == 7: 
                self['_type'] = DocumentID.PATENT
            elif len(idstr) == 8: 
                int(idstr, 10)
                self['_type'] = DocumentID.APPLICATION
            elif len(idstr) in [10, 11]: 
                int(idstr, 10)
                self['_type'] = DocumentID.PUBLICATION
            else:
                self['_type'] = DocumentID.UNKNOWN
        
        except ValueError: 
            self['_type'] = DocumentID.UNKNOWN

    
    def from_xml(self, root):
    
        self['country'] = root.findtext('./country', default='')
        self['doc_number'] = root.findtext('./doc-number', default='')
        self['kind'] = root.findtext('./kind', default='')
        self['date'] = format_date(root.findtext('./date', default=''))
        self._set_type()
        

class Address(PatentAssignmentElement):

    def from_xml(self, root):
        self['address_1'] = root.findtext('./address-1', default='')
        self['address_2'] = root.findtext('./address-2', default='')
        self['address_3'] = root.findtext('./address-3', default='')
        self['address_4'] = root.findtext('./address-4', default='')
        self['city'] = root.findtext('./city', default='')
        self['state'] = root.findtext('./state', default='')
        self['country'] = root.findtext('./country-name', default='')
        self['postcode'] = root.findtext('./postcode', default='')


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
        self['page_count'] = root.findtext('./page-count', default='')
        self['date_last_update'] = format_date(root.findtext('./last-update-date/date', default=''))
        self['date_recorded'] = format_date(root.findtext('./recorded-date/date', default=''))
        self['correspondent'] = Correspondent(root.find('./correspondent'))
        self['conveyance_text'] = root.findtext('./conveyance-text', default='')
        self['_rf'] = "%s_%s" % (self['reel_no'], self['frame_no'])


class PatentAssignment(dict):
    
    def __init__(self, xml, filename=None): 
        dict.__init__(self)
        self.xml = xml
        self.filename = filename
        self['_meta'] = metadata(filename)
        self.tree = ElementTree.fromstring(self.xml)
        self['assignment_record'] = AssignmentRecord(self.tree.find('./assignment-record'))
        self['patent_assignors'] = []
        for node in self.tree.iterfind('./patent-assignors/patent-assignor'):
            a = Assignor(node)
            self['patent_assignors'].append(a)
        self['patent_assignees'] = []
        for node in self.tree.iterfind('./patent-assignees/patent-assignee'):
            a = Assignee(node)
            self['patent_assignees'].append(a)
        self['patent_properties'] = []
        for node in self.tree.iterfind('./patent-properties/patent-property'):
            p = PatentProperty(node)
            self['patent_properties'].append(p)
        self['patent_properties_count'] = len(self['patent_properties'])


def metadata(filename): 

    data = {
        'parser_version': __version__, 
    }
    
    try: 
        fn = os.path.basename(filename)
        match = re.match(r"^ad(\d{8})(-(\d\d))?.xml$", fn)
        date, _, batch = match.groups()
        data['googl_date_published'] = format_date(date) 
        data['googl_type'] = 'backside' if batch else 'frontside' 
        data['googl_filename'] = fn
    
    except (AttributeError, TypeError) as exc:
        logger.error(filename) 
        logger.error(exc)
        pass
    
    return data
    
        

def parse(line, filename):

    if line.startswith("<patent-assignment>"): 
        parsed = PatentAssignment(line, filename=filename)
        return parsed

    else:
        return None


def args_parser():

    parser = argparse.ArgumentParser(description="esbench USPTO patent assignment parser.")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('-i', metavar='INDENT', type=int, default=None, help="if set, format JSON output with indent (default: %(default)s)")
    parser.add_argument('infile', nargs='?', type=str, default=None, help="if set, read input from that file, ano output to file with same filename but '.json' extension; the input file must end with '.xml' (default: %(default)s)")
    return parser


def main():

    logging.basicConfig(level=logging.WARNING)
    args = args_parser().parse_args()
    sort = True if args.i else False

    try: 
        if args.infile: 
            with open(args.infile, 'rU') as infile: 
                with open("%s.json" % (args.infile[:-4],), 'w') as outfile: 
                    for line in infile: 
                        parsed = parse(line, args.infile)
                        if parsed:
                            s = json.dumps(parsed, indent=args.i, sort_keys=sort)
                            outfile.write("%s\n" % s)
                            sys.stderr.write("%i," % len(parsed['patent_properties']))
                        else:
                            continue
        
        else:
            for line in sys.stdin:
                parsed = parse(line)
                if parsed:
                    s = json.dumps(parsed, indent=args.i, sort_keys=sort)
                    print(s)
                    sys.stderr.write("%i," % len(parsed['patent_properties']))
                else:
                    continue

        sys.stderr.write("\n")

    except IOError:
        logger.warning("Exiting with IO error")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()

# ls -1 /Volumes/MK/uspto/frontside/xml/*.xml | xargs -P 2 -n 1 python parse.py 2> /Volumes/MK/uspto/frontside/frontside.log
