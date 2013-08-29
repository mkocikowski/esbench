# -*- coding: UTF-8 -*-

import datetime
import logging
import argparse
import sys
import json

from xml.etree import ElementTree
# from lxml import etree as ElementTreeLXML


__version__ = (0, 0, 1)

logger = logging.getLogger(__name__)


def format_date(s):
    try: 
        dt = datetime.datetime.strptime(s, r"%Y%m%d")
        return dt.strftime(r"%Y-%m-%d")
    except ValueError: 
        return ""


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
    
    def __init__(self, xml): 
        dict.__init__(self)
        self.xml = xml
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




def args_parser():

    parser = argparse.ArgumentParser(description="esbench USPTO patent assignment parser.")
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('-i', metavar='INDENT', type=int, default=None, help="if set, format JSON output with indent (default: %(default)s)")
    return parser


def main():

    logging.basicConfig(level=logging.WARNING)
    args = args_parser().parse_args()

    try: 
        for line in sys.stdin:
            parsed = PatentAssignment(line)
            print(json.dumps(parsed, indent=args.i, sort_keys=True))

    except IOError:
        logger.warning("Exiting with IO error")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()





# 
# APPLICATION = 'app'
# PATENT = 'pat'
# PUBLICATION = 'pub'
# UNKNOWN = ''
# 
# def _type(idstr):
# 
#     try: 
#         if len(idstr) == 7: 
#             return PATENT
#         elif len(idstr) == 8: 
#             int(idstr, 10)
#             return APPLICATION
#         elif len(idstr) in [10, 11]: 
#             int(idstr, 10)
#             return PUBLICATION
#         else:
#             return UNKNOWN
# 
#     except ValueError:
#         return UNKNOWN
# 
# 
# def _date(s):
#     try: 
#         dt = datetime.datetime.strptime(s, r"%Y%m%d")
#         return dt.strftime(r"%Y-%m-%d")
#     except ValueError: 
#         return ""
# 
# 
# def _parse_docid(root):
# 
#     i = {
#         'country': root.findtext('./country', default=''), 
#         'doc_number': root.findtext('./doc-number', default=''), 
#         'kind': root.findtext('./kind', default=''), 
#         'date': _date(root.findtext('./date', default='')), 
#         '_type': '', 
#     }
# 
#     i['type'] = _type(i['doc_number'])
#     return i
# 
# 
# def _parse_property(root):
# 
#     ids = [_parse_docid(node) for node in root.iterfind('./document-id')]
# 
#     d = {
#         'invention_title': root.findtext('./invention-title', default=''), 
#         'invention_title_language': 'EN', 
#         'document_ids': ids,
#     }
# 
#     return d
# 
# def _parse_entity(root):
#     
#     d = {
#         'date_execution': _date(root.findtext('./execution-date/date', default='')), 
#         'name': root.findtext('./name', default=''), 
#         'address': {
#             'address_1': root.findtext('./address-1', default=''),
#             'address_2': root.findtext('./address-2', default=''),
#             'address_3': root.findtext('./address-3', default=''),
#             'address_4': root.findtext('./address-4', default=''),
#             'city': root.findtext('./city', default=''),
#             'state': root.findtext('./state', default=''),
#             'country_name': root.findtext('./country-name', default=''),
#             'postcode': root.findtext('./postcode', default=''),
#         }, 
#     }
#     
#     return d
#     
# 
# def parse(root):
# 
#     node = root.find('./assignment-record')
#     ar = {
#         'reel_no': node.findtext('./reel-no', default=''), 
#         'frame_no': node.findtext('./frame-no', default=''), 
#         'page_count': node.findtext('./page_count', default=''), 
#         'date_last_update': _date(node.findtext('./last-update-date/date', default='')), 
#         'date_recorded': _date(node.findtext('./recorded-date/date', default='')), 
#         'conveyance_text': node.findtext('./conveyance-text', default=''),         
#         'correspondent': _parse_entity(node.find('./correspondent')), 
#         '_rf': '', 
#     }    
#     ar['_rf'] = "%s_%s" % (ar['reel_no'], ar['frame_no'])
# 
#     assors = [_parse_entity(node) for node in root.iterfind('./patent-assignors/patent-assignor')]
#     assees = [_parse_entity(node) for node in root.iterfind('./patent-assignees/patent-assignee')]
# 
#     props = [_parse_property(node) for node in root.iterfind('./patent-properties/patent-property')]
# 
#     d = {
#         'assignment_record': ar, 
#         'patent_assignors': assors, 
#         'patent_assignees': assees, 
#         'patent_properties': props, 
#     }
#     
#     return d
#     
#         

