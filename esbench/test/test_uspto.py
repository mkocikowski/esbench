# -*- coding: UTF-8 -*-

import datetime
import os.path
import unittest
import json

import esbench.uspto

ASSIGNMENT = """<patent-assignment><assignment-record><reel-no>31066</reel-no><frame-no>972</frame-no><last-update-date><date>20130824</date></last-update-date><purge-indicator>N</purge-indicator><recorded-date><date>20130823</date></recorded-date><page-count>4</page-count><correspondent><name>OTIS PATENT LAW</name><address-1>1181 WADE STREET</address-1><address-2>HIGHLAND PARK, IL 60035</address-2></correspondent><conveyance-text>ASSIGNMENT OF ASSIGNORS INTEREST (SEE DOCUMENT FOR DETAILS).</conveyance-text></assignment-record><patent-assignors><patent-assignor><name>MOYER, ILAN</name><execution-date><date>20110526</date></execution-date></patent-assignor><patent-assignor><name>LOBOVSKY, MAXIM</name><execution-date><date>20110526</date></execution-date></patent-assignor></patent-assignors><patent-assignees><patent-assignee><name>MASSACHUSETTS INSTITUTE OF TECHNOLOGY</name><address-1>77 MASSACHUSETTS AVE.</address-1><city>CAMBRIDGE</city><state>MASSACHUSETTS</state><postcode>02139</postcode></patent-assignee></patent-assignees><patent-properties><patent-property><document-id><country>US</country><doc-number>13116504</doc-number><kind>X0</kind><date>20110526</date></document-id><document-id><country>US</country><doc-number>20110289741</doc-number><kind>A1</kind><date>20111201</date></document-id><invention-title lang="en">Methods and apparatus for applying tension to a motion transmission element</invention-title></patent-property></patent-properties></patent-assignment>"""
PARSED = """{"assignment_record": {"conveyance_text": "ASSIGNMENT OF ASSIGNORS INTEREST (SEE DOCUMENT FOR DETAILS).", "correspondent": {"address": {"address_1": "1181 WADE STREET", "address_2": "HIGHLAND PARK, IL 60035", "address_3": "", "address_4": "", "city": "", "country_name": "", "postcode": "", "state": ""}, "name": "OTIS PATENT LAW"}, "date_last_update": "2013-08-24", "date_recorded": "2013-08-23", "frame_no": "972", "reel_no": "31066"}, "patent_assignees": [{"address": {"address_1": "77 MASSACHUSETTS AVE.", "address_2": "", "address_3": "", "address_4": "", "city": "CAMBRIDGE", "country_name": "", "postcode": "02139", "state": "MASSACHUSETTS"}, "name": "MASSACHUSETTS INSTITUTE OF TECHNOLOGY"}], "patent_assignors": [{"address": {"address_1": "", "address_2": "", "address_3": "", "address_4": "", "city": "", "country_name": "", "postcode": "", "state": ""}, "date_execution": "2011-05-26", "name": "MOYER, ILAN"}, {"address": {"address_1": "", "address_2": "", "address_3": "", "address_4": "", "city": "", "country_name": "", "postcode": "", "state": ""}, "date_execution": "2011-05-26", "name": "LOBOVSKY, MAXIM"}], "patent_properties": [{"document-ids": [{"_type_expected": "APP", "_type_inferred": "APP", "country": "US", "date": "2011-05-26", "doc_number": "13116504", "kind": "X0"}, {"_type_expected": "PUB", "_type_inferred": "PUB", "country": "US", "date": "2011-12-01", "doc_number": "20110289741", "kind": "A1"}], "invention-title": "METHODS AND APPARATUS FOR APPLYING TENSION TO A MOTION TRANSMISSION ELEMENT", "invention-title-language": "EN"}]}"""

class USPTOTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tmp = []

    @classmethod
    def tearDownClass(cls):
        for fn in cls.tmp:
            os.remove(fn)

    def setUp(self):
        self.files = []

    def test_urls(self):
        self.assertEqual(1, len(list(esbench.uspto.urls())))
        self.assertEqual(
            (datetime.date.today().strftime(r'http://storage.googleapis.com/patents/assignments/2013/ad%Y%m%d.zip'),
             datetime.date.today().strftime(r'ad%Y%m%d.zip')), 
            list(esbench.uspto.urls())[0])
        self.assertLess(len(list(esbench.uspto.urls(400))), 365)

#     def test_download_and_unzip(self): 
#         for url, fn in esbench.uspto.urls(4):
#             saved = esbench.uspto.download(url, fn)
#             if saved: 
#                 self.assertTrue(os.path.exists(saved))
#                 self.files.append(saved)
#                 self.tmp.append(saved)
#         
#         for fn in self.files:
#             for xfn in esbench.uspto.extract(fn):
#                 self.assertTrue(xfn.endswith(".xml"))
#                 self.tmp.append(xfn)
# 

    def test_parse(self):
        parsed = list(esbench.uspto.parse(ASSIGNMENT))[0]
        self.assertTrue(isinstance(parsed, dict))


    def test_PatentAssignment(self):
        ar = esbench.uspto.PatentAssignment(ASSIGNMENT)
        s = json.dumps(ar, indent=None, sort_keys=True)        
        self.assertEqual(s, PARSED)
        
if __name__ == "__main__":
    unittest.main()     

