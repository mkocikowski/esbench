# -*- coding: UTF-8 -*-

import datetime
import os.path
import unittest

import esbench.uspto

ASSIGNMENT = """<patent-assignment><assignment-record><reel-no>31066</reel-no><frame-no>972</frame-no><last-update-date><date>20130824</date></last-update-date><purge-indicator>N</purge-indicator><recorded-date><date>20130823</date></recorded-date><page-count>4</page-count><correspondent><name>OTIS PATENT LAW</name><address-1>1181 WADE STREET</address-1><address-2>HIGHLAND PARK, IL 60035</address-2></correspondent><conveyance-text>ASSIGNMENT OF ASSIGNORS INTEREST (SEE DOCUMENT FOR DETAILS).</conveyance-text></assignment-record><patent-assignors><patent-assignor><name>MOYER, ILAN</name><execution-date><date>20110526</date></execution-date></patent-assignor><patent-assignor><name>LOBOVSKY, MAXIM</name><execution-date><date>20110526</date></execution-date></patent-assignor></patent-assignors><patent-assignees><patent-assignee><name>MASSACHUSETTS INSTITUTE OF TECHNOLOGY</name><address-1>77 MASSACHUSETTS AVE.</address-1><city>CAMBRIDGE</city><state>MASSACHUSETTS</state><postcode>02139</postcode></patent-assignee></patent-assignees><patent-properties><patent-property><document-id><country>US</country><doc-number>13116504</doc-number><kind>X0</kind><date>20110526</date></document-id><document-id><country>US</country><doc-number>20110289741</doc-number><kind>A1</kind><date>20111201</date></document-id><invention-title lang="en">Methods and apparatus for applying tension to a motion transmission element</invention-title></patent-property></patent-properties></patent-assignment>"""

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
        
        
        
if __name__ == "__main__":
    unittest.main()     

