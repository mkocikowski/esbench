# -*- coding: UTF-8 -*-

import datetime
import os.path
import unittest
import json

from .. import download

# ASSIGNMENT = """<patent-assignment><assignment-record><reel-no>29564</reel-no><frame-no>141</frame-no><last-update-date><date>20130104</date></last-update-date><purge-indicator>N</purge-indicator><recorded-date><date>20130103</date></recorded-date><page-count>6</page-count><correspondent><name>PERKINS COIE LLP</name><address-1>P.O. BOX 1247</address-1><address-2>PATENT - SEA</address-2><address-3>SEATTLE, WA 98111-1247</address-3></correspondent><conveyance-text>ASSIGNMENT OF ASSIGNORS INTEREST (SEE DOCUMENT FOR DETAILS).</conveyance-text></assignment-record><patent-assignors><patent-assignor><name>SCHMIDT, PETER</name><execution-date><date>20110812</date></execution-date></patent-assignor><patent-assignor><name>KEMPPAINEN, KURT</name><execution-date><date>20110812</date></execution-date></patent-assignor><patent-assignor><name>BASCHE, RAHN</name><execution-date><date>20110816</date></execution-date></patent-assignor></patent-assignors><patent-assignees><patent-assignee><name>WORDLOCK, INC.</name><address-1>2855 KIFER ROAD, SUITE 245</address-1><city>SANTA CLARA</city><state>CALIFORNIA</state><postcode>95051</postcode></patent-assignee></patent-assignees><patent-properties><patent-property><document-id><country>US</country><doc-number>29400306</doc-number><kind>X0</kind><date>20110825</date></document-id><document-id><country>US</country><doc-number>D662395</doc-number><kind>B1</kind><date>20120626</date></document-id><invention-title lang="en">COMBINATION DISC LOCK</invention-title></patent-property></patent-properties></patent-assignment>"""
ASSIGNMENT = """<patent-assignment><assignment-record><reel-no>22040</reel-no><frame-no>610</frame-no><last-update-date><date>20090101</date></last-update-date><purge-indicator>N</purge-indicator><recorded-date><date>20081230</date></recorded-date><page-count>3</page-count><correspondent><name>GARDNER GROFF GREENWALD &amp; VILLANUEVA. PC</name><address-1>2018 POWERS FERRY ROAD</address-1><address-2>SUITE 800</address-2><address-3>ATLANTA, GA 30339</address-3></correspondent><conveyance-text>ASSIGNMENT OF ASSIGNORS INTEREST (SEE DOCUMENT FOR DETAILS).</conveyance-text></assignment-record><patent-assignors><patent-assignor><name>DABDOUB, ATIF M.</name><execution-date><date>20081218</date></execution-date></patent-assignor></patent-assignors><patent-assignees><patent-assignee><name>UNICHEM TECHNOLOGIES, INC.</name><address-1>1266 WEST PACES FERRY ROAD</address-1><address-2>SUITE 405</address-2><city>ATLANTA</city><state>GEORGIA</state><postcode>30327</postcode></patent-assignee></patent-assignees><patent-properties><patent-property><document-id><country>US</country><doc-number>12207945</doc-number><kind>X0</kind><date>20080910</date></document-id><document-id><country>US</country><doc-number>20090005527</doc-number><kind>A1</kind><date>20090101</date></document-id><invention-title lang="en">PHOSPHONIC COMPOUNDS AND METHODS OF USE THEREOF</invention-title></patent-property></patent-properties></patent-assignment>"""

class DownloadTest(unittest.TestCase):


    def test_urls(self):
    
        self.assertEqual(
            ('http://storage.googleapis.com/patents/assignments/2009/ad20090101.zip', 'ad20090101.zip'), 
            download.urls().next()
        )


    def test_download_and_unzip(self): 

        files = []
        
        try: 
            for url, fn in download.urls():
                zfn = download.download(url, fn)
                if zfn: 
                    self.assertTrue(os.path.exists(zfn))
                    files.append(zfn)
        
            for fn in files:
                for xfn in download.extract(fn):
                    self.assertTrue(xfn.endswith(".xml"))
                    self.assertTrue(os.path.exists(zfn))
                    files.append(xfn)
        
        finally:
            for fn in files:
                try: os.remove(fn)
                except: pass


    def test_get_records(self): 
        
        lines = list(download.lines(days=1))
        self.assertEqual(6216, len(lines))
        self.assertEqual(ASSIGNMENT, lines[-1])


# 
# if __name__ == "__main__":
#     unittest.main()     

