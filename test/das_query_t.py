
# system modules
import re
import sys
import unittest
from   cStringIO import StringIO

# DAS modules
import DAS.utils.jsonwrapper as json
from   DAS.core.das_query import DASQuery
from   DAS.core.das_mongocache import encode_mongo_query, decode_mongo_query, loose
from   DAS.utils.utils import genkey
from   DAS.core.das_core import DASCore

DASCORE = DASCore()

class testDASQuery(unittest.TestCase):

    def test_query_initialization(self):
        "Test DAS query init"
        iquery = 'file dataset=/a/b/c'
        dquery = DASQuery(iquery)
        self.assertEqual(dquery, DASQuery(iquery))
        self.assertEqual(dquery, DASQuery(dquery))
        self.assertEqual(dquery, DASQuery(dquery.mongo_query))
        self.assertEqual(dquery, DASQuery(dquery.storage_query))

    def test_query_properties(self):
        "Test DAS query properties"
        iquery = 'file dataset=/a/b/c instance=global'
        query  = DASQuery(iquery)
        self.assertEqual('global', query.instance)

        iquery = 'file dataset=/a/b/c system=dbs'
        query  = DASQuery(iquery)
        self.assertEqual('dbs', query.system)

        iquery = 'file dataset=/a/b/c | unique | grep file.name'
        query  = DASQuery(iquery)
        filters = query.filters
        filters.sort()
        self.assertEqual(['file.name', 'unique'], filters)

    def test_query_str_repr(self):
        "Test DAS query str/repr method"
        mquery = {'fields':['file'], 'spec':{u'dataset.name':'/a/b/c'}}
        iquery = 'file dataset=/a/b/c'
        msg = "<query: %s, mongo_query: %s>" % (iquery, mquery)
        query  = DASQuery(iquery)
        self.assertEqual(msg, repr(query))
        sys.stdout = StringIO()
        print query, # do not append new line
        expect = sys.stdout.getvalue()
        self.assertEqual(expect, iquery)

    def test_query_representations(self):
        "Test different DAS query representations"
        iquery = 'file dataset=/a/b/c'
        mquery = {'fields':['file'], 'spec':{'dataset.name':'/a/b/c'}}
        squery = {'fields':['file'],
                        'spec':[{'key':'dataset.name', 'value':'"/a/b/c"'}]}

        q1 = DASQuery(iquery)
        q2 = DASQuery(mquery)
        q3 = DASQuery(squery)

        self.assertEqual(iquery, q1.query)
        self.assertEqual(mquery, q1.mongo_query)
        self.assertEqual(squery, q1.storage_query)

        self.assertEqual(iquery, q2.query)
        self.assertEqual(mquery, q2.mongo_query)
        self.assertEqual(squery, q2.storage_query)

        self.assertEqual(iquery, q3.query)
        self.assertEqual(mquery, q3.mongo_query)
        self.assertEqual(squery, q3.storage_query)

    def test_encode(self):
        "test query encoding"
        q1 = {'fields':None, 'spec':{'a.b.c':'ghi', 'd.e.f': 'jkl'}}
        q2 = {'fields':None, 'spec':{'a.b.c':'ghi', 'd.e.f': 'jkl'}}
        sq1 = DASQuery(q1).storage_query
        sq2 = encode_mongo_query(q2)
        self.assertEqual(json.JSONEncoder(sort_keys=True).encode(sq1),
                         json.JSONEncoder(sort_keys=True).encode(sq2))

    def test_decode(self):
        "test query decoding"
        sq1 = {'fields':None,
               'spec':[{'key':'a.b.c', 'value': '"ghi"'},
                       {'key':'d.e.f', 'value':'"jkl"'}]}
        sq2 = {'fields':None,
               'spec':[{'key':'a.b.c', 'value': '"ghi"'},
                       {'key':'d.e.f', 'value':'"jkl"'}]}
        q1 = DASQuery(sq1).mongo_query
        q2 = decode_mongo_query(sq2)
        self.assertEqual(json.JSONEncoder(sort_keys=True).encode(q1),
                         json.JSONEncoder(sort_keys=True).encode(q2))

    def test_qhash(self):
        "Test qhash property"
        sq1 = {'fields':None,
               'spec':[{'key':'a.b.c', 'value': '"ghi"'},
                       {'key':'d.e.f', 'value':'"jkl"'}]}
        sq2 = {'fields':None,
               'spec':[{'key':'a.b.c', 'value': '"ghi"'},
                       {'key':'d.e.f', 'value':'"jkl"'}]}
        qh1 = DASQuery(sq1).qhash
        qh2 = genkey(sq2)
        self.assertEqual(qh1, qh2)

    def test_bare_query(self):
        "Test bare query method"
        q1 = {'fields':None, 'spec':{'a.b.c':'ghi', 'd.e.f': 'jkl'}, 'filters':['foo']}
        q2 = {'fields':None, 'spec':{'a.b.c':'ghi', 'd.e.f': 'jkl'}, 'filters':['foo']}

        bq1 = DASQuery(q1).to_bare_query().mongo_query
        bq2 = DASCORE.bare_query(q2)

        self.assertEqual(json.JSONEncoder(sort_keys=True).encode(bq1),
                         json.JSONEncoder(sort_keys=True).encode(bq2))

    def test_loose_query(self):
        "Test loose property of DAS query"
        iquery = "file dataset=/a/b/c"
        mquery = {'fields':['file'], 'spec':{'dataset.name':'/a/b/c*'}}
        
        result = DASQuery(iquery).loose_query
        self.assertEqual(mquery, result)

    def test_pattern_query(self):
        "Test pattern property of DAS query"
        val    = '/a/b/c'
        query  = 'file dataset=%s' % val
        pat    = re.compile('^%s.*' % val)
        mquery = {'fields':['file'], 'spec':{'dataset.name':pat}}
        pattern_query = DASQuery(query).pattern_query
        result = pattern_query['spec']['dataset.name']
        self.assertEqual(pat.pattern, result.pattern)

if __name__ == '__main__':
    unittest.main()
