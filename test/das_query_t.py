
import unittest
from DAS.utils.das_query import DASQuery
from DAS.core.das_mongocache import encode_mongo_query, decode_mongo_query, loose
import DAS.utils.jsonwrapper as json
from DAS.utils.utils import genkey
from DAS.core.das_core import DASCore

DASCORE = DASCore()

class testDASQuery(unittest.TestCase):
    def test_encode(self):
        q1 = {'fields':None, 'spec':{'a.b.c':'ghi', 'd.e.f': 'jkl'}}
        q2 = {'fields':None, 'spec':{'a.b.c':'ghi', 'd.e.f': 'jkl'}}
        sq1 = DASQuery(mongo_query=q1).storage_query
        sq2 = encode_mongo_query(q2)
        
        self.assertEqual(json.JSONEncoder(sort_keys=True).encode(sq1),
                         json.JSONEncoder(sort_keys=True).encode(sq2))
        
    def test_decode(self):
        sq1 = {'fields':None, 'spec':[{'key':'a.b.c', 'value': '"ghi"'}, {'key':'d.e.f', 'value':'"jkl"'}]}
        sq2 = {'fields':None, 'spec':[{'key':'a.b.c', 'value': '"ghi"'}, {'key':'d.e.f', 'value':'"jkl"'}]}
        q1 = DASQuery(storage_query=sq1).mongo_query
        q2 = decode_mongo_query(sq2)
        
        self.assertEqual(json.JSONEncoder(sort_keys=True).encode(q1),
                         json.JSONEncoder(sort_keys=True).encode(q2))
        
    def test_qhash(self):
        sq1 = {'fields':None, 'spec':[{'key':'a.b.c', 'value': '"ghi"'}, {'key':'d.e.f', 'value':'"jkl"'}]}
        sq2 = {'fields':None, 'spec':[{'key':'a.b.c', 'value': '"ghi"'}, {'key':'d.e.f', 'value':'"jkl"'}]}
        
        qh1 = DASQuery(storage_query=sq1).qhash
        qh2 = genkey(sq2)
        
        self.assertEqual(qh1, qh2)
        
    def test_bare_query(self):
        q1 = {'fields':None, 'spec':{'a.b.c':'ghi', 'd.e.f': 'jkl'}, 'filters':['foo']}
        q2 = {'fields':None, 'spec':{'a.b.c':'ghi', 'd.e.f': 'jkl'}, 'filters':['foo']}
        
        bq1 = DASQuery(mongo_query=q1).to_bare_query().mongo_query
        bq2 = DASCORE.bare_query(q2)
        
        self.assertEqual(json.JSONEncoder(sort_keys=True).encode(bq1),
                         json.JSONEncoder(sort_keys=True).encode(bq2))
        
    def test_loose_query(self):
        q1 = {'fields':None, 'spec':{'a.b.c':'ghi', 'd.e.f': 'jkl'}}
        q2 = {'fields':None, 'spec':{'a.b.c':'ghi', 'd.e.f': 'jkl'}}
        
        lq1 = DASQuery(mongo_query=q1).to_loose_query().mongo_query
        lq2 = loose(q2)
        
        self.assertEqual(json.JSONEncoder(sort_keys=True).encode(lq1),
                         json.JSONEncoder(sort_keys=True).encode(lq2))

    
if __name__ == '__main__':
    unittest.main()