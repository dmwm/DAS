__author__ = 'vidma'

import unittest
from das_kwdsearch_t import KeywordSearchAbstractTester, Timer

class KwdSearchPerformance(KeywordSearchAbstractTester):
    def runQuery(self, query):
        print "running q=", query
        with Timer() as t:
            results = self.kws.search(query, dbsmngr=self.global_dbs_inst)
        print([t.interval, len(results)])
        return t.interval, results

    def test_performance(self):
        self.runQuery('dataset /Zmm where "number of events=3" tier reco group tier0')
        self.runQuery('/Zmm where "number of events"')
        self.runQuery('dataset /Zmm where number of events=3 tier reco group tier0')




    def runTest(self):
        pass



if __name__ == '__main__':
    t = KwdSearchPerformance()
    t.setUp()
    print("=======================\n"
          "set up done. running performance tests")

    import cProfile
    if True:

        cProfile.run("t.test_performance()", filename="kwdsearch_perf.cprofile")
    else:
        unittest.main()
