#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
main module for Keyword Search
"""
from math import exp
import pprint

from cherrypy import thread_data, request

from DAS.core.das_process_dataset_wildcards import get_global_dbs_mngr
from DAS.keywordsearch.config import *
from DAS.keywordsearch.tokenizer import tokenize, cleanup_query
from DAS.keywordsearch.presentation.result_presentation import result_to_DASQL, DASQL_2_NL
from DAS.keywordsearch.entry_points import get_entry_points
from DAS.keywordsearch.metadata.schema_adapter_factory import getSchema
from DAS.keywordsearch.config import K_RESULTS_TO_STORE
from DAS.keywordsearch.rankers.exceptions import TimeLimitExceeded


class KeywordSearch:
    def get_missing_required_inputs(self):
        """
        APIs have their input constraints, and in some cases
        only specific combinations of intpus are accepted, for example:

        file run=148126 [requires dataset or block]

        more complex cases:
        file dataset=/Zmm/*/* site=T1_CH_CERN [requires exact dataset name!]
        """
        pass


    schema = None


    def __init__(self, dascore):
        #das_schema_adapter.init(dascore)
        self.schema = getSchema(dascore)

        ranker = 'fast'
        if ranker == 'fast':
            from DAS.extensions import fast_recursive_ranker
            self.ranker = fast_recursive_ranker
        else:
            from DAS.keywordsearch.rankers import simple_recursive_ranker
            self.ranker = simple_recursive_ranker


    def init_dbs_mngr(self, dbsmngr, inst, DEBUG=False):
        # retrieve DBS instance, and store it in request
        # TODO: shall not be part of this function call
        if True:
            if DEBUG: print 'DBS inst parameter:', inst
            if not dbsmngr:
                if isinstance(inst, str):
                    dbsmngr = get_global_dbs_mngr(inst=inst)
                else:
                    dbsmngr = get_global_dbs_mngr()
        request.dbsmngr = dbsmngr
        return dbsmngr


    def tokenize_query(self, query, DEBUG=False):
        if not isinstance(query, unicode) and isinstance(query, str):
            query = unicode(query)
        if DEBUG: print 'Query:', query
        clean_query = cleanup_query(query)
        if DEBUG: print 'CLEAN Query:', clean_query
        tokens = tokenize(query)
        if DEBUG: print 'TOKENS:', tokens
        if DEBUG: print 'Query after cleanup:', query
        # TODO: some of EN 'stopwords' may be quite important e.g.  'at', 'between', 'where'
        return query, tokens


    #@profile
    def process_results(self, keywords, query):
        results = thread_data.results[:]
        if DEBUG:
            print "============= Results for: %s ===" % query


        # did we store all results?
        if K_RESULTS_TO_STORE:
            # if not, we used heap, where items are tuples: (score, result)
            #results.sort(key=lambda item: item[0], reverse=True)
            results = map(lambda item: item[1], results)
        results.sort(key=lambda item: item['score'], reverse=True)
        #print 'len(results)', len(results)
        #print '------'
        best_scores = {}
        get_best_score = lambda scores, q: \
            scores.get(q, {'score': -float("inf")})['score']

        for r in results:
            result = result_to_DASQL(r)
            result[
                'query_in_words'] = DASQL_2_NL(result['das_ql_tuple'])
            result['query_html'] = result_to_DASQL(r, frmt='html')['query']
            query = result['query']

            if USE_LOG_PROBABILITIES:
                if DEBUG:
                    print result['score'], '-->', exp(result['score']), query
                result['score'] = exp(result['score'])

            if get_best_score(best_scores, query) < result['score']:
                best_scores[query] = result
        best_scores = best_scores.values()
        best_scores.sort(key=lambda item: item['score'], reverse=True)
        # normalize scores, if results are non empty
        if best_scores:
            query_len_norm_fact = self.ranker.normalization_factor_by_query_len(keywords)

            _get_score = lambda item: item['score']
            max_score = _get_score(max(best_scores, key=_get_score))


            # for displaying the score bar, we want to obtain scores <= 1.0
            visual_norm_fact = query_len_norm_fact
            max_score_normalized = max_score / query_len_norm_fact
            if max_score_normalized > 1.0:
                visual_norm_fact = max_score_normalized * query_len_norm_fact

            # SCORE is normalized by query length.
            # close to 1 is good, as all keywords are mapped,
            # negative or close to 0 is bad as either no keywords were mapped,
            # or possibly false query interpretation received many penalties

            for r in best_scores:
                r['len_normalized_score'] = _get_score(r) / query_len_norm_fact
                r['scorebar_normalized_score'] = _get_score(
                    r) / visual_norm_fact
        if DEBUG:
            print '\n'.join(
                '%.2f: %s' % (r['score'], r['result']) for r in best_scores)
        return best_scores

    def search(self, query, inst=None, dbsmngr=None, timeout=5):
        """
        Performs the keyword search
        returns: (error or None, result_list)
        """
        #DEBUG = False
        self.init_dbs_mngr(dbsmngr, inst)

        query, tokens = self.tokenize_query(query, DEBUG=DEBUG)

        keywords = [kw.strip() for kw in tokens
                    if kw.strip()]
        chunks, schema_ws, values_ws = get_entry_points(keywords, DEBUG)


        if MINIMAL_DEBUG:
            print '============= Q: %s, tokens: %s ' % (query, str(tokens))
            print '============= Schema mappings (TODO) =========='
            pprint.pprint(schema_ws)
            print '=============== Values mappings (TODO) ============'
            pprint.pprint(values_ws)

        thread_data.results = []

        # static per request
        thread_data.keywords_list = keywords

        err = None
        try:
            # TODO: add time limit into DAS settings
            self.ranker.perform_search(schema_ws, values_ws, kw_list=keywords,
                                       chunks=chunks, time_limit=timeout)
        except TimeLimitExceeded as e:
            print e
            print 'time limit exceeded, still returning some results...'
            err = e

        return err, self.process_results(keywords, query)
