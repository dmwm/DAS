#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
main module for Keyword Search
"""
from math import exp
import pprint

from cherrypy import thread_data, request

from DAS.keywordsearch.config import USE_LOG_PROBABILITIES, DEBUG, \
    MINIMAL_DEBUG
from DAS.keywordsearch.tokenizer import tokenize, cleanup_query
from DAS.keywordsearch.presentation.result_presentation import \
    result_to_dasql, dasql_to_nl
from DAS.keywordsearch.entry_points import get_entry_points
from DAS.keywordsearch.metadata.schema_adapter_factory import get_schema
from DAS.keywordsearch.config import K_RESULTS_TO_STORE
from DAS.keywordsearch.rankers.exceptions import TimeLimitExceeded
from DAS.keywordsearch.whoosh.ir_entity_attributes import \
    load_index, build_index


class KeywordSearch(object):
    """ Keyword Search """
    schema = None

    def __init__(self, dascore):
        self.schema = get_schema(dascore)

        # import and initialize the ranker
        from DAS.extensions import fast_recursive_ranker
        self.ranker = fast_recursive_ranker
        self.ranker.initialize_ranker(self.schema)

        # build and load the whoosh index (listing fields in service outputs)
        build_index(self.schema.list_result_fields())
        load_index()

    @classmethod
    def set_dbs_inst(cls, dbs_inst):
        """ store dbs_inst in request """
        request.dbs_inst = dbs_inst

    @classmethod
    def tokenize_query(cls, query):
        """ CLean up and Tokenize the query """
        if not isinstance(query, unicode) and isinstance(query, str):
            query = unicode(query)

        clean_query = cleanup_query(query)
        tokens = tokenize(query)
        if DEBUG:
            print 'Query:', query
            print 'CLEAN Query:', clean_query
            print 'TOKENS:', tokens
        return query, tokens

    def process_results(self, keywords, query):
        """
        prepare the results to be shown
        """
        results = thread_data.results[:]
        if DEBUG:
            print "============= Results for: %s ===" % query

        # did we store all results?
        if K_RESULTS_TO_STORE:
            # if not, we used heap, where items are tuples: (score, result)
            results = [item[1] for item in results]
        results.sort(key=lambda item: item['score'], reverse=True)
        #print 'len(results)', len(results)
        #print '------'
        best_scores = {}
        get_best_score = lambda scores, q: \
            scores.get(q, {'score': -float("inf")})['score']

        for res in results:
            result = result_to_dasql(res)
            result['query_in_words'] = dasql_to_nl(result['das_ql_tuple'])
            result['query_html'] = result_to_dasql(res, frmt='html')['query']
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
            _get_score = lambda item: item['score']
            max_score = _get_score(max(best_scores, key=_get_score))

            # for displaying the score bar, we want to obtain scores <= 1.0
            score_norm = max(1.0, max_score)
            for res in best_scores:
                # TODO: len_normalized_.. is not used. only needed in old ranker
                res['len_normalized_score'] = _get_score(res)
                res['scorebar_normalized_score'] = _get_score(res) / score_norm
        if DEBUG:
            print '\n'.join(
                '%.2f: %s' % (r['score'], r['result']) for r in best_scores)
        return best_scores

    def search(self, query, dbs_inst, timeout=5):
        """
        Performs the keyword search.
        Once timeout is passed, partial results are returned.
        returns: (error or None, result_list)
        """
        self.set_dbs_inst(dbs_inst)
        query, tokens = self.tokenize_query(query)
        keywords = [kw.strip() for kw in tokens
                    if kw.strip()]
        chunks, schema_ws, values_ws = get_entry_points(keywords)

        if MINIMAL_DEBUG:
            print '============= Q: %s, tokens: %s ' % (query, str(tokens))
            print '============= Schema mappings =========='
            pprint.pprint(schema_ws)
            print '=============== Values mappings ============'
            pprint.pprint(values_ws)

        thread_data.results = []
        # static per request
        thread_data.keywords_list = keywords

        err = None
        try:
            self.ranker.perform_search(schema_ws, values_ws, kw_list=keywords,
                                       chunks=chunks, time_limit=timeout)
        except TimeLimitExceeded as exc:
            print 'time limit exceeded, still returning some results...'
            print exc
            err = exc

        return err, self.process_results(keywords, query)
