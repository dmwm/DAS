#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
main module for Keyword Search
"""
from __future__ import print_function
from math import exp
import pprint

from cherrypy import thread_data, request

from nltk.corpus import stopwords


from DAS.keywordsearch.config import USE_LOG_PROBABILITIES, DEBUG, \
    MINIMAL_DEBUG
from DAS.keywordsearch.tokenizer import tokenize, cleanup_query
from DAS.keywordsearch.presentation.result_presentation import \
    result_to_dasql, dasql_to_nl
from DAS.keywordsearch.metadata.schema_adapter_factory import get_schema
from DAS.keywordsearch.config import K_RESULTS_TO_STORE
from DAS.keywordsearch.rankers.exceptions import TimeLimitExceeded


from DAS.keywordsearch.metadata.input_values_tracker \
    import init_trackers as init_value_trackers
from DAS.keywordsearch.entity_matchers.name_matching \
    import keyword_schema_weights
from DAS.keywordsearch.entity_matchers.value_matching \
    import keyword_value_weights
from DAS.keywordsearch.entity_matchers.kwd_chunks.chunk_matcher \
    import MultiKwdAttributeMatcher


EN_STOPWORDS = stopwords.words('english')


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
        fields = self.schema.list_result_fields()
        self.multi_kwd_searcher = MultiKwdAttributeMatcher(fields)

        # initialize the value trackers (primary_dataset, release, etc)
        init_value_trackers()

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
            print('Query:', query)
            print('CLEAN Query:', clean_query)
            print('TOKENS:', tokens)
        return query, tokens

    @classmethod
    def process_results(cls, query):
        """
        prepare the results to be shown
        """
        results = thread_data.results[:]
        if DEBUG:
            print("============= Results for: %s ===" % query)

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
                    print(result['score'], '-->', exp(result['score']), query)
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
                res['scorebar_normalized_score'] = _get_score(res) / score_norm
        if DEBUG:
            print('\n'.join(
                '%.2f: %s' % (r['score'], r['result']) for r in best_scores))
        return best_scores

    def search(self, query, dbs_inst, timeout=5):
        """
        Performs the keyword search.
        Once timeout is passed, partial results are returned.
        returns: (error or None, result_list)
        """
        self.set_dbs_inst(dbs_inst)
        query, tokens = self.tokenize_query(query)
        keywords = [kw.strip() for kw in tokens if kw.strip()]
        chunks, schema_ws, values_ws = self.get_entry_points(keywords)

        if MINIMAL_DEBUG:
            print('============= Q: %s, tokens: %s ' % (query, str(tokens)))
            print('============= Schema mappings ==========')
            pprint.pprint(schema_ws)
            print('------ chunks ---')
            pprint.pprint(chunks)
            print('=============== Values mappings ============')
            pprint.pprint(values_ws)

        thread_data.results = []
        # static per request
        thread_data.keywords_list = keywords

        err = None
        try:
            self.ranker.perform_search(schema_ws, values_ws, kw_list=keywords,
                                       chunks=chunks, time_limit=timeout)
        except TimeLimitExceeded as exc:
            print('time limit exceeded, still returning some results...')
            print(exc)
            err = exc

        return err, self.process_results(query)

    def get_entry_points(self, tokens):
        """
        calculates the entry points - assignment of individual keywords
         to the corresponding most possible interpretations (not taking into
         account their inter-dependencies)
        """
        keywords = [kw.strip() for kw in tokens
                    if kw.strip()]
        schema_ws = {}
        values_ws = {}
        for kwd_index, kwd in enumerate(keywords):
            kw_value = kw_schema = kwd
            if '=' in kwd and len(kwd.split('=')) == 2:
                kw_schema, kw_value = kwd.split('=')
            schema_ws[kwd] = keyword_schema_weights(kw_schema,
                                                    kwd_idx=kwd_index)
            if kwd not in EN_STOPWORDS and kw_value:
                values_ws[kwd] = keyword_value_weights(kw_value)

        chunks = self.multi_kwd_searcher.generate_chunks(keywords)
        return chunks, schema_ws, values_ws
