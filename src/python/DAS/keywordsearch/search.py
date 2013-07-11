#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
main module for Keyword Search
"""

from cherrypy import thread_data, request

from DAS.core.das_process_dataset_wildcards import get_global_dbs_mngr

from DAS.keywordsearch.tokenizer import tokenize, cleanup_query
from DAS.keywordsearch.presentation.result_presentation import *
from DAS.keywordsearch.rankers.simple_recursive_ranker import *
from DAS.keywordsearch.entry_points import get_entry_points
from DAS.keywordsearch.metadata import das_schema_adapter


from heapq import heappush, heappushpop
from DAS.keywordsearch.config import K_RESULTS_TO_STORE

def get_missing_required_inputs():
    """
    APIs have their input constraints, and in some cases
    only specific combinations of intpus are accepted, for example:

    file run=148126 [requires dataset or block]

    more complex cases:
    file dataset=/Zmm/*/* site=T1_CH_CERN [requires exact dataset name!]
    """
    pass






def init(dascore):
    das_schema_adapter.init(dascore)



def init_dbs_mngr(dbsmngr, inst, DEBUG = False):

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


def tokenize_query(query, DEBUG=False):
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



def search(query, inst=None, dbsmngr=None):
    """
    Performs keyword search
    """

    # TODO: add DBS instance as parameter
    #DEBUG = False
    init_dbs_mngr(dbsmngr, inst)

    query, tokens = tokenize_query(query, DEBUG)

    keywords = [kw.strip() for kw in tokens
                if kw.strip()]

    if MINIMAL_DEBUG:
        print '============= Q: %s, tokens: %s ' % (query, str(tokens))

    chunks, schema_ws, values_ws = get_entry_points(keywords, DEBUG)


    #thread_data.results = []
    thread_data.results_dict = []

    # static per request
    thread_data.keywords_list = keywords
    # TODO:  schema_ws, values_ws, is also static, see performance differences...


    generate_schema_mappings(None, [], schema_ws, values_ws,
        kw_list=keywords, kw_index=0, old_score=0, chunks=chunks)


    if DEBUG: print "============= Results for: %s ===" % query

    results =  thread_data.results_dict[:]
    if DEBUG:
        print "RESULTSSSSSSSSSSSSSS"
        print results

    # did we store all results?
    if K_RESULTS_TO_STORE:
        # if not, we used heap, where items are tuples: (score, result)
        #results.sort(key=lambda item: item[0], reverse=True)
        results = map(lambda item: item[1], results)

    results.sort(key=lambda item: item['score'], reverse=True)

    best_scores = {}

    get_best_score = lambda scores, q: \
        scores.get(q, {'score': -float("inf")})['score']


    from math import exp


    for r in results:
        result = result_to_DASQL(r)
        result['query_in_words'] = DASQL_2_NL(result['das_ql_tuple'])
        result['query_html'] = result_to_DASQL(r, frmt='html')['query']
        query = result['query']

        if USE_LOG_PROBABILITIES:
            if DEBUG:
                print result['score'],'-->', exp(result['score']), query
            result['score'] = exp(result['score'])


        if get_best_score(best_scores, query) < result['score']:
            best_scores[query] = result


    best_scores = best_scores.values()
    best_scores.sort(key=lambda item: item['score'], reverse=True)

    # normalize scores, if results are non empty
    if best_scores:
        query_len_norm_fact = normalization_factor_by_query_len(keywords)

        _get_score = lambda item: item['score']
        #min_score = _get_score(min(best_scores, key=_get_score))
        max_score = _get_score(max(best_scores, key=_get_score))

        #normalize = lambda item: (float(item['score']) - min_score) / \
        #                         (max_score - min_score)


        # for displaying the score bar, we want to obtain scores <= 1.0
        visual_norm_fact = query_len_norm_fact
        max_score_normalized = max_score / query_len_norm_fact
        if max_score_normalized > 1.0:
            visual_norm_fact = max_score_normalized * query_len_norm_fact


        for idx, r in enumerate(best_scores):

            # SCORE normalized by query length.
            # close to 1 is good, as all keywords are mapped,
            # negative or close to 0 is bad as either no keywords were mapped,
            # or possibly false query interpretation received many penalties

            best_scores[idx]['len_normalized_score'] = _get_score(r) / query_len_norm_fact
            best_scores[idx]['scorebar_normalized_score'] = _get_score(r) / visual_norm_fact



    if DEBUG:
        print '\n'.join(
                    '%.2f: %s' % (r['score'], r['result']) for r in best_scores)

    return best_scores



    # TODO: feature, the part of string that matches, e.g. dataset=*valid* vs status=valid !

def crap():
    print search('number of events in dataset *Run2012*PromptReco*/AOD')
    # site
    print search('custodial of dataset *Run2012*PromptReco*/AOD')
    print search(
        'how many events there are in lumi section 198952 of *Run2012*PromptReco*/AOD'.replace(
            'how many', 'number of'))
    print search(
        'number of events there are in lumi section 198952 of *Run2012*PromptReco*/AOD')
    print search('global tag *Run2012*PromptReco*/AOD')
    print search('dataset = Zmm  global tag')
    print search(
        'dataset=/GlobalSep07-A/Online-CMSSW_1_6_0_DAQ3/*  run = 20853 number of events')
    print search(
        'dataset run=148126 & site=T1_US_FNAL')
    print search('Zmm "number of events">10')
    print search('Zmm nevents>10')
    print search('Zmm block.nevents>10')

    # TODO: complex
    #print search('what is the custodial site of *Run2012*PromptReco*/AOD')