__author__ = 'vidma'

import cherrypy
import urllib
import cgi
import math

from itertools import groupby, imap


from DAS.keywordsearch.search import KeywordSearch
    # import search as keyword_search, init as init_kws

# from DAS.web.utils import HtmlString


avg = lambda l: len(l) and float(sum(l))/len(l)

#import dbs_daemon
#dbs_daemon.KEEP_EXISTING_RECORDS_ON_RESTART =1
#dbs_daemon.SKIP_UPDATES =1



class KeywordSearchHandler(object):

    def _get_link_to_query(self, query, kw_query=''):
        params = cherrypy.request.params.copy()
        params['input'] = query

        # preserve keyword query which led to certain structured query
        if kw_query:
            params['kwquery'] = kw_query

        das_url = '/das/request?' + urllib.urlencode(params)
        return das_url


    def __init__(self, dascore):
        if not dascore:
            raise  Exception("dascore needed")
        self.kws = KeywordSearch(dascore)



    def _prepare_score_bar(self, q):
        '''
        prepares the score bar for each query (max_w & w is in pixels)
        '''

        max_w = 50
        min_w = 3
        score = max(min(q['len_normalized_score'], 1.0), 0.0)
        w = math.floor((max_w - min_w) * q['scorebar_normalized_score']) + min_w
        color_class = (score < 0.35) and 'low' or \
                      (score < 0.60) and 'avg' or 'high'
        return {'max_w': max_w,
                    'w': w,
                    'style': color_class,
                    'score': q['len_normalized_score']}

    def _prepare_trace(self, q):
        '''
        used for debugging, is passed as escaped html string to be later used by javascript
         (implementation doesn't matter much)
        '''
        t = list(q['trace'])
        t.sort()
        thtml = \
            '<ul style="width: 500px;background-color: #fff;"><li>' + \
            '</li><li>'.join(str(item) for item in t) + \
            '</li></ul>' + \
            'score: %.2f; query len norm score (-inf; ~1.0): %.2f' % (
                q['score'], q['len_normalized_score'])

        return cgi.escape(thtml,  quote=True)


    def _get_top_entities(self, proposed_queries, top_k =5):
        ''' returns k top-scoring  entities '''

        #keyfunc =lambda x: x['entity']
        # = groupby(sorted(proposed_queries, key=keyfunc), key=keyfunc)
        #get_max_score = lambda x: max(x, key=lambda x: x['score'])

        #best_scores = ( (g, imap(get_max_score, vals))
        #                for (g, vals) in groups)

        best_scores = {}
        for r in proposed_queries:
            best_scores[r['entity']] = max(r['score'],
                                             best_scores.get(r['entity']))

        best_scores = sorted(best_scores.items(),
                             key=lambda (g, score): score, reverse=True)
        hi_score_result_types = [g for (g, score) in best_scores[:top_k]]

        if len(hi_score_result_types) < 2:
            return []

        hi_score_result_types.append('any')
        return hi_score_result_types

    def handle_search(self, webm, query, inst,  initial_exc_message = '', dbsmngr=None, is_ajax=False):
        '''
        performs the search, and renders the search results
        '''
        proposed_queries = self.kws.search(query, inst, dbsmngr= dbsmngr)

        # no need for result type filter if # of results is low

        # get top 5 entity types
        hi_score_result_types = self._get_top_entities(proposed_queries) \
                                if len(proposed_queries) > 6 else False

        # process the results
        for q in proposed_queries:
            q['link'] = self._get_link_to_query(q['result'], kw_query=query)
            q['trace'] = self._prepare_trace(q)
            q['bar'] = self._prepare_score_bar(q)


        # TODO: add user info to logs if available
        # (e.g. certificate auth, to filter out queries submitted by developers)
        initial_err_message ='Initial error message: ' + initial_exc_message

        return webm.templatepage('kwdsearch_results',
                                    msg='',
                                    is_ajax=is_ajax,
                                    initial_err_message =initial_err_message,
                                    proposed_queries = proposed_queries,
                                    hi_score_result_types = hi_score_result_types)
        #return html
