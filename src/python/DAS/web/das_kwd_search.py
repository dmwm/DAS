__author__ = 'vidma'

import cherrypy
import urllib
import cgi
import math
#from itertools import *

from DAS.keywordsearch.search import search as keyword_search, init as init_kws

from DAS.web.utils import HtmlString


avg = lambda l: len(l) and float(sum(l))/len(l)

import dbs_daemon
dbs_daemon.KEEP_EXISTING_RECORDS_ON_RESTART =1
dbs_daemon.SKIP_UPDATES =1



class KeywordSearchHandler:

    @ staticmethod
    def init(dascore):
        init_kws(dascore)




    @staticmethod
    def handle_search(webm, query, inst,  initial_exc_message = '', dbsmngr=None, is_ajax=False):
        # TODO: DBS instance
        proposed_queries = keyword_search(query, inst, dbsmngr= dbsmngr)

        # get the top 5 entities
        entity_scores = {}
        for r in proposed_queries:
            entity_scores[r['entity']] = max(r['score'], entity_scores.get(r['entity']))
        entity_scores = entity_scores.items();
        entity_scores.sort(key=lambda item: item[1], reverse=True)
        #print entity_scores
        hi_score_result_types = [e[0] for e in entity_scores[:5]]
        # TODO: better use 5 best scoring rts...!

        hi_score_result_types.append('see all')


        html = ''

        if len(hi_score_result_types) > 1:
            html += """
            <div class="select-result-type">
                <span class="tooltip">Are searching for:<span class="classic">You may filter suggestions by the entity they return</span></span>
                        &nbsp;<span class="rt-filters">%(rt-filters)s</span>
                        </div>
                    """ % {
                        'rt-filters': ",&nbsp;".join([
                            """<a href="javascript: filterByResultType('%(rt)s', this)">%(rt)s</a>""" % {'rt': rt}
                            for rt in hi_score_result_types])
                    }

        # append links
        for q in proposed_queries:
            q['query_escaped'] = cgi.escape(q['result'], quote=True)
            q['nl_query_escaped'] = cgi.escape(q['query_in_words'], quote=True)
            q['link'] = KeywordSearchHandler._get_link_to_query(q['result'], kw_query=query)
            print q['link']
            q['trace'] = list(q['trace'])
            q['trace'].sort()
            q['trace'] = cgi.escape('<ul style="width: 500px;background-color: #fff;"><li>' +
                                        '</li><li>'.join([str(item) for item in q['trace']]) +
                                    '</li></ul>' +
                                    'score: %.2f; query len norm score (-inf; ~1.0): %.2f' % (
                                        q['score'], q['len_normalized_score']),
                                    quote=True)
            #
            max_w = 50
            min_w = 3
            score = max(min(q['len_normalized_score'], 1.0), 0.0)
            w = math.floor((max_w - min_w) * q['scorebar_normalized_score']) + min_w
            color_class = (q['len_normalized_score'] < 0.35) and 'low' or \
                    (q['len_normalized_score'] < 0.60) and 'avg' or 'high'

            q['bar'] = ('<div class="score-bar" style="width: %(max_w)dpx;">' + \
                       '  <div class="score-bar-inner score-bar-inner-%(style)s" style="width: %(w)dpx;"></div>'
                       '  <span class="score-num">%(score).2f</span> ' +\
                       '</div>') % {'max_w': max_w,
                                   'w': w,
                                   'style': color_class,
                                   'score': q['len_normalized_score']}


        # Equivalent DAS query:
        html += '\n'.join(["""
            <div class="kws-result result-with-entity-%(entity)s">
                %(bar)s <a class="kws-link" href="%(link)s" target="_blank"
                                    title="Explanation: &lt;br/&gt; %(nl_query_escaped)s">%(query_html)s</a>
                <a class="debug" title="%(trace)s">debug</a>
            </div>
            """ % r for r in proposed_queries ])
        # TODO: add user info to logs if available (e.g. certificate auth, to filter out queries submitted by developers)

        initial_err_message ='Initial error message: ' + initial_exc_message
        #msg = '<br>\n'.join(proposed_queries)
        html = HtmlString(html)

        return 1, webm.templatepage('das_kwdsearch_res', msg=html, is_ajax=is_ajax, initial_err_message=initial_err_message)
        # TODO: html links
        #return html

    @staticmethod
    def _get_link_to_query(query, kw_query=''):
        params = cherrypy.request.params.copy()
        params['input'] = query

        # preserve keyword query which led to certain structured query
        if kw_query:
            params['kwquery'] = kw_query

        das_url = '/das/request?' + urllib.urlencode(params)
        return das_url
