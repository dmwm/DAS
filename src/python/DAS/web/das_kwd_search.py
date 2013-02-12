__author__ = 'vidma'

import cherrypy
import urllib
import cgi
from itertools import *

from DAS.keywordsearch.search import search as keyword_search

from DAS.web.utils import HtmlString


avg = lambda l: len(l) and float(sum(l))/len(l)

import dbs_daemon
dbs_daemon.KEEP_EXISTING_RECORDS_ON_RESTART =1
dbs_daemon.SKIP_UPDATES =1



class KeywordSearchHandler:

    @staticmethod
    def render(webm, msg, html_error=None, tmpl='das_kwdsearch_res'):
        """Helper function which provide error template"""
        if  not html_error:
            return msg
        guide = '' #webm.templatepage('dbsql_vs_dasql', operators=', '.join(das_operators()))

        page = webm.templatepage(tmpl or 'das_ambiguous_html', msg=msg,
                                 base=webm.base, guide=guide)
        return page

    @staticmethod
    def handle_search(webm, query, inst,  initial_exc_message = ''):
        # TODO: DBS instance
        proposed_queries = keyword_search(query, inst)
        html = '<b>DAS is unable  to unambigously interpret your query.'\
              ' Is any of the queries below what you meant?</b><br>\n'


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


        html += '''

        '''

        if len(hi_score_result_types) > 1:
            html += """<div class="select-result-type">Are you searching for:
                        <span class="rt-filters">%(rt-filters)s</span>
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
            q['trace'] = cgi.escape('<ul style="width: 500px;background-color: #fff;"><li>' +'</li><li>'.join([str(item) for item in q['trace']]) + '</li></ul>', quote=True)


        # Equivalent DAS query:
        html += '\n'.join(["""
            <div class="kws-result result-with-entity-%(entity)s">
                %(score).2f: <a class="kws-link" href="%(link)s" target="_blank"
                                    title="Explanation: &lt;br/&gt; %(nl_query_escaped)s">%(query_escaped)s</a>
                <a class="debug" title="%(trace)s">debug</a>
            </div>
            """ % r for r in proposed_queries ])
        # TODO: add user info to logs if available (e.g. certificate auth, to filter out queries submitted by developers)

        html += '<br>\n<br>\nError message: ' + initial_exc_message
        #msg = '<br>\n'.join(proposed_queries)
        html = HtmlString(html)

        return 1, KeywordSearchHandler.render(webm, html, True, tmpl='das_kwdsearch_res')
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
