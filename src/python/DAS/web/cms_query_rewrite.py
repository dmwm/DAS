#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0301,C0103

from pprint import pprint

from DAS.core.das_query import DASQuery
from DAS.keywordsearch.metadata.schema_adapter_factory import getSchema
from DAS.keywordsearch.tokenizer import get_keyword_without_operator as \
    get_filter_name

class CMSQueryRewrite(object):
    """
    Handles the simple case of query rewriting which can be accomplished
    through one nested query which retrieves entity by it's PK.
    """
    def __init__(self, cms_rep):
        self.cms_rep = cms_rep
        self.dasmgr = self.cms_rep.dasmgr
        # schema adapter from kws
        # TODO: get_field_list_for_entity_by_pk could be moved to DAS Core or...
        self.schema_adapter = getSchema(dascore=self.cms_rep.dasmgr)


    def _get_one_row_with_all_fields(self, dasquery):
        """
        returns a query result ignoring the grep filter(s)
        """
        mongo_query = dasquery.mongo_query.copy()
        mongo_query['filters'] = {}
        dasquery = DASQuery(mongo_query)

        data = list(self.dasmgr.get_from_cache(dasquery, idx=0, limit=1))
        if len(data):
            return data[0]

    def get_fields_in_query_result(self, dasquery):
        '''
        returns a list of fields in the results of dasquery (must be in cache)
        '''

        # if we have filter/aggregator get one row from the given query
        # this requires qhash to be in cache
        if dasquery.mongo_query:
            row = self._get_one_row_with_all_fields(dasquery)
            return self.cms_rep.get_result_fieldlist(row)

        return []


    def _do_query_rewrite(self, q, fields_avail, pk):
        """
        generates a nested query that:
         * requests entity by PK
         * projects the missing fields
        """
        q_filters = q.filters

        # find all lookup (primary keys) for given das entity
        # It is safe to combine the queries
        # TODO: add actual PK
        filters_first = [f for f in q_filters
                         if get_filter_name(f) in fields_avail]
        filters_nested = [f for f in q_filters
                          if get_filter_name(f) not in fields_avail]

        q1_mongo = q.mongo_query.copy()
        q1_mongo['filters'] = {
            'grep': list(set(filters_first) | set([pk, ])),
        }
        q1 = DASQuery(q1_mongo)
        q2 = q.mongo_query.copy()
        q2['spec'] = {pk: '<PK>'}
        q2['filters'] = {'grep': list(filters_nested)}
        q2 = DASQuery(q2)

        msg = '''
                DAS (or underlying services) do not support this query yet.
                Still you can run multiple queries and combine their results:

                    query: %(q1_str)s
                    for each result:
                        query: %(q2_str)s   (replacing <PK> with value of %(pk)s returned)

                Combination of results from the two queries will give you the results expected,
                     except for aggregations which have to be implemented manually.
                See documentation on <a href="%(cli)s">Command Line Interface</a>
                ''' % {'q1_str': q1.convert2dasql(self.dasmgr),
                       'q2_str': q2.convert2dasql(self.dasmgr),
                       'pk': pk,
                       'cli': 'https://cms-http-group.web.cern.ch/cms-http-group/apidoc/das/current/das_client.html'
                }
        #print msg
        return msg

    def check_filter_existence(self, dasquery):
        '''
        run dataset=/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM | grep  run.bfield

        >>> False
        '''
        DEBUG = True

        if not dasquery.filters:
            return

        fields_available = set(self.get_fields_in_query_result(dasquery))

        # TODO: shall we care about compound lookups?
        entity = dasquery.mongo_query['fields'][0]

        q_filters = dasquery.filters
        q_fieldset = set(get_filter_name(field) for field in q_filters)
        q_fields_missing = q_fieldset - fields_available

        if not q_fields_missing:
            # no rewrite needed
            return

        if DEBUG:
            pprint(['DASQUERY:', dasquery.mongo_query])
            pprint(['RESULT ENTITY:', entity])
            pprint(['FILTERS FOR DAS QUERY:', dasquery.filters])
            pprint(['PARAMS FOR DAS QUERY:', dasquery.params()])
            pprint(['Feilds available in Current Query:', fields_available])

        # TODO: here I may need to privide a particular PK
        pks_of_entity = list(set(self.cms_rep.dasmapping.mapkeys(entity)) \
                             & fields_available)

        # try any of PKs available
        for pk in pks_of_entity:
            # list of fields for given entity retrieved by PK
            # TODO: I think this is not differentiated by PK yet...
            fields_in_nested_by_pk = \
                self.schema_adapter.get_field_list_for_entity_by_pk(entity, pk)

            query_rewritable = q_fields_missing.issubset(
                set(fields_in_nested_by_pk))

            # if all fields that are still missing, are available in query='entity PK=pk'
            if query_rewritable and q_fields_missing:
                print 'Rewrite OK'
                return self._do_query_rewrite(dasquery, fields_available, pk)

        return False