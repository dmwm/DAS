#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

from pprint import pprint
from copy import deepcopy

from DAS.core.das_query import DASQuery

from DAS.keywordsearch.metadata.schema_adapter_factory import getSchema
from DAS.keywordsearch.tokenizer import get_keyword_without_operator as get_filter_name
#get_filter_name = lambda x: x


class CMSQueryRewrite(object):
    def __init__(self, cms_rep):
        #self.dasmgr = dasmgr
        self.cms_rep = cms_rep
        # schema adapter from kws
        # TODO: get_field_list_for_entity_by_pk could be moved to DAS Core or somewhere...
        self.schema_adapter = getSchema(dascore=self.cms_rep.dasmgr)


    @property
    def dasmgr(self):
        return self.cms_rep.dasmgr

    def _get_one_row_with_all_fields(self, dasquery):
        """
        returns a query result ignoring the grep filter(s)
        """
        #from copy import deepcopy
        #mongo_query = deepcopy(dasquery.mongo_query)
        mongo_query = dasquery.mongo_query.copy()
        mongo_query['filters'] = {}
        dasquery = DASQuery(mongo_query)

        data = list(self.dasmgr.get_from_cache(dasquery, idx=0, limit=1))
        # print 'get_one_row_all_fields: data=', data
        if len(data):
            return data[0]

    def get_fields_in_query_result(self, dasquery, remove_grep=True):
        '''
        returns a list of available fields in the results of dasquery (must be in cache)
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
        # TODO: add unique
        q1 = DASQuery(q1_mongo)
        # TODO: we should actually leave any filters which are still available!!!
        print 'first query', q1.convert2dasql(self.dasmgr), q1.mongo_query
        q2 = q.mongo_query.copy()
        # das_conditions: pk returned by first query
        q2['spec'] = {pk: '<PK>'}

        # TODO: add other operands (?dont remember what?), not only grep, min, max, unique...
        # (these would have to be post-processed again....)
        q2['filters'] = {u'grep': list(filters_nested)}
        q2 = DASQuery(q2)
        print 'Nested query:', q2.convert2dasql(self.dasmgr), q2.mongo_query

        # TODO: use a better template for this?
        msg = '''
                DAS (and it's underlying services) do not support this query directly yet,
                 however, you could use the instructions bellow to fetch the results yourself.

                you need to:

                    query: %(q1_str)s
                    for each result:
                        query: %(q2_str)s   (replacing <PK> with value of %(pk)s returned)

                    (the combination (join) of results from the two queries will give you the results expected,
                     except the aggregations which you will have to combine yourself).
                ''' % {'q1_str': q1.convert2dasql(self.dasmgr),
                       'q2_str': q2.convert2dasql(self.dasmgr),
                       'pk': pk}
        print msg
        return msg

    def check_filter_existence(self, dasquery):
        '''
        run dataset=/ZMM/Summer11-DESIGN42_V11_428_SLHC1-v1/GEN-SIM | grep  run.bfield

        >>> False
        '''
        DEBUG = True

        if not dasquery.filters:
            return False

        fields_available = set(self.get_fields_in_query_result(dasquery))

        # TODO: shall we care about compound lookups?
        entity = dasquery.mongo_query['fields'][0]

        q_filters = dasquery.filters
        q_fieldset = set(get_filter_name(field) for field in q_filters)

        q_fields_available = q_fieldset & fields_available
        q_fields_missing = q_fieldset - fields_available

        if DEBUG:
            pprint(['DASQUERY:', dasquery.mongo_query])
            pprint(['RESULT ENTITY:', entity])
            pprint(['TODO: FILTERS FOR DAS QUERY:', dasquery.filters])
            pprint(['TODO: PARAMS FOR DAS QUERY:', dasquery.params()])
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