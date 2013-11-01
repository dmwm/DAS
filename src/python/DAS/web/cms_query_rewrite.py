#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103,W0511
"""
Handles the simple case of query rewriting which can be accomplished
through one nested query which retrieves entity by it's PK.
"""

from pprint import pprint

from DAS.core.das_query import DASQuery
from DAS.core.das_ql import das_record_keys
from DAS.utils.das_config import das_readconfig

from DAS.keywordsearch.metadata.schema_adapter_factory import getSchema
from DAS.keywordsearch.tokenizer import get_keyword_without_operator as \
    get_filter_name


class CMSQueryRewrite(object):
    """
    Handles the simple case of query rewriting which can be accomplished
    through one nested query which retrieves entity by it's PK.
    """

    _MSG_TMPL = \
        '''
        DAS (or underlying services) do not support this query yet.
        Still you can run multiple queries and combine their results:

        %(q1_str)s
        for each result:
            %(q2_str)s  (replace <PK> with value of %(pk)s from first query)

        Combination of the two queries will give the results expected,
        except for aggregations which have to be implemented manually.
        '''
    # get the link to CLI documentation
    _cfg = das_readconfig()
    CLI_LINK = _cfg.get('query_rewrite', {}).get('dasclient_doc_url', '')
    if CLI_LINK:
        _MSG_TMPL += \
            '''

            See documentation on Command Line Interface:
            %(cli)s
            '''

    def __init__(self, cms_rep):
        self.cms_rep = cms_rep
        self.dasmgr = self.cms_rep.dasmgr
        self.entity_names = self._build_short_daskeys(self.dasmgr)
        # schema adapter from kws
        # TODO: get_field_list_for_entity_by_pk could be moved to DAS Core or...
        self.schema_adapter = getSchema(dascore=self.dasmgr)

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
        """
        returns a list of fields in the results of dasquery (must be in cache)
        """

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

        msg = self._MSG_TMPL % {'q1_str': self.convert2dasql(q1),
                                'q2_str': self.convert2dasql(q2),
                                'pk': pk,
                                'cli': self.CLI_LINK}
        return msg

    def check_fields(self, dasquery):
        """
        if not all of required fields are available, check if they could be
        obtained trough a nested query on PK. if so return a message to user.

        e.g. transforms:
            dataset dataset=/*/*reco*/* | grep dataset.name, dataset.nevents>3

        into:
            dataset dataset=/*/*reco*/* | grep dataset.name
            and:   dataset dataset=<PK> | grep dataset.nevents>3
        """
        DEBUG = False

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
            pprint(['DASQUERY:', dasquery.query])
            pprint(['RESULT ENTITY:', entity])
            pprint(['FILTERS FOR DAS QUERY:', dasquery.filters])
            pprint(['PARAMS FOR DAS QUERY:', dasquery.params()])
            pprint(['Feilds available in Current Query:', fields_available])

        pks_of_entity = list(set(self.cms_rep.dasmapping.mapkeys(entity)) &
                             fields_available)

        # try any of PKs available
        for pk in pks_of_entity:
            # list of fields for given entity retrieved by PK
            # TODO: this is not differentiated by PK yet in schema adapter...
            fields_in_nested_by_pk = \
                self.schema_adapter.get_field_list_for_entity_by_pk(entity, pk)

            query_rewritable = q_fields_missing.issubset(
                set(fields_in_nested_by_pk))

            # if all fields that are still missing,
            #  are available in query='entity PK=pk'
            if query_rewritable and q_fields_missing:
                print 'Rewrite OK'
                return self._do_query_rewrite(dasquery, fields_available, pk)

        return False

    @classmethod
    def _build_short_daskeys(cls, dascore):
        """
        prepares data need for converting back into short das keys
        (e.g. run.run_number->run).

        P.S. Even if in most cases removing everything after '.' shall work,
        I think there was some usecase where it didn't...
        """

        # init field mappings
        entity_names = {}
        for das_key in dascore.das_keys():  # the short daskeys
            long_daskeys = dascore.mapping.mapkeys(das_key)

            for entity_long in long_daskeys:
                entity_names[entity_long] = das_key
        return entity_names

    def convert2dasql(self, q):
        """
        transform DASQuery object back into DASQL (human readable form)
        """
        fields = q.mongo_query.get('fields', []) or []
        exclude = das_record_keys()
        lookups = (f for f in fields
                   if f not in exclude)

        das_conditions = q.mongo_query.get('spec', {}).iteritems()

        short_daskey = lambda ldaskey: \
            self.entity_names.get(ldaskey, ldaskey)

        dasql = ','.join(lookups)
        dasql += ' '  # space between lookup and conditions
        dasql += ' '.join('%s=%s' % (short_daskey(k), v)
                          for k, v in das_conditions)

        # add post_filters
        if q.filters:
            dasql += ' | grep ' + ', '.join(q.filters)

        # not implemented:  language specials - between, date
        #  (however these are not supported by keyword search neither)
        # also aggregators (including unique) - left for user to implement
        return dasql.strip()
