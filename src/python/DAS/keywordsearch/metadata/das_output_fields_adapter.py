#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103,W0511,C0111
# disabled: invalid name, TODOs,missing docstring

import pprint
from collections import defaultdict
from itertools import chain
from DAS.keywordsearch.config import EXCLUDE_RECORDS_WITH_ERRORS

FULL_DEBUG = False

# TODO: DAS Specific fields in results of every entity that signify error, etc
# TODO: DAS conflict could be useful
# TODO: shall this be excluded as well or not? 'das.conflict',
# TODO: KeyLearning:process_query_record got inconsistent system/urn/das_id length

# TODO:currently some of the fields are not refering to the same entity, e.g.
#  u'release': set([u'dataset.name', u'release.name']),
# which is actually coming from APIs that return the parameters..

# ---- TODO: For now, below is still rather ugly -----
das_specific_fields = ['*.error', '*.reason', 'qhash']

entity_names = {}  # dataset.name -> values, user, user.email, user.name
search_field_names = []  # i.e. das key

# a dict by entity of fields in service results
_result_fields_by_entity = {}


def flatten(listOfLists):
    """Flatten one level of nesting"""
    return chain.from_iterable(listOfLists)



def get_outputs_field_list(dascore):
    """
    makes a list of fields available in each DAS entity
    """
    # TODO: compound lookups!
    # TODO: take some titles from DAS integration schema if defined, e.g.
    # site.replica_fraction -->  File-replica presence

    if FULL_DEBUG:
        print 'keylearning collection:', dascore.keylearning.col
        print 'keylearning members:'
        pprint.pprint([item for item in dascore.keylearning.list_members()])
        print 'result attributes (all):'

    fields_by_entity = defaultdict(set)
    for r in dascore.keylearning.list_members():
        # TODO: this may fail
        result_type = dascore.mapping.primary_key(r['system'], r['urn'])
        # if  keylearning is outdated and urn don't exist, skip such records
        if not result_type:
            continue

        result_members = r.get('members', [])
        fields = [m for m in result_members
                  if m not in das_specific_fields and
                  m.replace(result_type, '*') not in das_specific_fields]

        contain_errors = [m for m in result_members
                          if m.endswith('.error')]

        if contain_errors:
            if FULL_DEBUG:
                print 'WARNING: contain errors: ', result_type, '(' + ', '.join(
                r.get('keys', [])) + ')', ':', ', '.join(fields)
            if EXCLUDE_RECORDS_WITH_ERRORS:
                continue

        # TODO: however not all of the "keys" are used
        if FULL_DEBUG:
            print result_type, '(' + ', '.join(r.get('keys', [])) + ')', ':', \
                ', '.join(fields)

        fields_by_entity[result_type] |= set(fields)

    # assign the titles from presentation cache
    titles_by_field = {}

    if FULL_DEBUG:
        print 'presentation cache:'
        pprint.pprint(dascore.mapping.presentationcache)
    for entries_by_entity in dascore.mapping.presentationcache.itervalues():
        for dic in entries_by_entity:
            field_name = dic['das']
            field_title = dic['ui']
            titles_by_field[field_name] = field_title

    if FULL_DEBUG:
        print 'fields by entity'
        pprint.pprint(fields_by_entity)

    # build the result (fields, their titles)
    results_by_entity = defaultdict(dict)
    for entity, fields in fields_by_entity.iteritems():
        for field in fields:
            results_by_entity[entity][field] = {
                'field': field,
                #'field_name': field.split('.')[-1], # last item after .
                # TODO: add synonyms: from predef, ent.val, mapping db
                'title': titles_by_field.get(field, ''),
                #  if there's no title defined  it's probably less important
                'importance': bool(titles_by_field.get(field))
            }

    if FULL_DEBUG:
        print 'results by entity'
        pprint.pprint(results_by_entity)

    return results_by_entity
