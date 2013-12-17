#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Gathers the list of fields available in service outputs
"""
import pprint
from collections import defaultdict
from itertools import chain
from DAS.keywordsearch.config import EXCLUDE_RECORDS_WITH_ERRORS

FULL_DEBUG = False
DAS_RESERVED_FIELDS = ['*.error', '*.reason', 'qhash']


def flatten(list_of_lists):
    """Flatten one level of nesting"""
    return chain.from_iterable(list_of_lists)


def is_reserved_field(field, result_type):
    """ returns whether the field is reserved, e.g. *.error, *.reason, qhash """
    return field in DAS_RESERVED_FIELDS or \
        field.replace(result_type, '*') in DAS_RESERVED_FIELDS


def result_contained_errors(rec):
    """ decide whether keylearning record contain errors (i.e. as responses from
    services contained errors) and whether the record shall be excluded """
    errors = any(1 for field in rec.get('members', [])
                 if field.endswith('.error'))
    if errors and FULL_DEBUG:
        print 'WARN: contain errors: ', rec.get('keys'), ':', rec.get('members')
    return errors


def get_outputs_field_list(dascore):
    """
    makes a list of output fields available in each DAS entity
    this is taken from keylearning collection.
    """
    fields_by_entity = defaultdict(set)
    for rec in dascore.keylearning.list_members():
        result_type = dascore.mapping.primary_key(rec['system'], rec['urn'])
        # if keylearning is outdated and urn don't exist, skip such records
        if not result_type:
            continue
        if result_contained_errors(rec) and EXCLUDE_RECORDS_WITH_ERRORS:
            continue

        # build list of fields
        fields = [field for field in rec.get('members', [])
                  if not is_reserved_field(field, result_type)]
        fields_by_entity[result_type] |= set(fields)
        if FULL_DEBUG:
            print result_type, rec.get('keys', []), ':', fields

    # assign the titles from presentation cache
    titles_by_field = {}
    for titles in dascore.mapping.presentationcache.itervalues():
        for entry in titles:
            field_name = entry['das']
            field_title = entry['ui']
            titles_by_field[field_name] = field_title

    # build the result made of fields and their titles
    results_by_entity = defaultdict(dict)
    for entity, fields in fields_by_entity.iteritems():
        for field in fields:
            results_by_entity[entity][field] = {
                'name': field,
                'title': titles_by_field.get(field, ''),
                #  if there's no title defined  it's probably less important
                'importance': bool(titles_by_field.get(field)),
                'lookup': entity  # use lookups
            }

    return results_by_entity


def print_debug(dascore, fields_by_entity, results_by_entity):
    """ verbose output for get_outputs_field_list """
    if FULL_DEBUG:
        print 'keylearning collection:', dascore.keylearning.col
        print 'keylearning members:'
        pprint.pprint([item for item in dascore.keylearning.list_members()])
        print 'presentation cache:'
        pprint.pprint(dascore.mapping.presentationcache)
        print 'fields by entity'
        pprint.pprint(fields_by_entity)
        print 'results by entity'
        pprint.pprint(results_by_entity)
