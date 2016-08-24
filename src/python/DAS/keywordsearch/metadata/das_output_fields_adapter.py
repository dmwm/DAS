#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Gathers the list of fields available in service outputs
"""
from __future__ import print_function
import pprint
from collections import defaultdict
from itertools import chain
from DAS.keywordsearch.config import EXCLUDE_RECORDS_WITH_ERRORS, DEBUG, \
    MINIMAL_DEBUG

FULL_DEBUG = DEBUG
DAS_RESERVED_FIELDS = ['*.error', '*.reason', 'qhash']


def flatten(list_of_lists):
    """Flatten one level of nesting"""
    return chain.from_iterable(list_of_lists)


def is_reserved_field(field, result_type):
    """returns whether the field is reserved, e.g. `*.error`, `*.reason`, `qhash`"""
    return field in DAS_RESERVED_FIELDS or \
        field.replace(result_type, '*') in DAS_RESERVED_FIELDS


def result_contained_errors(rec):
    """decide whether keylearning record contain errors (i.e. as responses from
    services contained errors) and whether the record shall be excluded"""
    errors = any(1 for field in rec.get('members', [])
                 if field.endswith('.error'))
    if errors and FULL_DEBUG:
        print('WARN: contain errors: ', rec.get('keys'), ':', rec.get('members'))
    return errors


def get_titles_by_field(dascore):
    """returns a dict of titles taken from presentation cache"""
    titles_by_field = {}
    for titles in dascore.mapping.presentationcache.values():
        for entry in titles:
            field_name = entry['das']
            field_title = entry['ui']
            titles_by_field[field_name] = field_title

    return titles_by_field


def get_outputs_field_list(dascore):
    """
    makes a list of output fields available in each DAS entity
    this is taken from keylearning collection.
    """
    # build field list for each lookup (entity or their combination)
    fields_by_lookup = defaultdict(set)
    for rec in dascore.keylearning.list_members():
        #result_type = dascore.mapping.primary_key(rec['system'], rec['urn'])
        sys, urn = rec['system'], rec['urn']
        try:
            lookup = ','.join(sorted(dascore.mapping.api_lkeys(sys, urn)))
        except KeyError:
            # if keylearning is outdated and urn don't exist, skip such records
            continue

        if result_contained_errors(rec) and EXCLUDE_RECORDS_WITH_ERRORS:
            continue

        fields_by_lookup[lookup] |= set(field
                                        for field in rec.get('members', [])
                                        if not is_reserved_field(field, lookup))
    # build the result made of fields and their titles
    titles_by_field = get_titles_by_field(dascore)
    field_data_by_lookup = defaultdict(dict)
    for lookup, fields in fields_by_lookup.items():
        for field in fields:
            field_data_by_lookup[lookup][field] = {
                'name': field,
                'title': titles_by_field.get(field, ''),
                #  if there's no title defined  it's probably less important
                'importance': bool(titles_by_field.get(field)),
                'lookup': lookup  # use lookups
            }

    print_debug(dascore, fields_by_lookup, field_data_by_lookup)
    return field_data_by_lookup


def print_debug(dascore, fields_by_entity, results_by_entity):
    """verbose output for get_outputs_field_list"""
    if FULL_DEBUG:
        print('keylearning collection:', dascore.keylearning.col)
        print('keylearning members:')
        pprint.pprint([item for item in dascore.keylearning.list_members()])
        print('presentation cache:')
        pprint.pprint(dascore.mapping.presentationcache)
        print('fields by entity')
        pprint.pprint(fields_by_entity)
        print('results by entity')
        pprint.pprint(results_by_entity)
    elif MINIMAL_DEBUG:
        print('fields by entity')
        pprint.pprint(fields_by_entity)
