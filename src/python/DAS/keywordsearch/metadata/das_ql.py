#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
defines DASQL and keyword search features, e.g. what shall be considered as:
* word
* simple operators
* aggregation operators (not implemented)

"""

import itertools

KWS_OPERATORS = r'(>=|<=|<|>|=)'
WORD = r'[a-zA-Z0-9_\-*/.@:#]+'
AGGR_OPERATORS = [
    #'grep': [
    #    {'type': 'filter',
    #     'synonyms': ['filter', 'where']},
    #],
    {'type': 'aggregator',
     'op': 'avg(%(field)s)',
     'name': 'avg',
     'synonyms': ['average', ]},

    {'type': 'aggregator',
     'op': 'count(%(field)s)',
     'name': 'count',
     'synonyms': ['how many', ]},

    {'type': 'aggregator',
     'op': 'min(%(field)s)',
     'name': 'min',
     'synonyms': ['minimum', 'smallest', ]},

    {'type': 'aggregator',
     'op': 'max(%(field)s)',
     'name': 'max',
     'synonyms': ['largest', 'max', 'maximum']},

    {'type': 'aggregator',
     'op': 'sum(%(field)s)',
     'name': 'sum',
     'synonyms': ['total', 'total sum']},
    {'type': 'aggregator',
     'op': 'median(%(field)s)',
     'name': 'median',
     'synonyms': ['median']},

    {'synonyms': ['order by', 'sort by', ], # largest *Zmm* dataset -> largest dataset Zmm
     'op': 'sort %(field)s',
     'name': 'sort',
     'type': 'sort'}
]

for op in AGGR_OPERATORS:
    op['synonyms_all'] = op['synonyms']
    op['synonyms_all'].append(op['name'])


def flatten(list_of_lists):
    """Flatten one level of nesting"""
    return itertools.chain.from_iterable(list_of_lists)


def get_operator_synonyms():
    """ return synonyms for das aggregation operators (not used yet) """
    return flatten(op.get('synonyms_all', [])
                   for op in AGGR_OPERATORS)
