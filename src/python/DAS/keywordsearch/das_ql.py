__author__ = 'vidma'

import itertools

kws_operators = r'(>=|<=|<|>|=)'
word = r'[a-zA-Z0-9_\-*/.@:#]+'



# TODO: this is not yet used...
aggr_operators = [
    # TODO: is this needed explicitly?
    #'grep': [
    #    {'type': 'filter',
    #     'synonyms': ['filter', 'where']},
    #],
    {'type': 'aggregator',
     'op': 'avg(%(field)s)',
     'name': 'avg',
     'synonyms': ['average',]},

    # TODO:'number of' is ambiguos with 'number of events' in dataset etc

    {'type': 'aggregator',
     'op': 'count(%(field)s)',
     'name': 'count',
     'synonyms': ['how many', ]
     # TODO: number of -- not unique!! we have 'number of events' field!
    },

    {'type': 'aggregator',
     'op': 'min(%(field)s)',
     'name': 'min',
     # TODO: note, smallest IS NOT unambigous!
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

    # TODO: problem: selecting between max and sort!
    # TODO: e.g. largest dataset vs what is the largest size of dataset

    # TODO: for size fields, smallest/largest

    {'synonyms':
     # TODO: entities that have size!! simpler approach just use expansion, e.g.
     # largest *Zmm* dataset -> largest dataset Zmm
     # TODO:  {'largest (dataset|file|block)': 'ENTITY.size'}
         ['order by', 'sort by', ],
     'op': 'sort %(field)s',
     'name': 'sort',
     'type': 'sort'},

    #{'synonyms':
    #     ['order by %(field)s descending', ], # 'smallest [entity]'
    # 'op': 'sort -%field',
    # 'name': 'sort_desc',
    # 'type': 'sort'}
]

for op in aggr_operators:
    op['synonyms_all'] = op['synonyms']
    op['synonyms_all'].append(op['name'])


def flatten(listOfLists):
    'Flatten one level of nesting'

    return itertools.chain.from_iterable(listOfLists)

def get_operator_synonyms():
    return flatten([op.get('synonyms_all', []) for op in aggr_operators])

