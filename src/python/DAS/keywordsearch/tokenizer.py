#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
__author__ = 'vidma'

import re

from nltk.internals import  convert_regexp_to_nongrouping

kws_operators = r'(>=|<=|<|>|=)'


def cleanup_query(query):
    """
    Returns cleaned query by applying a number of transformation patterns
    that removes spaces and simplifies the conditions

    >>> cleanup_query('number of events = 33')
    'number of events=33'

    >>> cleanup_query('number of events >    33')
    'number of events>33'

    # TODO: new trouble
    number of events more than 33
    with more events than 33
    with more than 33 events
    with 100 or more events
    >>> cleanup_query('more than 33 events')
    '>33 events'


    >>> cleanup_query('X more than 33 events')
    'X>33 events'


    """
    # TODO: preprocess the query
    replacements = {
        # get rid of multiple spaces
        r'\s+': ' ',

        'r(find|show|display)( me)\s?': '',

        # transform word-based operators
        r'\s?more than (?=\d+)': '>',
        r'\s?more or equal( than| to)? (?=\d+)': '>',

        r'\s?less than (?=\d+)': '<',
        r'\s?less or equal( to| than)? (?=\d+)': '<',

        r'\s?equals?( to)? (?=\d+)': '=',

        # remove extra spaces
        r'\s*=\s*': '=',
        r'\s*>\s*': '>',
        r'\s*>=\s*': '>=',
        r'\s*<=\s*': '<=',
        r'\s*<\s*': '<',
        }
    # TODO: compile regexps
    #TODO: this is useful for one term keywords, but more complex for multi-keyword ones (which are present for post-filters on API results)
    # TODO: shall we do chunking before anything else? but it is not reliable
    for regexp, repl in replacements.items():
        query = re.sub(regexp, repl, query)
    return query



def get_keyword_without_operator(keyword):
    '''
    splits keyword on operator

    >>> get_keyword_without_operator('number of events >= 10')
    'number of events'

    >>> get_keyword_without_operator('dataset')
    'dataset'

    >>> get_keyword_without_operator('dataset=Zmm')
    'dataset'
    '''
    global kws_operators
    return re.split(kws_operators, keyword)[0].strip()


def get_operator_and_param(keyword):
    '''
    splits keyword on operator

    >>> get_operator_and_param('number of events >= 10')
    {'param': '10', 'op': '>='}

    >>> get_operator_and_param('dataset')


    >>> get_operator_and_param('dataset=Zmm')
    {'param': 'Zmm', 'op': '='}

    '''
    global kws_operators
    parts = re.split(kws_operators, keyword)
    # e.g. parts = ['number of events ', '>=', ' 10']
    if len(parts) == 3:
        return {'op': parts[1].strip(),
                'param': parts[2].strip()}

    #print parts
    return None


def tokenize(query):
    """
    tokenizes the query retaining the phrases in brackets together
    it also tries to group "word operator word" sequences together, such as

    "number of events">10 or dataset=/Zmm/*/raw-reco

    so it could be used for further processing.

    special characters currently allowed in data values include: _*/-

    For example:

    >>> tokenize('file dataset=/Zmm*/*/raw-reco lumi=20853 nevents>10 "number of events">10 /Zmm*/*/raw-reco')
    ['file', 'dataset=/Zmm*/*/raw-reco', 'lumi=20853', 'nevents>10', 'number of events>10', '/Zmm*/*/raw-reco']

    >>> tokenize('file dataset=/Zmm*/*/raw-reco lumi=20853 dataset.nevents>10 "number of events">10 /Zmm*/*/raw-reco')
    ['file', 'dataset=/Zmm*/*/raw-reco', 'lumi=20853', 'dataset.nevents>10', 'number of events>10', '/Zmm*/*/raw-reco']

    >>> tokenize("file dataset=/Zmm*/*/raw-reco lumi=20853 dataset.nevents>10 'number of events'>10 /Zmm*/*/raw-reco")
    ['file', 'dataset=/Zmm*/*/raw-reco', 'lumi=20853', 'dataset.nevents>10', 'number of events>10', '/Zmm*/*/raw-reco']


    >>> tokenize('user=vidmasze@cern.ch')
    ['user=vidmasze@cern.ch']
    """
    #TODO: if needed we may add support for parentesis e.g. sum(number of events)

    global kws_operators
    operators = kws_operators

    query = cleanup_query(query)
    # first remove extra spaces
    #operators = r'[=><]{1,2}'

    word = r'[a-zA-Z0-9_\-*/.@]+'
    #word =
    regexp =\
    r'''
    "[^"]+" %(operators)s %(word)s | # word in brackets plus operators
    "[^"]+"  |  # word in brackets

    '[^']+' %(operators)s %(word)s | # word in brackets plus operators
    '[^']+'  |  # word in brackets

    %(word)s %(operators)s %(word)s | # word op word
    %(word)s | # word
    \S+" # any other non-whitespace sequence
    ''' % locals()
    regexp = convert_regexp_to_nongrouping(regexp)
    regexp = re.compile(regexp, re.VERBOSE)
    # re.findall(regexp,  'file dataset=dataset lumi=20853 nevents>10 "number ofevents">10 /Zmm*/*/raw-reco')

    # TODO: remove brackets?
    # TODO: split on operator?
    return [m.replace('"', '').replace("'", '') for m in re.findall(regexp,  query)]

if __name__ == '__main__':
    print tokenize('file dataset=/Zmm*/*/raw-reco lumi=20853 nevents>10 "number of events">10 /Zmm*/*/raw-reco')
    print tokenize('file dataset=/Zmm*/*/raw-reco lumi=20853 dataset.nevents>10 "number of events">10 /Zmm*/*/raw-reco')
    print tokenize("file dataset=/Zmm*/*/raw-reco lumi=20853 dataset.nevents>10 'number of events'>10 /Zmm*/*/raw-reco")
    import doctest
    doctest.testmod()

