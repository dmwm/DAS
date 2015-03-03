#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Module description:
 - first clean up input keyword query (rm extra spaces, standardize notation)
 - then it tokenizes the query into:
    * individual query terms
    * compound query terms in brackets (e.g. "number of events")
    * phrases: "terms operator value" (e.g. nevent > 1, "number of events"=100)
"""

import re
try:
    from nltk.internals import convert_regexp_to_nongrouping
except:
    from DAS.keywordsearch.nltk_legacy import convert_regexp_to_nongrouping
from DAS.keywordsearch.metadata import das_ql


AGGREGATORS_ENABLED = False
KWS_OPERATORS = das_ql.KWS_OPERATORS
AGGR_OPERATORS = das_ql.AGGR_OPERATORS
WORD = das_ql.WORD
CLEANUP_SUBS = [
    # get rid of multiple spaces
    (r'\s+', ' '),

    (r'^(find|show|tell|display|retrieve|select)( me)?\s?', ''),
    # simplest way to process 'when' questions (attributes *_time;
    # date is almost useless as result type currently (?), could could be input)
    (r'^when\s', 'time '),

    # transform word-based operators
    (r'\s?more than (?=\d+)', '>'),
    (r'\s?more or equal( than| to)? (?=\d+)', '>'),
    (r'\s?less than (?=\d+)', '<'),
    (r'\s?less or equal( to| than)? (?=\d+)', '<'),
    (r'\s?equals?( to)? (?=\d+)', '='),

    # remove extra spaces
    (r'\s*=\s*', '='),
    (r'\s*>\s*', '>'),
    (r'\s*>=\s*', '>='),
    (r'\s*<=\s*', '<='),
    (r'\s*<\s*', '<'),

    # process dates into DAS format (must be preceded by operator, as
    # dataset may also contain dates)
    # e.g. = 2012-02-01 --> 20120201
    (KWS_OPERATORS + r'\s?([1-2][0-9]{3})-([0-1][0-9])-([0-3][0-9])',
     r'\1 \2\3\4')]
CLEANUP_SUBS = list((re.compile(regexp), repl)
                    for regexp, repl in CLEANUP_SUBS)

TOKENIZER_PATTERNS = \
    r'''
    "[^"]+" %(KWS_OPERATORS)s %(WORD)s | # word in brackets plus operators
    "[^"]+"  |  # word in brackets

    '[^']+' %(KWS_OPERATORS)s %(WORD)s | # word in brackets plus operators
    '[^']+'  |  # word in brackets

    %(WORD)s %(KWS_OPERATORS)s %(WORD)s | # word op word
    %(WORD)s | # word
    \S+" # any other non-whitespace sequence
    ''' % locals()
TOKENIZER_PATTERNS = convert_regexp_to_nongrouping(TOKENIZER_PATTERNS)
TOKENIZER_PATTERNS = re.compile(TOKENIZER_PATTERNS, re.VERBOSE)


def cleanup_query(query):
    """
    Returns cleaned query by applying a number of transformation patterns
    that removes spaces and simplifies the conditions

    >>> cleanup_query('number of events = 33')
    'number of events=33'

    >>> cleanup_query('number of events >    33')
    'number of events>33'

    >>> cleanup_query('more than 33 events')
    '>33 events'

    >>> cleanup_query('X more than 33 events')
    'X>33 events'

    >>> cleanup_query('find datasets where X more than 33 events')
    'datasets where X>33 events'

    >>> cleanup_query('=2012-02-01')
    '= 20120201'

    >>> cleanup_query('>= 2012-02-01')
    '>= 20120201'
    """

    for regexp, repl in CLEANUP_SUBS:
        query = re.sub(regexp, repl, query)
    return query


def get_keyword_without_operator(keyword):
    """
    splits keyword on operator

    >>> get_keyword_without_operator('number of events >= 10')
    'number of events'

    >>> get_keyword_without_operator('dataset')
    'dataset'

    >>> get_keyword_without_operator('dataset=Zmm')
    'dataset'
    """
    #global kws_operators
    res = re.split(KWS_OPERATORS, keyword)[0].strip()

    # check for containment of aggregators
    if AGGREGATORS_ENABLED:
        for operator in AGGR_OPERATORS:
            synonyms = operator['synonyms_all']
            for syn in synonyms:
                if res.startswith(syn):
                    return res.replace(syn, '').strip()

    return res


def test_operator_containment(keyword):
    """
    returns whether a keyword token contains an operator
    (this is useful then processing a list of tokens,
    as only the last token may have an operator)

    >>> test_operator_containment('number of events >= 10')
    True

    >>> test_operator_containment('number')
    False

    """
    return bool(re.findall(KWS_OPERATORS, keyword))


def get_operator_and_param(keyword):
    """
    splits keyword on operator

    >>> get_operator_and_param('number of events >= 10')
    {'type': 'filter', 'param': '10', 'op': '>='}

    >>> get_operator_and_param('dataset')


    >>> get_operator_and_param('dataset=Zmm')
    {'type': 'filter', 'param': 'Zmm', 'op': '='}
    """

    parts = re.split(KWS_OPERATORS, keyword)
    # e.g. parts = ['number of events ', '>=', ' 10']
    if len(parts) == 3:
        return {'op': parts[1].strip(),
                'param': parts[2].strip(),
                'type': 'filter'}

    # aggregator
    if len(parts) == 1 and AGGREGATORS_ENABLED:
        for operator in AGGR_OPERATORS:
            synonyms = operator['synonyms_all']
            for syn in synonyms:
                if keyword.startswith(syn):
                    return {
                        'op': operator['name'],
                        'op_pattern': operator['op'],
                        'type': operator['type'],
                    }

    return None


def tokenize(query):
    """
    tokenizes the query retaining the phrases in brackets together
    it also tries to group "word operator word" sequences together, such as

    .. doctest::

        "number of events">10 or dataset=/Zmm/*/raw-reco

    so it could be used for further processing.

    special characters currently allowed in data values include: _*/-

    For example:

    .. doctest::

        >>> tokenize('file dataset=/Zmm*/*/raw-reco lumi=20853 nevents>10'\
                     '"number of events">10 /Zmm*/*/raw-reco')
        ['file', 'dataset=/Zmm*/*/raw-reco', 'lumi=20853', 'nevents>10', \
    'number of events>10', '/Zmm*/*/raw-reco']

        >>> tokenize('file dataset=/Zmm*/*/raw-reco lumi=20853 dataset.nevents>10'\
                     '"number of events">10 /Zmm*/*/raw-reco')
        ['file', 'dataset=/Zmm*/*/raw-reco', 'lumi=20853', 'dataset.nevents>10', \
    'number of events>10', '/Zmm*/*/raw-reco']

        >>> tokenize("file dataset=/Zmm*/*/raw-reco lumi=20853 dataset.nevents>10" \
                     "'number of events'>10 /Zmm*/*/raw-reco")
        ['file', 'dataset=/Zmm*/*/raw-reco', 'lumi=20853', 'dataset.nevents>10', \
    'number of events>10', '/Zmm*/*/raw-reco']


        >>> tokenize('user=vidmasze@cern.ch')
        ['user=vidmasze@cern.ch']

    """
    query = cleanup_query(query)
    # obtain bare tokens which include quotes
    token_matches = re.findall(TOKENIZER_PATTERNS,  query)
    # remove unneeded quotes
    tokens = [m.replace('"', '').replace("'", '')
              for m in token_matches]
    return tokens

if __name__ == '__main__':
    print tokenize('file dataset=/Zmm*/*/raw-reco lumi=20853 nevents>10 '
                   '"number of events">10 /Zmm*/*/raw-reco')
    print tokenize('file dataset=/Zmm*/*/raw-reco lumi=20853 dataset.nevents>10'
                   ' "number of events">10 /Zmm*/*/raw-reco')
    print tokenize("file dataset=/Zmm*/*/raw-reco lumi=20853 dataset.nevents>10"
                   " 'number of events'>10 /Zmm*/*/raw-reco")
    import doctest
    doctest.testmod()
