#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
'''
Module description:
 - first cleans up input keyword query (remove extra spaces, standardize notation)
 - then it tokenizes the query into:
    * individual query terms
    * compound query terms in brackets (e.g. "number of events")
    * strings of "terms operator value" (e.g. nevent > 1, "number of events"=100)
'''


import re
from nltk.internals import  convert_regexp_to_nongrouping



kws_operators = r'(>=|<=|<|>|=)'
word = r'[a-zA-Z0-9_\-*/.@:]+'



cleanup_subs = [
    # get rid of multiple spaces
    (r'\s+', ' '),

    # TODO: this is temporary until supporting of specific patterns
    (r'^(find|show|display|retrieve|select)( me)?\s?', ''),
    # simplest way to process 'when' questions (attributes *_time; date is almost useless as result type currently (?), could could be input)
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
    (kws_operators + r'\s?([1-2][0-9]{3})-([0-1][0-9])-([0-3][0-9])', r'\1 \2\3\4')
]
compile_repl_pattern = lambda (regexp, repl): (re.compile(regexp), repl)
cleanup_subs = map(compile_repl_pattern, cleanup_subs)


#word =
tokenizer_patterns = \
    r'''
    "[^"]+" %(kws_operators)s %(word)s | # word in brackets plus operators
    "[^"]+"  |  # word in brackets

    '[^']+' %(kws_operators)s %(word)s | # word in brackets plus operators
    '[^']+'  |  # word in brackets

    %(word)s %(kws_operators)s %(word)s | # word op word
    %(word)s | # word
    \S+" # any other non-whitespace sequence
    ''' % locals()
tokenizer_patterns = convert_regexp_to_nongrouping(tokenizer_patterns)
tokenizer_patterns = re.compile(tokenizer_patterns, re.VERBOSE)



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

    >>> cleanup_query('find datasets where X more than 33 events')
    'datasets where X>33 events'

    >>> cleanup_query('=2012-02-01')
    '= 20120201'

    >>> cleanup_query('>= 2012-02-01')
    '>= 20120201'

    """

    for regexp, repl in cleanup_subs:
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
    #global kws_operators
    return re.split(kws_operators, keyword)[0].strip()

def test_operator_containment(keyword):
    '''
    returns whether a keyword token contains an operator
    (this is useful then processing a list of tokens,
    as only the last token may have an operator)

    >>> test_operator_containment('number of events >= 10')
    True

    >>> test_operator_containment('number')
    False

    '''
    return bool(re.findall(kws_operators, keyword))



def get_operator_and_param(keyword):
    '''
    splits keyword on operator

    >>> get_operator_and_param('number of events >= 10')
    {'param': '10', 'op': '>='}

    >>> get_operator_and_param('dataset')


    >>> get_operator_and_param('dataset=Zmm')
    {'param': 'Zmm', 'op': '='}

    '''

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

    #global kws_operators
    operators = kws_operators

    query = cleanup_query(query)
    # first remove extra spaces
    #operators = r'[=><]{1,2}'


    # TODO: remove brackets?

    return [m.replace('"', '').replace("'", '') for m in re.findall(tokenizer_patterns,  query)]

if __name__ == '__main__':
    print tokenize('file dataset=/Zmm*/*/raw-reco lumi=20853 nevents>10 "number of events">10 /Zmm*/*/raw-reco')
    print tokenize('file dataset=/Zmm*/*/raw-reco lumi=20853 dataset.nevents>10 "number of events">10 /Zmm*/*/raw-reco')
    print tokenize("file dataset=/Zmm*/*/raw-reco lumi=20853 dataset.nevents>10 'number of events'>10 /Zmm*/*/raw-reco")
    import doctest
    doctest.testmod()

