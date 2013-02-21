__author__ = 'vidma'

from DAS.keywordsearch.tokenizer import tokenize

DEBUG = True
def complete_last_token(query, selection_index, inst=None, dbsmngr=None, _DEBUG=False):
    '''
    TODO: shall we autocomplete the complete query or the last/selected token?

    it we take the current topic

    token types:
        keyword
        das_key
        post_filter

    '''
    tokens = tokenize(query)
    if DEBUG: print 'TOKENS:', tokens

    pass