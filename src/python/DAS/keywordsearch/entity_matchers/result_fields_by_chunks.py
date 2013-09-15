from collections import defaultdict

__author__ = 'vidma'

import math
import pprint

from DAS.keywordsearch.config import *
#from DAS.keywordsearch.metadata.das_schema_adapter import list_result_fields
from DAS.keywordsearch.config import mod_enabled
from DAS.keywordsearch.whoosh.ir_entity_attributes import load_index, search_index
from DAS.keywordsearch.tokenizer import get_keyword_without_operator,\
    get_operator_and_param, test_operator_containment
from DAS.keywordsearch.nlp import filter_stopwords

from DAS.keywordsearch.metadata.schema_adapter_factory import getSchema


def generate_chunks_no_ent_filter(keywords):
    """
    params: keywords - a tokenized list of keywords (e.g. ["a b c", 'a', 'b'])
    returns: a list of fields matching a combination of nearby keywords
    {
        '[result_type]':
            [ matched_field, ...]
    }
    """

    if not mod_enabled('SERVICE_RESULT_FIELDS'):
        return {}

    _DEBUG = False

    W_PHRASE = 1.5

    # TODO: These could be increased for short queries or lowered for long ones
    RESULT_LIMIT_PHRASES = 20
    RESULT_LIMIT_TOKEN_COMBINATION = 10
    # max len of tokens to consider as a sequence
    # (e.g. number of events --> "number of events")
    MAX_TOKEN_COMBINATION_LEN =  4


    fields_by_entity = getSchema().list_result_fields()


    load_index()

    # first filter out the phrases (we wont combine them with anything)
    phrase_kwds = filter(lambda kw: ' ' in kw, keywords)


    # we may also need to remove operators, e.g. "number of events">10, 'block.nevents>10'

    matches = defaultdict(list)

    for kwd in phrase_kwds:
        phrase = get_keyword_without_operator(kwd)

        res = search_index(
            keywords=phrase,
            limit=RESULT_LIMIT_PHRASES)

        max_score = res and res[0]['score']
        for r in res:
            #r['len'] =  1
            r['len'] = len(r['keywords_matched'])


            entity = r['result_type']
            r['field'] = fields_by_entity[entity][r['field']]
            r['tokens_required'] = [kwd]
            # TODO: check if a full match and award these, howerver some may be misleading,e.g. block.replica.site is called just 'site'!!!
            # therefore, if nothing is pointing to block.replica we shall not choose block.replica.site
            # TODO: shall we divide by variance or stddev?

            # penalize terms that have multiple matches
            r['score'] *= W_PHRASE
            if USE_IR_SCORE_NORMALIZATION_LOCAL:
                r['score'] /= max_score

            matches[entity].append(r)

    # now process partial matches and their combinations
    str_len = len(keywords)
    max_len = min(len(keywords), MAX_TOKEN_COMBINATION_LEN)
    for l in xrange(1, max_len+1):
        for start in xrange(0, str_len-l+1):
            chunk = keywords[start:start+l]

            # check for full name match to a attribute
            if len(chunk) == 1 and '.' in chunk[0]:
                match = getSchema().check_result_field_match(chunk[0])
                if match:
                    entity, field = match
                    r = {'field': field}
                    r['len'] = 1
                    #r['field']
                    r['tokens_required'] = chunk
                    r['score'] = 20.0
                    matches[entity].append(r)




            if DEBUG: print 'chunk:', chunk

            # exclude chunks with "a b c" (as these were processed earlier)
            if filter(lambda c:' ' in c, chunk):
                continue

            # only the last term in the chunk is allowed to contain operator
            if filter(test_operator_containment, chunk[:-1]):
                continue


            if DEBUG: print 'len=', l, '; start=', start, 'chunk:', chunk

            chunk_kwds = map(get_keyword_without_operator, chunk)

            s_chunk = ' '.join(chunk_kwds)
            res = search_index(
                keywords= s_chunk ,
                limit=RESULT_LIMIT_TOKEN_COMBINATION)
            max_score = res and res[0]['score']
            for r in res:
                # TODO: use only matched keywords here
                #r['len'] = len(filter_stopwords(chunk))
                r['len'] = len(r['keywords_matched'])
                entity = r['result_type']
                r['field'] = fields_by_entity[entity][r['field']]
                r['tokens_required'] = chunk

                if USE_IR_SCORE_NORMALIZATION_LOCAL:
                    r['score'] /= max_score


                # TODO: check if a full match and award these, howerver some may be misleading,e.g. block.replica.site is called just 'site'!!!
                # therefore, if nothing is pointing to block.replica we shall not choose block.replica.site
                # TODO: shall we divide by variance or stddev?


                matches[entity].append(r)
    # Use longest useful matching  as a heuristic to filter out crap, e.g.
    # file Zmm number of events > 10, shall match everything,

    _format_match_str = lambda item: '%.2f %s for: %s' \
                                           % (item['score'], item['field_name'], ','.join(item['tokens_required']))

    # TODO: use SCORE!!!
    # return the matches in sorted order (per result type)
    for entity, m_list in matches.iteritems():
        #print 'trying to sort:'
        #pprint.pprint(full_matches[entity])
        for m in m_list:
            m['predicate'] = get_operator_and_param(m['tokens_required'][-1])
            m['field_name'] = m['field']['field']
            m['tokens_required_non_stopw'] = filter_stopwords(m['tokens_required'])
            m['tokens_required_set'] = set(m['tokens_required'])


        m_list.sort(key=lambda f: f['score'], reverse=True)

        # because IR based matching is fairly dumb now, prune out the useless matches
        purge = []
        for m in m_list:
            for m1 in m_list:
                # TODO: check predicate?
                if m1 != m and \
                    m['field_name'] == m1['field_name'] and \
                    m['tokens_required_set'].issubset(m1['tokens_required_set']) and \
                    m['score']+0.01 >= m1['score']:
                        purge.append(m1)
                        #print 'will delete useless match:', _format_match_str(m1)

        matches[entity] = filter(lambda m: not m in purge, m_list)

    # normalize the scores (if any)
    # actually the IR score tell something.. if it's around ~10-20 it's a good match
    _get_max_score = lambda m_list: reduce(max, map(lambda m: m['score'], m_list), 0)
    scores = map(_get_max_score, matches.values())
    max_score = reduce(max, scores, 0)

    if DEBUG: print 'max_score', max_score

    fsmoothing = lambda x: (x / max(20.0, max_score))

    if USE_IR_SCORE_SMOOTHING:
        fsmoothing = lambda x: math.log(1.0+x) / math.log(1.0+max(max_score, 20))

    if max_score:
        for ent_matches in matches.itervalues():
            for m in ent_matches:
                m['score'] = fsmoothing(m['score'])

    # if enabled, prune low scoring chunks
    if mod_enabled('RESULT_FIELD_CHUNKER_PRUNE_LOW_TERMS'):
        cutoff = mod_enabled('RESULT_FIELD_CHUNKER_PRUNE_LOW_TERMS')
        for key in matches:
            matches[key] = filter(lambda m: m['score'] > cutoff, matches[key])

    if MINIMAL_DEBUG:
        print "chunks"
        for result_type, m in matches.items():
            print 'result_type:', result_type
            pprint.pprint(map(_format_match_str, m))
    return matches

