__author__ = 'vidma'

import math
import pprint

from DAS.keywordsearch.config import *
#from DAS.keywordsearch.metadata.das_schema_adapter import list_result_fields
from DAS.keywordsearch.config import mod_enabled
from DAS.keywordsearch.whoosh.ir_entity_attributes import load_index, search_index
from DAS.keywordsearch.tokenizer import get_keyword_without_operator,\
    get_operator_and_param, test_operator_containment

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

    matches = {}

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


            if not matches.has_key(entity):
                matches[entity] = []
            matches[entity].append(r)

    # now process partial matches and their combinations
    str_len = len(keywords)
    max_len = min(len(keywords), MAX_TOKEN_COMBINATION_LEN)
    for l in xrange(1, max_len+1):
        for start in xrange(0, str_len-l+1):
            chunk = keywords[start:start+l]

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

                if not matches.has_key(entity):
                    matches[entity] = []
                matches[entity].append(r)
        # Use longest useful matching  as a heuristic to filter out crap, e.g.
    # file Zmm number of events > 10, shall match everything,


    # TODO: use SCORE!!!
    # return the matches in sorted order (per result type)
    for entity in matches.keys():
        #print 'trying to sort:'
        #pprint.pprint(full_matches[entity])
        for m in matches[entity]:
            m['predicate'] = get_operator_and_param(m['tokens_required'][-1])


        matches[entity].sort(key=lambda f: f['score'], reverse=True)


    # normalize the scores (if any)
    # TODO: actually the IR score tell something.. if it's around ~10 it's a good match

    _get_max_score = lambda m_list: reduce(max, map(lambda m: m['score'], m_list), 0)
    scores = map(_get_max_score, matches.values())
    max_score = reduce(max, scores, 0)

    if DEBUG: print 'max_score', max_score


    fsmoothing = lambda x: (x / 20.0 < 1) and x / 20.0 or 1.0
    fsmoothing = lambda x: (x / max(20.0, max_score))

    if USE_IR_SCORE_SMOOTHING:
        fsmoothing = lambda x: math.sqrt(1.0+x) / math.sqrt(1.0+max_score)
        fsmoothing = lambda x: math.log(1.0+x) / math.log(1.0+max(max_score, 20))



    if max_score:
        for ent_matches in matches.values():
            for m in ent_matches:
                # print "m['score']", m['score'], 'mlen:', m['len']
                # pprint.pprint(m)
                m['field_name'] = m['field']['field']
                # TODO: actually this kind of normalization is not necessarily fair because
                # some combinations could be scored very high, e.g. phrases...
                # as a temporary "HACK" we use a smoothing function which would make these differences milder
                m['score'] = fsmoothing(m['score'])



    # if enabled, prune low scoring chunks
    if mod_enabled('RESULT_FIELD_CHUNKER_PRUNE_LOW_TERMS'):
        cutoff = mod_enabled('RESULT_FIELD_CHUNKER_PRUNE_LOW_TERMS')
        for key in matches.keys():
            matches[key] = filter(lambda m: m['score'] > cutoff, matches[key])


    if _DEBUG:
        print 'chunks generated:'
        pprint.pprint(matches)
    else:
        for result_type, m in matches.items():
            print 'result_type:', result_type
            pprint.pprint(map(lambda item: '%.2f %s for: %s' \
                                           % (item['score'], item['field_name'], ','.join(item['tokens_required'])), m))
    return matches

