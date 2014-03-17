# coding=utf-8
"""
Matches keywords into fields in service outputs.
This currently works using whoosh, an information retrieval library.
"""
from collections import defaultdict
import pprint

from DAS.keywordsearch.config import CHUNK_N_PHRASE_RESULTS, \
    CHUNK_N_TOKEN_COMBINATION_RESULTS, W_PHRASE, DEBUG, MINIMAL_DEBUG, \
    USE_IR_SCORE_NORMALIZATION_LOCAL, MAX_TOKEN_COMBINATION_LEN
from DAS.keywordsearch.config import get_setting
from DAS.keywordsearch.tokenizer import get_keyword_without_operator, \
    get_operator_and_param, test_operator_containment
from DAS.keywordsearch.nlp import filter_stopwords
from DAS.keywordsearch.metadata.schema_adapter_factory import get_schema
from DAS.keywordsearch.entity_matchers.kwd_chunks.ir_entity_attributes import \
    SimpleIREntityAttributeMatcher


def check_validity(field_rec, fields_by_entity):
    """
    fields in whoosh index may be outdated in comparison with dasmaps...
    """
    entity = field_rec['result_type']
    field = field_rec['field']
    if not entity in fields_by_entity or not field in fields_by_entity[entity]:
        return False
    return True


def add_full_fieldmatch(kwd, matches):
    """ check for full match to am attribute, e.g. dataset.nevents """
    if '.' in kwd:
        match = get_schema().check_result_field_match(kwd)
        if match:
            entity, field = match
            result = {'field': field,
                      'len': 1,
                      'tokens_required': [kwd, ],
                      'score': 20.0}
            matches[entity].append(result)


class MultiKwdAttributeMatcher(object):
    """
    Matches chunks of keywords into fields in service outputs.
    """
    fields_idx = None

    def __init__(self, fields):
        """
        Upon each instantialization it recreates the Whoosh IR index based
        on the field data given.
        """
        self.fields_idx = SimpleIREntityAttributeMatcher(fields)

    def generate_chunks(self, keywords):
        """
        params: a tokenized list of keywords (e.g. ["a b c", 'a', 'b'])
        returns: a list of fields matching a combination of nearby keywords

        .. doctest::

            {
                '[result_type]':
                    [ matched_field, ...]
            }
        """

        if not get_setting('SERVICE_RESULT_FIELDS'):
            return {}

        matches = self.get_phrase_matches(keywords)
        self.append_subquery_matches(keywords, matches)
        # return the matches in sorted order (per result type)
        for entity, m_list in matches.iteritems():
            for match in m_list:
                last_token = match['tokens_required'][-1]
                tokens_used = match['tokens_required']
                match['predicate'] = get_operator_and_param(last_token)
                match['field_name'] = match['field']['name']
                match['tokens_required_non_stopw'] = \
                    filter_stopwords(tokens_used)
                match['tokens_required_set'] = set(tokens_used)

            m_list.sort(key=lambda f: f['score'], reverse=True)

            # as IR based matching is fairly dumb now,
            # prune out the useless matches
            purge = []
            for m1 in m_list:
                for m2 in m_list:
                    tokens1 = m1['tokens_required_set']
                    tokens2 = m2['tokens_required_set']
                    if (m2 != m1 and m1['field_name'] == m2['field_name'] and
                            tokens1.issubset(tokens2) and
                            m1['score'] + 0.01 >= m2['score']):
                        # mark a useless match for deletion
                        purge.append(m2)
            matches[entity] = [match for match in m_list if match not in purge]

        normalize_scores(matches)

        # if enabled, prune low scoring chunks
        if get_setting('RESULT_FIELD_CHUNKER_PRUNE_LOW_TERMS'):
            cutoff = get_setting('RESULT_FIELD_CHUNKER_PRUNE_LOW_TERMS')
            for key in matches:
                matches[key] = [match for match in matches[key]
                                if match['score'] > cutoff]

        print_debug(matches)
        return matches

    def get_phrase_matches(self, keywords):
        """
        get phrase matches from IR index
        """
        fields_by_entity = get_schema().list_result_fields()

        # first filter out the phrases (we wont combine them with anything)
        phrase_kwds = [kw for kw in keywords if ' ' in kw]

        matches = defaultdict(list)
        for kwd in phrase_kwds:
            # remove operators, e.g. "number of events">10 => number of events
            phrase = get_keyword_without_operator(kwd)
            # get ranked list of matches
            results = self.fields_idx.search_index(kwds=phrase,
                                                   limit=CHUNK_N_PHRASE_RESULTS)

            max_score = results and results[0]['score']
            for result in results:
                #r['len'] =  1
                result['len'] = len(result['keywords_matched'])
                entity = result['result_type']
                if not check_validity(result, fields_by_entity):
                    continue

                # TODO: this shall be done in presentation level
                result['field'] = fields_by_entity[entity][result['field']]
                result['tokens_required'] = [kwd]

                # penalize terms that have multiple matches
                result['score'] *= W_PHRASE
                if USE_IR_SCORE_NORMALIZATION_LOCAL:
                    result['score'] /= max_score

                matches[entity].append(result)

        return matches

    def append_subquery_matches(self, keywords, matches):
        """
        get matches to individual and nearby keywords (non phrase)
        """

        # check for full name matches to a attribute, e.g. dataset.nevents
        for kwd in keywords:
            add_full_fieldmatch(kwd, matches)

        fields_by_entity = get_schema().list_result_fields()
        str_len = len(keywords)
        max_len = min(len(keywords), MAX_TOKEN_COMBINATION_LEN)
        for length in xrange(1, max_len + 1):
            for start in xrange(0, str_len - length + 1):
                chunk = keywords[start:start + length]
                # exclude phrases with "a b c" (as these were processed earlier)
                if any(c for c in chunk if ' ' in c):
                    continue
                # only the last term in the chunk is allowed to contain operator
                if any(test_operator_containment(kw) for kw in chunk[:-1]):
                    continue
                if DEBUG:
                    print 'chunk:', chunk
                    print 'len=', length, '; start=', start, 'chunk:', chunk

                s_chunk = ' '.join(get_keyword_without_operator(kw)
                                   for kw in chunk)
                results = self.fields_idx.search_index(
                    kwds=s_chunk,
                    limit=CHUNK_N_TOKEN_COMBINATION_RESULTS)
                max_score = results and results[0]['score']
                for result in results:
                    result['len'] = len(result['keywords_matched'])
                    entity = result['result_type']
                    if not check_validity(result, fields_by_entity):
                        continue
                    result['field'] = fields_by_entity[entity][result['field']]
                    result['tokens_required'] = chunk
                    if USE_IR_SCORE_NORMALIZATION_LOCAL:
                        result['score'] /= max_score
                    matches[entity].append(result)


def print_debug(matches):
    """ prints out the debug if needed """
    if not MINIMAL_DEBUG:
        return
    print "chunks"
    for result_type, match in matches.items():
        print 'result_type:', result_type
        pprint.pprint([
            '{0:.2f} {1:s} for: {2:s}'.format(
                item['score'], item['field_name'], str(item['tokens_required']))
            for item in match])


def normalize_scores(matches):
    """
    Normalize the IR scores into [0..1].
    currently we assume IR scores around 20 to be OK,
    so we just divide them by max(20, max_score).
    """
    # actually the IR score tell something.. good match is around ~10-20
    get_max_score = lambda m_list: reduce(max, (m['score'] for m in m_list), 0)
    max_score = reduce(max, (get_max_score(m)
                             for m in matches.values()), 0)
    if DEBUG:
        print 'max_score', max_score

    fsmoothing = lambda x: (x / max(20.0, max_score))
    #if USE_IR_SCORE_SMOOTHING:
    #    fsmoothing = lambda x: math.log(1 + x) / math.log(
    #        1 + max(max_score, 20))
    if max_score:
        for ent_matches in matches.itervalues():
            for match in ent_matches:
                match['score'] = fsmoothing(match['score'])
