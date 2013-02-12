#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

#TODO: implement simplified version of
# /home/vidma/Desktop/DAS/DAS_code/DAS/prototypes/!!! Keyword Search over Relational Databases_Metadata_BergamaschiDGLV11.pdf
"""
Standard vocabulary:
field -- api input parameter
value

Entry points for keyword search:
- system ? [eg dbs/phedex only?]

- entity name
- field name (das short, das long?, of the service?)
- field value matching a it's regexp constraint at some API:
    especially useful for sufficiently restrictive constraintse.g:
    * dataset name
        (worse if that's a wildcard pattern --> but don't we allow wildcard only here?)
    * filename
    * email
    * release: CMSSW_*

    * date?


    and fields with (almost) static low-arity values:
    * site
    * tier
    * status
    * datatype


    we may by chance have some of the values requested in our long-term historical cache, so to
    figure out matching fields


Other important part are the selections and maybe even filters:
    (users may be interested only in specific sub-field, currently the summaries e.g. for dataset costs a lot)
    - so it may match a result item's field name
    - result items field value? (worse, because we don't have any data about this)

Post processing:
a number of best and cheap matches could be port-processed by running the queries live...


General ideas:
 - each of these are assigned a weight



Any synonyms we have?


--------
A keyword may be matched into:
- result type (entity)
- result fields

- input parameter name
- input parameter value


----

TODO: Springer 2012
The fundamental difference is that they do not assume any a-priori access to the database instance. Un-
avoidably, the approaches are based on schema and meta-data, i.e., the intensional
information, which makes them applicable to scenarios where the other techniques
cannot work.


--------------------
  Statisttics
----------------------
* services
    - execution time: min max avg [some deviation that could be calculated cheaply]
    - number of results returned

* data
    - (!) arity of each entity (i.e. number of possible values)
        --> calculate historically or request from services (doable for DBS for now)
    -



--------------------------
    Historical data
--------------------------
    historical query results?
    --> if arity of some parameters is high, the possible caching combinations are also VERY high
"""
# TODO: check how databases are managing views and their optimizations (statistcis etc)


# TODO: check on interconnectiins between services (data returned, compatibility etc)


# TODO: approximate keyword search? feasible on DBs, worse here

# TODO: approach to history

# TODO: a high performance key-value store for cache and historical info

# TODO: incorporate all the possible values


import re
import pprint
import math

from nltk.corpus import stopwords

from DAS.keywordsearch.das_schema_adapter import *
from DAS.keywordsearch.entity_matching import *

import DAS.keywordsearch.das_schema_adapter as integration_schema

from DAS.keywordsearch.tokenizer import tokenize, get_keyword_without_operator, get_operator_and_param, cleanup_query
from DAS.keywordsearch.config import mod_enabled

from DAS.keywordsearch.whoosh.service_fields import load_index, search_index

from DAS.keywordsearch.value_matching import keyword_value_weights

DEBUG = False


en_stopwords = stopwords.words('english')


def _get_reserved_terms():
    """
    terms that shall be down-ranked
    """
    entities = ['dataset', 'run', 'block', 'file', 'site']
    operators = integration_schema.get_operator_synonyms()
    return set(entities) | set(operators)

# TODO: this has to be implemented in a better way
def penalize_highly_possible_schema_terms_as_values(keyword, schema_ws):
    """
    it is important to avoid misclassifying dataset, run as values.
    these shall be allowed only if explicitly requested.
    """

    if not mod_enabled('DOWNRANK_TERMS_REFERRING_TO_SCHEMA'):
        return 0.0

    # TODO: this is just a quick workaround
    if keyword in _get_reserved_terms(): #['dataset', 'run', 'block', 'file', 'site']:
        # TODO: each reserved term shall have a different weight, e.g. operators lower than entity?
        return -5.0

    _DEBUG = 0
    if schema_ws:
        # e.g. configuration of dataset Zmmg-13Jul2012-v1 site=T1_* location is at T1_*
        # 4.37: dataset dataset=*Zmmg-13Jul2012-v1* site=T1_*
        # 4.37: dataset dataset=*dataset* site=T1_*
        # I should look at distribution and compare with other keywords
        f_avg_ = lambda items: len(items) and sum(items) / len(items) or None
        f_avg = lambda items: f_avg_(filter(None, items))
        f_avg_score = lambda interpretations: f_avg(
            map(lambda item: item[0], interpretations))
        # global average schema score
        avg_score = f_avg([f_avg_score(keyword_scores)
                           for keyword_scores in schema_ws.values()]) or 0.0
        # average score for this keyword
        keyword_schema_score = f_avg_score(schema_ws[keyword]) or 0.0
        if _DEBUG:
            print "avg schema score for '%s' is %.2f; avg schema = %.2f " % (
                keyword, keyword_schema_score, avg_score)
        if avg_score < keyword_schema_score:
            return 3*min(-0.5, -(keyword_schema_score- avg_score))

    return 0.0


def generate_result_filters(keywords_list, chunks, keywords_used,
                            old_score, requested_entity, values_mapping,
                            result_filters=[], field_idx_start=0,
                            traceability=set(), result_fields_included = set()):
    global final_mappings
    # try assigning result values for particular entity
    # TODO: we use greedy strategy trying to assign largest keyword sequence (field chunks are sorted)
    requested_entity_short = requested_entity.split('.')[0]
    for field_idx, match in enumerate(chunks.get(requested_entity_short, [])):
        if field_idx_start > field_idx:
            continue

        target_fieldname = match['field']['field']

        # we are anyway including this into filter
        if target_fieldname == requested_entity:
            continue

        # if all required keywords are still available
        if set(match['keywords_required']).isdisjoint(keywords_used) and \
                (target_fieldname not in result_fields_included):
            print 'required fields available for:', match
            keywords_used_ = keywords_used | set(match['keywords_required'])

            weight = match['field']['importance'] and 0.2 or 0.1


            _result_filters = result_filters[:]
            target = target_fieldname


            if match['predicate']:
                pred = match['predicate']
                target = (target_fieldname, pred['op'], pred['param'])

                # promote matches with operator
                match['score'] *= 3.0

            delta_score = old_score + weight * match['score']
            if len(match['keywords_required']) == 1:
                delta_score += penalize_highly_possible_schema_terms_as_values(
                                    match['keywords_required'][0], None)



            _result_filters.append(target)


            new_score = old_score + delta_score
            
            adjusted_score_ = penalize_non_mapped_keywords(
                                keywords_used_, keywords_list, new_score)
                

            # TODO: do we support set literal (set([]) == {}); requires: 2.7a3+
            # http://stackoverflow.com/questions/2243049/what-do-you-think-about-pythons-new-set-literal-2-7a3

            _traceability = traceability | set([
                                                    (tuple(match['keywords_required']),
                                                    'result_projection',
                                                    target,
                                                    delta_score), ])

            final_mappings.append((
                adjusted_score_, requested_entity,
                tuple(values_mapping.items()),
                tuple(_result_filters),
                tuple(_traceability)
            ))



            if len(keywords_used_) < len(set(keywords_list)):
                generate_result_filters(keywords_list, chunks, keywords_used_,
                    old_score + delta_score, requested_entity, values_mapping,
                    result_filters =_result_filters, field_idx_start=field_idx + 1,
                    traceability=_traceability,
                    result_fields_included = result_fields_included | set([target_fieldname ]))


def penalize_non_mapped_keywords(keywords_used, keywords_list, score):
    '''
    penalizes keywords that have not been mapped.
    '''
    #TODO: take into account different POS.
    # e.g. stopwords shall not be penalized, but in some cases could give increase,
    # e.g. TODO: where is smf, 'how big/large' is

    #N_keywords = len(keywords_list)

    global en_stopwords

    N_total_kw = len(set(keywords_used))
    N_keywords_without_stops = sum([1  for kw in set(keywords_list)
       if  not kw in en_stopwords ])

    return score * min(len(keywords_used), N_total_kw) / N_keywords_without_stops


def generate_value_mappings(requested_entity, fields_included, schema_ws,
                            values_ws,
                            old_score, values_mapping={}, # TODO
                            keywords_used=set(),
                            keywords_list= [], keyword_index=0, chunks=[],
                            traceability= set(),
                            result_projection_forbidden=set()):
    SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT = 0.2

    # TODO: modify the value and schema mappings weights according to previous mappings
    global final_mappings
    UGLY_DEBUG = False

    fields_included = set(fields_included)
    fields_covered_by_values_set = set(values_mapping.keys())

    # (as a final condition) now every field in fields_included that were guessed in earlier step, has to be covered by values
    # newones could still be added


    if UGLY_DEBUG:
        print 'generate_value_mappings(', requested_entity, fields_included, schema_ws, values_ws, old_score, values_mapping, keywords_used, keywords_list, keyword_index, ')'

    if keyword_index == len(keywords_list):
        #print keyword_index, 'final'
        # DAS requires at least one filtering attribute
        if fields_covered_by_values_set and fields_covered_by_values_set.issuperset(
            fields_included) and\
           validate_input_params(fields_included, final_step=True,
               entity=requested_entity):
            if UGLY_DEBUG: print 'VALUES MATCH:', (
                requested_entity, fields_included, values_mapping ),\
            validate_input_params(fields_included, final_step=True,
                entity=requested_entity)

            # Adjust the final score to favour mappings that cover most keywords
            # TODO: this could be probably better done by introducing P(Unknown)


            if not requested_entity:
                # if not entity was guessed, infer it from service parameters
                entities = entities_for_input_params(fields_included)
                if UGLY_DEBUG: print 'Result entities matching:', entities

                for requested_entity in entities.keys():
                    adjusted_score = old_score

                    if requested_entity in values_mapping.keys():
                        adjusted_score += SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT

                    # TODO: how to handle non mapped items: probablity, divide, minus??
                    # TODO: shall we use probability-like scoring?
                    adjusted_score = penalize_non_mapped_keywords(keywords_used,
                                                                  keywords_list,
                                                                  adjusted_score)

                    final_mappings.append(
                                    (adjusted_score, requested_entity,
                                    tuple(values_mapping.items()),
                                    tuple([]),
                                    tuple(traceability))
                    )

                    generate_result_filters(keywords_list, chunks,
                        keywords_used | result_projection_forbidden,
                        old_score, requested_entity,
                        values_mapping, traceability = traceability,
                        )


            else:
                adjusted_score = penalize_non_mapped_keywords(keywords_used,
                                    keywords_list, old_score)

                final_mappings.append(
                    (adjusted_score, requested_entity,
                    tuple(values_mapping.items()),
                    tuple([]),
                    tuple(traceability))
                )


                generate_result_filters(keywords_list, chunks,
                    keywords_used | result_projection_forbidden,
                    old_score, requested_entity,
                    values_mapping, traceability=traceability,
                    result_fields_included = result_projection_forbidden)

        return

    #print 'continuing at index:', keyword_index

    # we either take keyword[i] or not
    keyword = keywords_list[keyword_index]
    keyword_weights = values_ws.get(keyword, [])

    # case 1) we do not take keyword[i]:
    generate_value_mappings(requested_entity, fields_included, schema_ws,
        values_ws, old_score, values_mapping,
        keywords_used, keywords_list, keyword_index=keyword_index + 1,
        chunks=chunks, traceability=traceability,
        result_projection_forbidden=result_projection_forbidden)

    # case 2) we do take keyword[i]:
    if keyword not in keywords_used:
        for field_score, possible_mapping in keyword_weights:
            keyword_adjusted = keyword
            if isinstance(possible_mapping, dict):
                #print possible_mapping
                if possible_mapping.has_key('adjusted_keyword'):
                    keyword_adjusted = possible_mapping['adjusted_keyword']
                elif '=' in keyword:
                    keyword_adjusted = keyword.split('=')[-1]

                possible_mapping = possible_mapping['map_to']
            elif '=' in keyword:
                keyword_adjusted = keyword.split('=')[-1]

                # currently we do not allow mapping two keywords to the same value
            # TODO: It could be in theory useful combining a number of consecutive keywords refering to the same value
            if possible_mapping in fields_covered_by_values_set:
                continue

            vm_new = values_mapping.copy()
            vm_new[possible_mapping] = keyword_adjusted

            delta_score = field_score

            # we favour the values mappings that have also been refered in schema mapping (e.g. dataset *Zmm*)
            # TODO: It could be in theory useful combining a number of consecutive keywords refering to the same value
            if possible_mapping in fields_included:
                delta_score += SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT

                # we favour mappings which respect X=Y conditions in the keywords
                if '=' in keyword:
                    smapping = [(field_score, smapping) for (field_score, smapping) in
                                schema_ws[keyword]
                                if smapping == possible_mapping]
                    if smapping:
                        # increase by the score of mapping into schema (to only favour likely schema mappings)
                        delta_score += smapping[0][
                                     0] + SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT



            # TODO: penalize keywords that are mapping well to the schema entities
            if True and not '=' in keyword:
                delta_score += penalize_highly_possible_schema_terms_as_values(keyword,
                                schema_ws)


            new_score = delta_score +  old_score
            new_fields = fields_included | set([possible_mapping])

            if validate_input_params(new_fields, final_step=False,
                entity=requested_entity):
                generate_value_mappings(requested_entity,
                    fields_included=new_fields,\
                    values_mapping=vm_new,\
                    old_score=new_score,
                    keywords_used=keywords_used | set([keyword]),\
                    schema_ws=schema_ws, values_ws=values_ws,
                    keyword_index=keyword_index + 1,
                    keywords_list=keywords_list,
                    chunks=chunks,
                    traceability= traceability| set([(keyword,
                                                      'value_for',
                                                      possible_mapping,
                                                     delta_score)]),
                    result_projection_forbidden = result_projection_forbidden)


                # (as a final condition) now every field in fields_included that were guessed in earlier step, has to be covered by values
                # newones could still be added


# TODO: the recursion is dumb, we could at least use some pruning
def generate_schema_mappings(requested_entity, fields_old, schema_ws, values_ws,
                             old_score, keywords_list=[], keyword_index=0,
                             keywords_used=set([]),
                             chunks=[],
                             traceability= set([])):
    # TODO: keyword order is important
    UGLY_DEBUG = False

    # generate_values_mappings()
    # TODO: shall we modify the value and schema mappings weights according to previous mappings HERE or only when doing VALUE matching?

    # TODO: it would be better to consider all items in decreasing scores
    global final_mappings

    if keyword_index == len(keywords_list):
        # TODO: check if required fields are functioning properly !!!
        if validate_input_params(fields_old, final_step=True,
            entity=requested_entity):
            if UGLY_DEBUG: print 'SCHEMA MATCH:', (
                requested_entity, fields_old), validate_input_params(
                fields_old, final_step=True, entity=requested_entity)


        # if we used a compound keyword A=B for schema, its still available for values
        keywords_used_wo_operators = set([k for k in keywords_used
                              if not '=' in k])
        result_projection_forbidden = set([k for k in keywords_used
                                           if '=' in k])
        # try to map values based on this
        generate_value_mappings(requested_entity, fields_old, schema_ws,
            values_ws, old_score,
            keywords_used=keywords_used_wo_operators, keywords_list=keywords_list,
            chunks=chunks,  traceability=traceability,
            result_projection_forbidden = result_projection_forbidden)


        if UGLY_DEBUG: print (
            requested_entity, fields_old, schema_ws, values_ws)
        return

    # At keyword position (i) we can either:
    #    1) leave it out (a value, or non relevant/not mappable)
    #    2) take keyword i and map it to:
    #        a) requested entity (result type)
    #        b) schema entity (api input param)
    #        c) both
    #


    keyword = keywords_list[keyword_index]
    schema_w = schema_ws[keyword]

    # opt 1) do not take keyword[i]
    generate_schema_mappings(requested_entity, fields_old, schema_ws, values_ws,
        keywords_list=keywords_list, keyword_index=keyword_index + 1,
        old_score=old_score,\
        keywords_used=keywords_used, chunks=chunks, traceability=traceability)

    # opt 2) take it:
    for schema_score, possible_mapping in schema_w:
        if possible_mapping in fields_old:
            continue

        fields_new = fields_old[:]
        fields_new.append(possible_mapping)
        #print 'validating', (f, requested_entity)

        # opt 2.a) take as api input param entity
        if validate_input_params(fields_new, entity=requested_entity):
            if UGLY_DEBUG: print 'validated', (requested_entity, fields_new)

            delta_score = schema_score
            generate_schema_mappings(requested_entity, fields_new, schema_ws,
                values_ws,
                keywords_list=keywords_list, keyword_index=keyword_index + 1,
                old_score=old_score + delta_score, keywords_used=keywords_used | set([keyword]),
                # TODO: add current core
                chunks=chunks,  traceability=traceability | set([(keyword,
                                                                  'schema',
                                                                  possible_mapping,
                                                                  delta_score)]))


        # opt 2.b) take as requested entity (result type)
        if not requested_entity:
            delta_score = schema_score

            # TODO: use focus extraction instead!!!
            # if this is the first keyword mapped to schema (we expect entity name to come first)
            if not keywords_used and (keyword_index+1) * 1.9 < len(keywords_list):
                delta_score *= 0.5 * (
                    float(len(keywords_list)) - keyword_index) / len(
                    keywords_list)

            if validate_input_params(fields_old,
                entity=possible_mapping):
                if UGLY_DEBUG:  print 'validated', (
                    possible_mapping, fields_old)

                # TODO: currently the score is anyway being increased if a value is being mapped...
                generate_schema_mappings(possible_mapping, fields_old,
                    schema_ws, values_ws,
                    keywords_list=keywords_list,
                    keyword_index=keyword_index + 1,
                    old_score=old_score + delta_score,\
                    keywords_used=keywords_used | set([keyword]),
                    chunks=chunks,  traceability=traceability | set([(keyword,
                                                                      'requested_entity',
                                                                      possible_mapping,
                                                                     delta_score)]))

            # opt 2.c) take both as requested entity (result type) and  input param entity
            if validate_input_params(fields_new,
                entity=possible_mapping):

                if UGLY_DEBUG: print 'valid',(possible_mapping, fields_new)

                generate_schema_mappings(possible_mapping, fields_new,
                    schema_ws, values_ws,
                    keywords_list=keywords_list,
                    keyword_index=keyword_index + 1,
                    old_score=old_score + delta_score,\
                    keywords_used=keywords_used | set([keyword]),
                    chunks=chunks, traceability= traceability | set([(keyword, 'requested_entity', possible_mapping)]) |
                                                 set([(keyword,
                                                       'schema',
                                                       possible_mapping)])
                )


# TODO: we may need some extra stop condition...

def generate_chunks(keywords, keywords_str):
    """
    params: keywords - a tokenized list of keywords (e.g. ["a b c", 'a', 'b'])
    """

    fields_by_entity = list_result_fields()
    entities = fields_by_entity.keys()


    load_index()

    # first filter out the phrases (we wont combine them with anything)
    phrase_kwds = [kw for kw in keywords if ' ' in kw]


    # we may also need to remove operators, e.g. "number of events">10, 'block.nevents>10'

    matches = {}
    for entity in entities:
        matches[entity] = []
        for kwd in phrase_kwds:
            phrase = get_keyword_without_operator(kwd)

            res = search_index(
                keywords=phrase,
                result_type=entity)
            for r in res:
                r['len'] = len(phrase.split(' '))
                r['field'] = fields_by_entity[entity][r['field']]
                r['keywords_required'] = [kwd]
                # TODO: check if a full match and award these, howerver some may be misleading,e.g. block.replica.site is called just 'site'!!!
                # therefore, if nothing is pointing to block.replica we shall not choose block.replica.site
                # TODO: shall we divide by variance or stddev?

                # penalize terms that have multiple matches
                r['score'] /= len(res)

            matches[entity].extend(res)

    # TODO: now process partial matches
    max_len = len(keywords)
    for l in xrange(1, max_len+1):
        for start in xrange(0, max_len-l+1):
            chunk = keywords[start:start+l]

            # exclude chunks with "a b c"
            if [c for c in chunk if ' ' in c]:
                continue

            print 'len=', l, '; start=', start, 'chunk:', chunk
            for entity in entities:
                chunk_kwds = map(get_keyword_without_operator, chunk)

                s_chunk = ' '.join(chunk_kwds)
                res = search_index(
                    keywords= s_chunk ,
                    result_type=entity)
                for r in res:
                    r['len'] = len(chunk),
                    r['field'] = fields_by_entity[entity][r['field']]
                    r['keywords_required'] = chunk

                    # r['score'] *= 0.5 * l # penalize partial matches and promote longer matches

                    # penalize terms that have multiple matches
                    r['score'] *= 0.5 * len(chunk) / len(res)


                    # TODO: check if a full match and award these, howerver some may be misleading,e.g. block.replica.site is called just 'site'!!!
                    # therefore, if nothing is pointing to block.replica we shall not choose block.replica.site
                    # TODO: shall we divide by variance or stddev?

                matches[entity].extend(res)
    # Use longest useful matching  as a heuristic to filter out crap, e.g.
    # file Zmm number of events > 10, shall match everything,


    # TODO: use SCORE!!!
    # return the matches in sorted order (per result type)
    for entity, fields in fields_by_entity.items():
        #print 'trying to sort:'
        #pprint.pprint(full_matches[entity])
        for m in matches[entity]:
            pred = get_operator_and_param(m['keywords_required'][-1])
            m['predicate'] = None
            if pred:
                m['predicate'] = pred

        matches[entity].sort(key=lambda f: f['score'], reverse=True)


    print 'chunks generated:'
    pprint.pprint(matches)
    return matches



def DASQL_2_NL(dasql_tuple, html=True):
    #TODO: DASQL_2_NL
    """
    TODO: return natural language representation of a generated DAS query
     so to explain users what does it mean.
    """
    (result_type, short_input_params, result_projections, result_filters,
     result_operators) = dasql_tuple
    # TODO: support operators (sum, count) and post_filters
    # TODO: distinguish between grep filters and grep selections!!!

    result = result_type
    filters = ['%s=%s' % (f, v) for (f, v) in short_input_params]

    # sum(number of events IN dataset) where dataset=*Zmm*

    if result_filters:
        # TODO: add verbose name if any
        filters.extend(['%s %s %s' %
                        (integration_schema.get_result_field_title(result_type, field, technical=True, html=True),
                         op, val) for (field, op, val) in result_filters])

    filters = ' <b>AND</b> '.join(filters)

    if result_projections:
        # TODO: what if entity is different than result_type? We shall probably output that as well...
        result_projections = [
            '%s' % integration_schema.get_result_field_title(result_type, field, technical=True, html=True)
            for field in result_projections
        ]
        result_projections = ', '.join(result_projections)
        # TODO: use FOR EACH if selector is not the same, or includes a wildcard!
        return '<b>find</b> %(result_projections)s <b>for each</b> %(result_type)s <b>where</b> %(filters)s' % locals()

    return '<b>find</b> %(result)s <b>where</b> %(filters)s' % locals()



def search(query, inst=None, _DEBUG=False):
    """
    unit tests
    >>> search('vidmasze@cern.ch')
    user user=vidmasze@cern.ch
    """

    # TODO: add DBS instance as parameter

    DEBUG = True

    schema_ws = {}
    values_ws = {}



    # query = cleanup_query(query)
    if DEBUG: print 'Query:', query
    query = cleanup_query(query)
    if DEBUG: print 'CLEAN Query:', query
    tokens = tokenize(query)
    if DEBUG: print 'TOKENS:', tokens


    if DEBUG: print 'Query after cleanup:', query

    # TODO: some of EN 'stopwords' may be quite important e.g.  'at', 'between', 'where'
    en_stopwords = stopwords.words('english')
    keywords = [kw.strip() for kw in tokens
                if kw.strip()]

    for keyword_index, keyword in enumerate(keywords):
        # TODO: A=B, is a very good clue of what have to be mapped to what...
        kw_value = kw_schema = keyword

        if '=' in keyword:
            if len(keyword.split('=')) == 2:
                kw_schema, kw_value = keyword.split('=')

        is_stopword = keyword in en_stopwords

        schema_ws[keyword] = keyword_schema_weights(
            kw_schema, include_fields=True, keyword_index=keyword_index,
            is_stopword=is_stopword)

        if not is_stopword and kw_value:
            values_ws[keyword] = keyword_value_weights(kw_value)

    print '============= Q: %s ==========' % query
    if DEBUG:
        print '============= Schema mappings (TODO) =========='
        pprint.pprint(schema_ws)
        print '=============== Values mappings (TODO) ============'
        pprint.pprint(values_ws)

    global final_mappings
    final_mappings = []

    chunks = generate_chunks(keywords, ' '.join(keywords))

    generate_schema_mappings(None, [], schema_ws, values_ws,
        keywords_list=keywords, keyword_index=0, old_score=0, chunks=chunks)

    #pprint.pprint(final_mappings)

    print "============= Results for: %s ===" % query
    final_mappings = list(set(final_mappings))
    final_mappings.sort(key=lambda item: item[0], reverse=True)

    best_scores = {}

    first = 1

    for (score, result_type, input_params, result_projections_filters, traceability) in final_mappings:
        # short entity names
        s_result_type = entity_names[result_type]
        s_input_params = [(entity_names.get(field, field), value) for
                          (field, value) in input_params]
        s_input_params.sort(key=lambda item: item[0])

        s_query = s_result_type + ' ' + ' '.join(
            ['%s=%s' % (field, value) for (field, value) in s_input_params])

        result_projections = [p for p in result_projections_filters
                             if not isinstance(p, tuple)]
        result_filters = [p for p in result_projections_filters
                             if isinstance(p, tuple)]


        if result_projections or result_filters:

            print 'selections before:', result_projections
            result_projections = list(result_projections)

            # automatically add wildcard fields to selections (if any), so they would be displayed in the results
            if [1 for (field, value) in input_params if '*' in value]:
                result_projections.append(
                            result_type) # result type of primary key of requested entity
            # TODO: check if other wildcard fields are also there

            result_grep = result_projections[:]
            # add filters to grep
            s_result_filters = ['%s%s%s' % f for f in result_filters]
            result_grep.extend(s_result_filters)
            # TODO: NL description

            s_query += ' | grep ' + ', '.join(result_grep)
            print 'sprojections after:', result_projections
            print 'filters after:', result_filters


        das_ql_tuple = (s_result_type, s_input_params, result_projections, result_filters, [])

        result = {
            'result': s_query,
            'trace': traceability,
            'score': score,
            'query_in_words': DASQL_2_NL(das_ql_tuple),
            'entity': s_result_type,
        }
        if best_scores.get(s_query, {'score': -float("inf")})['score'] < score:
            best_scores[s_query] = result

    best_scores = best_scores.values()
    best_scores.sort(key=lambda item: item['score'], reverse=True)

    print '\n'.join(
        ['%.2f: %s' % (r['score'], r['result']) for r in best_scores])

    return best_scores


if __name__ == '__main__':
    # TODO: schema discovery (what are the fields not defined by the mapping)



    # TODO: more restrictive regexp shall win
    #print search('/Aaaa/Bbbb/Cccc')

    #doctest.testmod()


    # TODO: feature, the part of string that matches, e.g. dataset=*valid* vs status=valid !

    if False:
        print search('datasets at T1_CH_CERN')
        print search('datasets at T1_CH_*')

        print search(
            '/SingleElectron/Run2011A-WElectron-PromptSkim-v6/RAW-RECO status')

        print search(
            'is /SingleElectron/Run2011A-WElectron-PromptSkim-v6/RAW-RECO valid')

        print search('location of *Run2012*PromptReco*/AOD')

        print search(
            'where is /SingleElectron/Run2011A-WElectron-PromptSkim-v6/RAW-RECO')

        print search('where is Zmm')

        print search(
            '/SingleElectron/Run2011A-WElectron-PromptSkim-v6/RAW-RECO status')

    print search('number of events in dataset *Run2012*PromptReco*/AOD')


    # site
    print search('custodial of dataset *Run2012*PromptReco*/AOD')

    print search(
        'how many events there are in lumi section 198952 of *Run2012*PromptReco*/AOD'.replace(
            'how many', 'number of'))
    print search(
        'number of events there are in lumi section 198952 of *Run2012*PromptReco*/AOD')

    print search('global tag *Run2012*PromptReco*/AOD')

    print search('dataset = dataset  global tag')

    print search(
        'dataset=/GlobalSep07-A/Online-CMSSW_1_6_0_DAQ3/*  run = 20853 number of events')

    print search(
        'dataset run=148126 & site=T1_US_FNAL')

    print search('Zmm "number of events">10')

    print search('Zmm nevents>10')

    print search('Zmm block.nevents>10')


    #print greedy_chunker([], '*Run2012*PromptReco*/AOD number of events')

    # TODO: complex
    #print search('what is the custodial site of *Run2012*PromptReco*/AOD')