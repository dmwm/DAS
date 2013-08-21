#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
this currently  is a fairly nasty implementation of recursion-based
 exhaustive search scorer/ranker.
"""
# TODO: the code is quite messy, as it was developed as a prototype
#  supporting both the probabilistic logProb and just averaging scoring methods

__author__ = 'vidma'

import heapq

from heapq import heappush, heappushpop

from cherrypy import thread_data

#from DAS.keywordsearch.metadata._das_schema_adapter import validate_input_params, \
#    entities_for_input_params
from DAS.keywordsearch.metadata.schema_adapter_factory import getSchema


from DAS.keywordsearch.config import *

from DAS.keywordsearch.nlp import filter_stopwords, stemmer

from DAS.keywordsearch.tokenizer import get_keyword_without_operator, \
    test_operator_containment

from DAS.keywordsearch.metadata import das_ql



def _get_reserved_terms(stem=False):
    """
    terms that shall be down-ranked if contained in values or in grep-field names
    """
    # TODO: list of entities shall be taken from das_schema_adapter
    entities = ['dataset', 'run', 'block', 'file', 'site', 'config', 'time',
                'lumi']
    operators = das_ql.get_operator_synonyms()
    r = set(entities) | set(operators)

    if stem:
        r = map(lambda w: stemmer.stem(w), r)

    return r


def normalization_factor_by_query_len(keywords_list):
    """
    provides query score normalization factor (expected very good score)
     based on query length (excluding stopwords).
    keywords with operators get double score (as they are scored twice in the ranking)

    TODO: multi word phrases get multiplied score as well

    + 0.3 extra for entity_type and other boost features
    """

    kws_wo_stopwords = filter_stopwords(set(keywords_list))

    _get_phrase_len = lambda kw: len(
        filter_stopwords(set(get_keyword_without_operator(kw).split(' '))))

    expected_optimal_score = \
        sum(2.0 * _get_phrase_len(kw)  if test_operator_containment(kw)
            else 1.0 * _get_phrase_len(kw)
            for kw in kws_wo_stopwords) + 0.3

    if USE_LOG_PROBABILITIES:
        # TODO: we may be more kind, assuming 0.9 or so is very good
        return 1.0

    if expected_optimal_score < 1.0:
        expected_optimal_score = 1.0

    return expected_optimal_score


def penalize_non_mapped_keywords_(keywords_used, keywords_list, score,
                                  result_type=False):
    """
    penalizes keywords that have not been mapped.
    """
    #TODO: take into account different POS.
    # e.g. stopwords shall not be penalized, but in some cases could give increase,
    # e.g. TODO: where is smf, 'how big/large' is


    # TODO: what if we use probabilistic approach


    N_total_kw = len(set(keywords_used))
    N_kw_without_stopw = len(filter_stopwords(set(keywords_list)))

    N_kw_not_used = max(N_kw_without_stopw - len(keywords_used), 0)

    if USE_LOG_PROBABILITIES:
        score += logP(P_NOT_TAKEN) * N_kw_not_used

        # TODO: shall we map field>=value twice as in averaging approach? then not taking is penalized twice...
        # we add the score twice anyways...!!!

        N_not_mapped_all = max(N_total_kw - len(keywords_used) - N_kw_not_used,
                               0)
        score += logP(P_NOT_TAKEN_STOPWORD) * N_not_mapped_all

        if not result_type:
            score += logP(P_NOT_SPECIFIED_RES_TYPE)
        elif result_type == 'projection':
            # this has already +/- accounted for result_type (probability was multiplied)
            # we could add a high probability, so slightly favour explicit result types
            score += logP(0.8)

        return score

    if N_kw_without_stopw:
        return score * min(len(keywords_used), N_total_kw) / N_kw_without_stopw

    return score


# TODO: this has to be implemented in a better way
def penalize_highly_possible_schema_terms_as_values(keyword, schema_ws):
    """
    it is important to avoid missclassifying dataset, run as values.
    these shall be allowed only if explicitly requested.
    """

    if not mod_enabled('DOWNRANK_TERMS_REFERRING_TO_SCHEMA'):
        return 0.0

    # TODO: this is just a quick workaround
    if keyword in _get_reserved_terms(): #['dataset', 'run', 'block', 'file', 'site']:
        # TODO: each reserved term shall have a different weight, e.g. operators lower than entity?
        return logP(-5.0)

    if DEBUG: print '_get_reserved_terms(stem=True):', _get_reserved_terms(
        stem=True)

    if not ' ' in keyword and stemmer.stem(keyword) in _get_reserved_terms(
            stem=True): #['dataset', 'run', 'block', 'file', 'site']:
        # TODO: each reserved term shall have a different weight, e.g. operators lower than entity?
        return logP(-3.0)

    _DEBUG = 0
    # TODO: is stuff below useful at all?
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
            return 3 * min(-0.5, -(keyword_schema_score - avg_score))

    return 0.0


def store_result_(score, r_type, values_dict, r_filters, keywords_used,
                  trace=set(), result_type_specified=True):
    """
    result_type_specified - it is not always specified by the query,
        e.g. 'Zmmg' gives no such information
    """
    store_result_dict_(score, r_type, values_dict, r_filters, keywords_used,
                       trace, result_type_specified=result_type_specified)

    #result =   (score, r_type, values_dict.items(), tuple(r_filters), tuple(trace))
    #thread_data.results.append(result)




def store_result_dict_(score, r_type, values_dict, r_filters, keywords_used,
                       trace=set(), missing_inputs=None,
                       result_type_specified=True):
    # TODO: how to know that result_type is not mapped?
    _score = penalize_non_mapped_keywords_(keywords_used,
                                           thread_data.keywords_list, score,
                                           result_type=result_type_specified)

    _trace = tuple(trace | set([('adjusted_score', _score)]))

    result = {'score': _score,
              'result_type': r_type,
              'input_values': values_dict.items(),
              'result_filters': tuple(r_filters),
              'trace': _trace,
              'status': missing_inputs and 'missing_inputs' or 'OK',
              'missing_inputs': missing_inputs}


    class ScoreDescOrder(object):
            def __init__(self, num=0):
                self.num = num

            # inverse the order
            def __lt__(self, other):
                return self.num < other.num

            def __repr__(self):
                return self.num



    #heap_tuple = (ScoreDescOrder(score), result)
    heap_tuple = (_score, result)

    if DEBUG:
        print 'adding a result:'
        print heap_tuple



    # store only K-best results (if needed)
    if not K_RESULTS_TO_STORE:
        thread_data.results.append(result)
    else:
        # TODO: storing in the other way round may further improve the performance...
        if len(thread_data.results) > K_RESULTS_TO_STORE:
            # this adds the item, and removes the smallest
            popped_out = heappushpop(thread_data.results, heap_tuple)

            # TODO: check if this work as expected
            print 'smalest thrown away result:', popped_out[0], 'len=', len(thread_data.results)

        else:
            heappush(thread_data.results, heap_tuple)


def check_for_pruning(score):

    if False:
        # todo: this could be valid only at the higher stages...
        if not USE_LOG_PROBABILITIES:
            # prune out branches with very low scores
            #  (that is mostly due to sense-less assignments)

            if mod_enabled('PRUNE_NEGATIVE_SCORES') and \
               score < mod_enabled('PRUNE_NEGATIVE_SCORES'):
                return True

    if USE_LOG_PROBABILITIES:
        # low_score could be low_prob**n_kwds, e.g. 0.3**n_kwds, if we get it,
        # it wont be improved as it will always decrease...
        pass



    if False and USE_LOG_PROBABILITIES:
        # TODO: this can not work, because the score is finally
        # being adjusted by number of keywords matched
        # in penalize_non_mapped_keywords_()
        # -- but that would just decrease the score...

        # prune_scores_lower_than_worst
        heap_full = len(thread_data.results) > K_RESULTS_TO_STORE
        if mod_enabled('PRUNE_SCORES_LESS_THAN_WORST') and heap_full:
            # TODO: this is valid  only if we use heap
            worst_score = thread_data.results[0][0]
            threshold = mod_enabled('PRUNE_SCORES_LESS_THAN_WORST')

            if score < worst_score + threshold:
                return True

    return False



def generate_result_filters(keywords_list, chunks, keywords_used,
                            old_score, result_type, values_mapping,
                            result_filters=None, field_idx_start=0,
                            traceability=set(), result_fields_included=set(),
                            result_type_specified=True):
    if check_for_pruning(old_score):
            return


    if result_filters is None:
        result_filters = []

    # try assigning result values for particular entity
    # TODO: we use greedy strategy trying to assign largest keyword sequence (field chunks are sorted)
    requested_entity_short = result_type.split('.')[0]
    for field_idx, match in enumerate(chunks.get(requested_entity_short, [])):
        if field_idx_start > field_idx:
            continue

        target_fieldname = match['field_name']

        # we are anyway including this into filter
        if target_fieldname == result_type:
            continue

        if DEBUG: print 'target_fieldname:', target_fieldname

        # if all required keywords are still available
        if set(match['tokens_required']).isdisjoint(keywords_used) and \
                (target_fieldname not in result_fields_included):
            if DEBUG: print 'required fields available for:', match
            keywords_used_ = keywords_used | set(match['tokens_required'])

            #weight = match['field']['importance'] and 0.2 or 0.1


            _r_filters = result_filters[:]
            target = target_fieldname

            delta_score = match['score']

            if match['predicate']:
                pred = match['predicate']
                target = (target_fieldname, pred['op'], pred['param'])

                if not USE_LOG_PROBABILITIES:
                    # promote matches with operator
                    delta_score *= 2.0

            tokens = filter_stopwords(match['tokens_required'])
            if len(tokens) == 1:
                delta_score += penalize_highly_possible_schema_terms_as_values(
                    tokens[0], None)
            else:

                penalties = map(
                    lambda kwd: penalize_highly_possible_schema_terms_as_values(
                        kwd, None),
                    tokens)
                # for now, use average
                delta_score += len(penalties) and sum(penalties) / len(
                    penalties) or 0.0

            _r_filters.append(target)

            # we count each token separately in scoring
            new_score = old_score + len(tokens) * logP(delta_score)
            #adjusted_score_ = penalize_non_mapped_keywords(
            #                    keywords_used_, keywords_list, new_score)


            # TODO: do we support set literal (set([]) == {}); requires: 2.7a3+
            # http://stackoverflow.com/questions/2243049/what-do-you-think-about-pythons-new-set-literal-2-7a3

            _trace = traceability | set([
                (tuple(match['tokens_required']),
                 'result_projection',
                 target,
                 tuple({'delta_score': delta_score,
                        'field_score': match['score'],
                        'old_score': old_score,
                        'new_score': new_score,
                 }.items())), ])

            store_result_(new_score, result_type,
                          values_mapping,
                          _r_filters,
                          keywords_used_,
                          _trace,
                          result_type_specified=result_type_specified or 'projection')

            if len(keywords_used_) < len(set(keywords_list)):
                generate_result_filters(keywords_list, chunks, keywords_used_,
                                        new_score, result_type, values_mapping,
                                        result_filters=_r_filters,
                                        field_idx_start=field_idx + 1,
                                        traceability=_trace,
                                        result_fields_included=result_fields_included | set(
                                            [target_fieldname]))


def store_result_and_check_projections(
        SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT, UGLY_DEBUG, chunks,
        fields_covered_by_values_set, fields_included, keywords_list,
        keywords_used, old_score, result_projection_forbidden, result_type,
        trace, values_mapping):
    """
    this is run after  mapping keywords into values is done.
    it either stores the specific configuration as a result, or continues with
    generating with mapping the projections/api result post_filters
    """

    #print keyword_index, 'final'
    # DAS requires at least one filtering attribute
    if fields_covered_by_values_set and fields_covered_by_values_set.issuperset(
            fields_included) and \
            getSchema().validate_input_params(fields_included, final_step=True,
                                  entity=result_type):
        #if UGLY_DEBUG: print 'VALUES MATCH:', (
        #    result_type, fields_included, values_mapping ),\
        #validate_input_params(fields_included, final_step=True,
        #    entity=result_type)

        # Adjust the final score to favour mappings that cover most keywords
        # TODO: this could be probably better done by introducing P(Unknown)


        if not result_type:
            # if not entity was guessed, infer it from service parameters
            entities = getSchema().entities_for_inputs(fields_included)
            if UGLY_DEBUG: print 'Result entities matching:', entities

            for result_type in entities.keys():
                adjusted_score = old_score

                if result_type in values_mapping.keys():
                    adjusted_score += SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT

                # TODO: how to handle non mapped items: probablity, divide, minus??
                # TODO: shall we use probability-like scoring?
                #adjusted_score = penalize_non_mapped_keywords(keywords_used,
                #                                              keywords_list,
                #                                              adjusted_score)

                # TODO: are we validating the result 100%?
                store_result_(adjusted_score, result_type, values_mapping,
                    [], keywords_used | result_projection_forbidden, trace,
                              result_type_specified=False)

                generate_result_filters(keywords_list, chunks,
                                        keywords_used | result_projection_forbidden,
                                        old_score, result_type,
                                        values_mapping, traceability=trace,
                                        result_type_specified=False
                )


        else:
            #adjusted_score = penalize_non_mapped_keywords(keywords_used,
            #                                              keywords_list,
            #                                              old_score)

            store_result_(old_score, result_type, values_mapping,
                [], keywords_used | result_projection_forbidden, trace)

            generate_result_filters(keywords_list, chunks,
                                    keywords_used | result_projection_forbidden,
                                    old_score, result_type,
                                    values_mapping, traceability=trace,
                                    result_fields_included=result_projection_forbidden)


def generate_value_mappings(result_type, fields_included, schema_ws,
                            values_ws,
                            old_score, values_mapping=None, # TODO
                            keywords_used=set(),
                            keywords_list=(), keyword_index=0, chunks=(),
                            trace=set(),
                            result_projection_forbidden=set()):
    UGLY_DEBUG = False

    if check_for_pruning(old_score):
            return

    if values_mapping is None:
        values_mapping = {}

    SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT = 0.2

    # TODO: modify the value and schema mappings weights according to previous mappings


    fields_included = set(fields_included)
    fields_covered_by_values_set = set(values_mapping.keys())

    # (as a final condition) now every field in fields_included that were guessed in earlier step, has to be covered by values
    # newones could still be added

    # prune out branches with very low scores (that is due to sence-less assignments)
    if not USE_LOG_PROBABILITIES:
        if mod_enabled('PRUNE_NEGATIVE_SCORES') and old_score < mod_enabled(
                'PRUNE_NEGATIVE_SCORES'):
            return
    else:
        # TODO: pruning is harder
        pass

    if UGLY_DEBUG:
        print 'generate_value_mappings(', \
            result_type, fields_included, schema_ws, values_ws, old_score, \
            values_mapping, keywords_used, keywords_list, keyword_index, ')'

    if keyword_index == len(keywords_list):
        store_result_and_check_projections(
            SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT, UGLY_DEBUG,
            chunks, fields_covered_by_values_set, fields_included,
            keywords_list, keywords_used, old_score,
            result_projection_forbidden, result_type, trace, values_mapping)

        return

    #print 'continuing at index:', keyword_index

    # we either take keyword[i] or not
    keyword = keywords_list[keyword_index]
    keyword_weights = values_ws.get(keyword, [])

    # case 1) we do not take keyword[i]:
    generate_value_mappings(result_type, fields_included, schema_ws,
                            values_ws, old_score, values_mapping,
                            keywords_used, keywords_list,
                            keyword_index=keyword_index + 1,
                            chunks=chunks, trace=trace,
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
                    smapping = [(field_score, smapping) for
                                (field_score, smapping) in
                                schema_ws[keyword]
                                if smapping == possible_mapping]
                    if smapping:
                        # increase by the score of mapping into schema (to only favour likely schema mappings)
                        delta_score += smapping[0][
                                           0] + SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT



            # TODO: penalize keywords that are mapping well to the schema entities
            if True and not '=' in keyword:
                delta_score += penalize_highly_possible_schema_terms_as_values(
                    keyword,
                    schema_ws)

            new_score = logP(delta_score) + old_score
            new_fields = fields_included | set([possible_mapping])

            if getSchema().validate_input_params(new_fields, final_step=False,
                                     entity=result_type):
                generate_value_mappings(result_type,
                                        fields_included=new_fields,
                                        values_mapping=vm_new,
                                        old_score=new_score,
                                        keywords_used=keywords_used | set(
                                            [keyword]),
                                        schema_ws=schema_ws,
                                        values_ws=values_ws,
                                        keyword_index=keyword_index + 1,
                                        keywords_list=keywords_list,
                                        chunks=chunks,
                                        trace=trace | set([(keyword,
                                                            'value_for',
                                                            possible_mapping,
                                                            (
                                                                ('delta_score',
                                                                 delta_score),
                                                                ('old_score',
                                                                 old_score),
                                                                ('new_score',
                                                                 new_score),
                                                            ))]),
                                        result_projection_forbidden=result_projection_forbidden)


                # (as a final condition) now every field in fields_included that were guessed in earlier step, has to be covered by values
                # newones could still be added


def generate_schema_mappings(result_type, fields_old, schema_ws, values_ws,
                             old_score, kw_list=(), kw_index=0,
                             kw_used=set(),
                             chunks=(),
                             trace=set()):
    # TODO: the recursion is dumb, we could at least use some pruning
    # TODO: keyword order is important
    UGLY_DEBUG = False

    if check_for_pruning(old_score):
        return

    # generate_values_mappings()
    # TODO: shall we modify the value and schema mappings weights according to previous mappings HERE or only when doing VALUE matching?

    # TODO: it would be better to consider all items in decreasing scores, so we could just quit after defined amount of time

    if kw_index == len(kw_list):
        # TODO: check if required fields are functioning properly !!!
        if getSchema().validate_input_params(fields_old, final_step=True,
                                 entity=result_type):
            if UGLY_DEBUG: print 'SCHEMA MATCH:', (
                result_type, fields_old), getSchema().validate_input_params(
                fields_old, final_step=True, entity=result_type)


        # if we used a compound keyword A=B for schema, its still available for values
        keywords_used_wo_operators = set(
            filter(lambda k: not '=' in k, kw_used))
        result_projection_forbidden = set(filter(lambda k: '=' in k, kw_used))

        # try to map values based on this
        generate_value_mappings(result_type, fields_old, schema_ws,
                                values_ws, old_score,
                                keywords_used=keywords_used_wo_operators,
                                keywords_list=kw_list,
                                chunks=chunks, trace=trace,
                                result_projection_forbidden=result_projection_forbidden)

        if UGLY_DEBUG: print (
            result_type, fields_old, schema_ws, values_ws)
        return

    # At keyword position (i) we can either:
    #    1) leave it out (a value, or non relevant/not mappable)
    #    2) take keyword i and map it to:
    #        a) requested entity (result type)
    #        b) schema entity (api input param)
    #        c) both
    #


    kwd = kw_list[kw_index]
    schema_w = schema_ws[kwd]

    # opt 1) do not take keyword[i]
    generate_schema_mappings(result_type, fields_old, schema_ws, values_ws,
                             kw_list=kw_list, kw_index=kw_index + 1,
                             old_score=old_score,
                             kw_used=kw_used, chunks=chunks, trace=trace)

    # opt 2) take it:
    for schema_score, target_field in schema_w:
        if target_field in fields_old:
            continue

        fields_new = fields_old[:]
        fields_new.append(target_field)
        #print 'validating', (f, requested_entity)

        # opt 2.a) take as api input param entity
        if getSchema().validate_input_params(fields_new, entity=result_type):
            if UGLY_DEBUG: print 'validated', (result_type, fields_new)

            delta_score = schema_score
            generate_schema_mappings(result_type, fields_new, schema_ws,
                                     values_ws,
                                     kw_list=kw_list,
                                     kw_index=kw_index + 1,
                                     old_score=old_score + logP(delta_score),
                                     kw_used=kw_used | set([kwd]),
                                     chunks=chunks,
                                     trace=trace | set([(kwd, 'schema',
                                                         target_field,
                                                         delta_score)]))


        # opt 2.b) take as requested entity (result type)
        if not result_type:
            delta_score = schema_score

            # TODO: use focus extraction instead!!!
            # if this is the first keyword mapped to schema (we expect entity name to come first)
            if not kw_used and (kw_index + 1) * 1.9 < len(kw_list):
                delta_score *= (
                                   float(len(kw_list)) - kw_index) / len(
                    kw_list)

                if not USE_LOG_PROBABILITIES:
                    delta_score *= 0.5

            if getSchema().validate_input_params(fields_old, entity=target_field):
                if UGLY_DEBUG:  print 'validated', (
                    target_field, fields_old)

                # TODO: currently the score is anyway being increased if a value is being mapped...
                generate_schema_mappings(target_field, fields_old,
                                         schema_ws, values_ws,
                                         kw_list=kw_list,
                                         kw_index=kw_index + 1,
                                         old_score=old_score + logP(
                                             delta_score),
                                         kw_used=kw_used | set([kwd]),
                                         chunks=chunks,
                                         trace=trace | set([(kwd,
                                                             'requested_entity',
                                                             target_field,
                                                             delta_score)]))

            # opt 2.c) take both as requested entity (result type) and  input param entity
            if getSchema().validate_input_params(fields_new,
                                     entity=target_field):

                if UGLY_DEBUG: print 'valid', (target_field, fields_new)

                generate_schema_mappings(target_field, fields_new,
                                         schema_ws, values_ws,
                                         kw_list=kw_list,
                                         kw_index=kw_index + 1,
                                         old_score=old_score + logP(
                                             delta_score),
                                         kw_used=kw_used | set([kwd]),
                                         chunks=chunks,
                                         trace=trace | set([(kwd,
                                                             'requested_entity',
                                                             target_field,
                                                             delta_score)])
                                               | set([(kwd, 'schema',
                                                       target_field,
                                                       delta_score)])
                )

