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
The fundamental differ-
ence is that they do not assume any a-priori access to the database instance. Un-
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

from nltk.corpus import stopwords

from DAS.keywordsearch.das_schema_adapter import *
from DAS.keywordsearch.entity_matching import *

import DAS.keywordsearch.das_schema_adapter as integration_schema

DEBUG = False


def penalize_highly_possible_schema_terms_as_values(keyword, new_score,
                                                    schema_ws):
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
    print "avg schema score for '%s' is %.2f; avg schema = %.2f " % (
        keyword, keyword_schema_score, avg_score)
    if avg_score - keyword_schema_score < 0.5:
        new_score += avg_score - keyword_schema_score
        return new_score


def generate_result_filters(N_keywords, chunks, keywords_used,
                            probability, requested_entity, values_mapping,
                            result_filters=[], field_idx_start=0):
    global final_mappings
    # try assigning result values for particular entity
    # TODO: we use greedy strategy trying to assign largest keyword sequence (field chunks are sorted)
    requested_entity_short = requested_entity.split('.')[0]
    for field_idx, field in enumerate(chunks.get(requested_entity_short, [])):
        if field_idx_start > field_idx:
            continue

        # we are anyway including this into filter
        if field['field']['field'] == requested_entity:
            continue

        # if all required keywords are still available
        if set(field['keywords_required']).isdisjoint(keywords_used):
            print 'required fields available for:', field
            keywords_used_ = keywords_used | set(field['keywords_required'])
            probability_ = probability + 0.4 * len(
                set(field['keywords_required']))

            adjusted_score_ = probability_ * len(keywords_used_) / N_keywords

            _result_filters = result_filters[:]
            _result_filters.append(field['field']['field'])

            final_mappings.append((
                adjusted_score_, requested_entity,
                tuple(values_mapping.items()),
                tuple(_result_filters)))

            keywords_used_ = set(field['keywords_required']) | keywords_used
            if len(keywords_used_) < N_keywords:
                generate_result_filters(N_keywords, chunks, keywords_used_,
                    probability_, requested_entity, values_mapping,
                    _result_filters, field_idx_start=field_idx + 1)


def generate_value_mappings(requested_entity, fields_included, schema_ws,
                            values_ws,
                            probability, values_mapping={},
                            keywords_used=set([]),
                            keywords_list=[], keyword_index=0, chunks=[]):
    SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT = 0.3

    # TODO: modify the value and schema mappings weights according to previous mappings
    global final_mappings
    UGLY_DEBUG = False

    fields_included = set(fields_included)
    fields_covered_by_values_set = set(values_mapping.keys())

    # (as a final condition) now every field in fields_included that were guessed in earlier step, has to be covered by values
    # newones could still be added


    if UGLY_DEBUG:
        print 'generate_value_mappings(', requested_entity, fields_included, schema_ws, values_ws, probability, values_mapping, keywords_used, keywords_list, keyword_index, ')'

    if keyword_index == len(keywords_list):
        #print keyword_index, 'final'
        # DAS requires at least one filtering attribute
        if fields_covered_by_values_set and fields_covered_by_values_set.issuperset(
            fields_included) and\
           validate_input_params_mapping(fields_included, final_step=True,
               entity=requested_entity):
            if UGLY_DEBUG: print 'VALUES MATCH:', (
                requested_entity, fields_included, values_mapping ),\
            validate_input_params_mapping(fields_included, final_step=True,
                entity=requested_entity)

            # Adjust the final score to favour mappings that cover most keywords
            # TODO: this could be probably better done by introducing P(Unknown)
            N_keywords = len(schema_ws.keys())

            if not requested_entity:
                # if not entity was guessed, infer it from service parameters
                entities = entities_for_input_params(fields_included)
                if UGLY_DEBUG: print 'Result entities matching:', entities

                for requested_entity in entities.keys():
                    adjusted_score = probability * len(
                        keywords_used) / N_keywords
                    if requested_entity in values_mapping.keys():
                        adjusted_score += SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT
                    final_mappings.append((adjusted_score, requested_entity,
                                           tuple(values_mapping.items()),
                                           tuple([])  ))

                    generate_result_filters(N_keywords, chunks,
                        keywords_used, probability, requested_entity,
                        values_mapping)


            else:
                adjusted_score = probability * len(keywords_used) / N_keywords
                final_mappings.append((
                    adjusted_score, requested_entity,
                    tuple(values_mapping.items()),
                    tuple([]) ))

                generate_result_filters(N_keywords, chunks,
                    keywords_used, probability, requested_entity,
                    values_mapping)

        return

    #print 'continuing at index:', keyword_index

    # we either take keyword[i] or not
    keyword = keywords_list[keyword_index]
    keyword_weights = values_ws.get(keyword, [])

    # case 1) we do not take keyword[i]:
    generate_value_mappings(requested_entity, fields_included, schema_ws,
        values_ws, probability, values_mapping,
        keywords_used, keywords_list, keyword_index=keyword_index + 1,
        chunks=chunks)

    # case 2) we do take keyword[i]:
    if keyword not in keywords_used:
        for score, possible_mapping in keyword_weights:
            keyword_adjusted = keyword
            if isinstance(possible_mapping, dict):
                keyword_adjusted = possible_mapping['adjusted_keyword']
                possible_mapping = possible_mapping['map_to']
            elif '=' in keyword:
                keyword_adjusted = keyword.split('=')[-1]

                # currently we do not allow mapping two keywords to the same value
            # TODO: It could be in theory useful combining a number of consecutive keywords refering to the same value
            if possible_mapping in fields_covered_by_values_set:
                continue

            vm_new = values_mapping.copy()
            vm_new[possible_mapping] = keyword_adjusted

            new_score = probability + score

            # we favour the values mappings that have also been refered in schema mapping (e.g. dataset *Zmm*)
            # TODO: It could be in theory useful combining a number of consecutive keywords refering to the same value
            if possible_mapping in fields_included:
                new_score += SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT

                # we favour mappings which respect X=Y conditions in the keywords
                if '=' in keyword:
                    smapping = [(score, smapping) for (score, smapping) in
                                schema_ws[keyword]
                                if smapping == possible_mapping]
                    if smapping:
                        # increase by the score of mapping into schema (to only favour likely schema mappings)
                        new_score += smapping[0][
                                     0] + SCORE_INCREASE_FOR_SAME_ENTITY_IN_PARAM_AND_RESULT



            # TODO: penalize keywords that are mapping well to the schema entities
            #new_score = penalize_highly_possible_schema_terms_as_values(keyword,
            #    new_score, schema_ws)

            new_fields = fields_included | set([possible_mapping])

            if validate_input_params_mapping(new_fields, final_step=False,
                entity=requested_entity):
                generate_value_mappings(requested_entity,
                    fields_included=new_fields,\
                    values_mapping=vm_new,\
                    probability=new_score,
                    keywords_used=keywords_used | set([keyword]),\
                    schema_ws=schema_ws, values_ws=values_ws,
                    keyword_index=keyword_index + 1,
                    keywords_list=keywords_list,
                    chunks=chunks)


                # (as a final condition) now every field in fields_included that were guessed in earlier step, has to be covered by values
                # newones could still be added


# TODO: the recursion is dumb, we could at least use some pruning
def generate_schema_mappings(requested_entity, fields_old, schema_ws, values_ws,
                             probability, keywords_list=[], keyword_index=0,
                             keywords_used=set([]),
                             chunks=[]):
    # TODO: keyword order is important
    UGLY_DEBUG = False

    # generate_values_mappings()
    # TODO: shall we modify the value and schema mappings weights according to previous mappings HERE or only when doing VALUE matching?

    # TODO: it would be better to consider all items in decreasing scores
    global final_mappings

    if keyword_index == len(keywords_list):
        # TODO: check if required fields are functioning properly !!!
        if validate_input_params_mapping(fields_old, final_step=True,
            entity=requested_entity):
            if UGLY_DEBUG: print 'SCHEMA MATCH:', (
                requested_entity, fields_old), validate_input_params_mapping(
                fields_old, final_step=True, entity=requested_entity)
            #yield (requested_entity, fields_included)
            # TODO: for now, we immediately do recursion on values mappings, but this could be separated into two steps


            if not fields_old: # to be final answer fields must be covered by keywords that represent values
                # and as currently no APIs without parameters are supported, we just skip this...
                #final_mappings.append( (probability, requested_entity, tuple(set(fields_included))) )
                pass

        # if we used a compound keyword A=B for schema, its still available for values
        _keywords_used = set([k for k in keywords_used
                              if not '=' in k])
        # try to map values based on this
        generate_value_mappings(requested_entity, fields_old, schema_ws,
            values_ws, probability,
            keywords_used=_keywords_used, keywords_list=keywords_list,
            chunks=chunks)

        # TODO: I'm still unable to validate some options because the value attributes are missing (if not specified explicitly!)
        # E.G. dataset provided but no keyword 'dataset'

        #if len(keywords_used) == len(schema_ws.items()):
        #    return

        if UGLY_DEBUG: print (
            requested_entity, fields_old, schema_ws, values_ws)
        return

    """ At keyword position (i) we can either:
        1) leave it out (a value, or non relevant/not mappable)
        2) take keyword i and map it to:
            a) requested entity (result type)
            b) schema entity (api input param)
            c) both

    """
    # TODO: later we may consider more complex options -- aggregation, ordering

    #
    #for keyword,schema_w  in schema_ws.items():
    #for index, keyword in enumerate(keywords_list):
    #    # so we visit every combination only once
    #    if index < keyword_index:
    #        return


    keyword = keywords_list[keyword_index]
    schema_w = schema_ws[keyword]

    #if keyword in keywords_used:
    #    #print 'exclud'
    #    pass
    #print 'keyword:', keyword


    # opt 1) do not take keyword[i]
    generate_schema_mappings(requested_entity, fields_old, schema_ws, values_ws,
        keywords_list=keywords_list, keyword_index=keyword_index + 1,
        probability=probability,\
        keywords_used=keywords_used, chunks=chunks)

    # opt 2) take it:
    for score, possible_mapping in schema_w:
        if possible_mapping in fields_old:
            continue

        fields_new = fields_old[:]
        fields_new.append(possible_mapping)
        #print 'validating', (f, requested_entity)

        # opt 2.a) take as api input param entity
        if validate_input_params_mapping(fields_new, entity=requested_entity):
            if UGLY_DEBUG: print 'validated', (requested_entity, fields_new)
            # | set([keyword]
            generate_schema_mappings(requested_entity, fields_new, schema_ws,
                values_ws,
                keywords_list=keywords_list, keyword_index=keyword_index + 1,
                probability=probability + score, keywords_used=keywords_used,
                chunks=chunks)




        # opt 2.b) take as requested entity (result type)
        if not requested_entity:
            entity_score = score

            # if this is the first keyword mapped to schema (we expect entity name to come first)
            if not keywords_used and keyword_index * 1.5 < len(keywords_list):
                entity_score *= 1.8 * (
                    float(len(keywords_list)) - keyword_index) / len(
                    keywords_list)

            if validate_input_params_mapping(fields_old,
                entity=possible_mapping):
                if UGLY_DEBUG:  print 'validated', (
                    possible_mapping, fields_old)

                # TODO: currently the score is anyway being increased if a value is being mapped...
                generate_schema_mappings(possible_mapping, fields_old,
                    schema_ws, values_ws,
                    keywords_list=keywords_list,
                    keyword_index=keyword_index + 1,
                    probability=probability + entity_score,\
                    keywords_used=keywords_used | set([keyword]),
                    chunks=chunks)

            # opt 2.c) take both as requested entity (result type) and  input param entity
            if validate_input_params_mapping(fields_new,
                entity=possible_mapping):
                if UGLY_DEBUG:  print 'validated', (
                    possible_mapping, fields_new)
                #  could this be final mapping

                # TODO: currently the score is anyway being increased if a value is being mapped...
                # as later we may need promote items mapped to operators, we may need to increase the score either here or there
                generate_schema_mappings(possible_mapping, fields_new,
                    schema_ws, values_ws,
                    keywords_list=keywords_list,
                    keyword_index=keyword_index + 1,
                    probability=probability + entity_score,\
                    keywords_used=keywords_used | set([keyword]),
                    chunks=chunks)


# TODO: we may need some extra stop condition...


def greedy_chunker(keywords, keywords_str):
    """
    we currently allow multiple word tokens only for names of output fields
        (input fields are one word [except not important user email];
        input values are always one word,
    """
    # TODO: index some of the possible output values? e.g. status?
    # TODO: prefer complete or almost complete matches

    # TODO: some keywords may contain operators inside  (=, >, <=, etc)
    chunks = keywords

    keywords_str = keywords_str.lower()

    fields_by_entity = list_result_fields()
    # full match
    full_matches = {}

    en_stopwords = stopwords.words('english')
    remove_stopwords = lambda s: ' '.join([w for w in s.split(' ')
                                           if not w in en_stopwords])

    # we could have only ONE entity currently
    for entity, fields in fields_by_entity.items():
        full_matches[entity] = []

        for f in fields.values():
            # full match
            if f['title'] in keywords_str:
                # shall we count stop words?
                # TODO: titles may be not clean, e.g. 'email(s)'
                match = {
                    'field': f,
                    # TODO: stopwords... it's better if full match, but they shall not be required
                    'len': len(f['title'].split(' ')),
                    'keywords_required':
                        f['title'].split(' ')
                    # TODO: currently I assume WORDS DO NOT REPEAT!!!
                }
                full_matches[entity].append(match)

            # full match without stopwords and TODO: no order
            if remove_stopwords(f['title']) in remove_stopwords(keywords_str):
                # shall we count stop words?
                # TODO: titles may be not clean, e.g. 'email(s)'
                match = {
                    'field': f,
                    # TODO: stopwords... it's better if full match, but they shall not be required
                    'len': len(remove_stopwords(f['title']).split(' ')),
                    'keywords_required':
                        remove_stopwords(f['title']).split(' '),
                    # TODO: currently I assume WORDS DO NOT REPEAT!!!
                }
                full_matches[entity].append(match)
                # TODO: Porter-stemmer
                # TODO: free word order
                # TODO: synonyms!!!
                # take the biggest full match

    # return the matches in sorted order (per result type)
    for entity, fields in fields_by_entity.items():
        #print 'trying to sort:'
        #pprint.pprint(full_matches[entity])
        full_matches[entity].sort(key=lambda f: f['len'], reverse=True)

    pprint.pprint(full_matches)
    return full_matches


def DASQL_2_NL(dasql_tuple):
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
    filters = ' AND '.join(['%s=%s' % (f, v) for (f, v) in short_input_params])

    # sum(number of events IN dataset) where dataset=*Zmm*

    if result_projections:
        # TODO: what if entity is different than result_type? We shall probably output that as well...
        result_projections = [
        '%s' % integration_schema.get_result_field_title(result_type, field)
        for field in result_projections
        ]
        result_projections = ', '.join(result_projections)
        return 'RETRIEVE %(result_projections)s FOR EACH %(result_type)s WHERE %(filters)s' % locals()

    return 'RETRIEVE %(result)s WHERE %(filters)s' % locals()


def tokenizer():


def cleanup_query(query):
    """
    Returns cleaned query by applying a number of transformation patterns
    that removes spaces and simplifies the conditions

    >>> cleanup_query('number of events = 33')
    u'number of events=33'

    >>> cleanup_query('number of events >    33')
    u'number of events>33'

    # TODO: new trouble
    number of events more than 33
    with more events than 33
    with more than 33 events
    with 100 or more events
    >>> cleanup_query('more than 33 events')
    u'>33'

    """
    # TODO: preprocess the query
    replacements = {
        # get rid of multiple spaces
        r'\s+': ' ',

        # transform word-based operators
        r'more than (?=\d+)': '>',

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

    if DEBUG: print 'Query:', query

    query = cleanup_query(query)
    if DEBUG: print 'Query after cleanup:', query

    # TODO: some of EN 'stopwords' may be quite important e.g.  'at', 'between', 'where'
    en_stopwords = stopwords.words('english')
    keywords = [kw.strip() for kw in query.split(' ')
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

    chunks = greedy_chunker(keywords, ' '.join(keywords))

    generate_schema_mappings(None, [], schema_ws, values_ws,
        keywords_list=keywords, keyword_index=0, probability=0, chunks=chunks)

    #pprint.pprint(final_mappings)

    print "============= Results for: %s ===" % query
    final_mappings = list(set(final_mappings))
    final_mappings.sort(key=lambda item: item[0], reverse=True)

    best_scores = {}

    first = 1

    for (score, result_type, input_params, result_selections) in final_mappings:
        # short entity names
        s_result_type = entity_names[result_type]
        s_input_params = [(entity_names.get(field, field), value) for
                          (field, value) in input_params]
        s_input_params.sort(key=lambda item: item[0])

        s_query = s_result_type + ' ' + ' '.join(
            ['%s=%s' % (field, value) for (field, value) in s_input_params])

        if result_selections:
            # automatically add wildcard fields to selections (if any), so they would be displayed in the results
            print 'selections before:', result_selections
            result_selections = list(result_selections)
            for (field, value) in input_params:
                if '*' in value:
                    result_selections.append(
                        result_type) # result type of primary key of requested entity
            s_query += ' | grep ' + ', '.join(result_selections)
            print 'selections after:', result_selections

        best_scores[s_query] = max(best_scores.get(s_query, 0.0), score)

        # TODO: print debuging info of how scores were composed!!!
        # print "%.2f: %s %s" % (score, result_type, ' '.join(['%s=%s' % (field, value) for (field, value) in input_params]))
        #print schema_mapping

        if first:
            das_ql_tuple = (
                s_result_type, s_input_params, result_selections, [], [])
            print 'Best query in NL:', DASQL_2_NL(das_ql_tuple)
            first = 0

    best_scores = best_scores.items()
    best_scores.sort(key=lambda item: item[1], reverse=True)

    print '\n'.join(
        ['%.2f: %s' % (score, query) for (query, score) in best_scores])

    return best_scores


def crap(query):
    keyword = query
    for keyword in query.split(' '):
        best = keyword_regexp_weights(keyword)[0]
        print best
    return '%s=%s' % (best[1][0]['key'], keyword)

    #return score_keyword(keyword)[0]


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


    #print greedy_chunker([], '*Run2012*PromptReco*/AOD number of events')

    # TODO: complex
    #print search('what is the custodial site of *Run2012*PromptReco*/AOD')