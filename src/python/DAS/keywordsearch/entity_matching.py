__author__ = 'vidma'
"""
This module generates candidates for matching keywords into entity names, attributes or values.
It uses a number of similarity metrics and heuristics, along with exiting values.
"""

import difflib
import math, re

# for handling semantic and string similarities
import jellyfish
from nltk.corpus import wordnet

from DAS.keywordsearch import input_values_tracker
from DAS.keywordsearch.das_schema_adapter import *

from DAS.keywordsearch.config import mod_enabled

from nltk import stem
stemmer = stem.PorterStemmer()


# TODO: use mapping to entity attributes even independent of the entity itself (idf-like inverted index)

def string_distance(keyword, match_to, semantic=False, allow_low_scores= False):
    # TODO: use some good string similarity metrics: string edit distance, jacard, levenshtein, hamming, etc
    # TODO: use ontology

    # if contains is good, insertions are worse
    score = (jellyfish.jaro_winkler(keyword, match_to) +\
             difflib.SequenceMatcher(a=keyword, b=match_to).ratio() ) / 2

    # TODO: promote matching substrings

    # TODO: similarity shall not be used at all if the words are not similar enough

    if mod_enabled('STRING_DIST_ENABLE_NLTK_PORTER'):
        if stemmer.stem(keyword) == stemmer.stem(match_to):
            score = max(score, 0.7)
        # TODO: shall we do string-distance on top of stemmer?



    # TODO: we shall be able to handle attributes also
    if mod_enabled('STRING_DIST_ENABLE_NLTK_SEMANTICS') and semantic and not '.' in match_to:
        ks = wordnet.synsets(keyword)
        # TODO: we shall can select the relevant synsets for our schema entities manually for improved results
        if entity_wordnet_synsets.has_key(match_to):
            ms = [entity_wordnet_synsets[match_to]]
            #else:
            #ms = wordnet.synsets(match_to)
            if ms and ks:
                avg = lambda l: sum(l)/len(l)

                if DEBUG and keyword == 'location':
                    print 'location similarities to ', match_to, ['%.2f' %  k.wup_similarity(m) for k in ks for m in ms if k.wup_similarity(m)]
                similarities = [k.wup_similarity(m) for k in ks for m in ms if k.wup_similarity(m)]
                semantic_score = similarities and max(similarities) or 0.0

                if score < 0.7:
                    score = semantic_score
                else:
                    score = max(semantic_score, (score + 2*semantic_score)/3)

    if allow_low_scores:
        return score if score > 0.1 else  0
    else:
        return score if score > 0.5 else  0



def keyword_schema_weights(keyword,  use_fields=True, include_fields =False, \
                           include_operators=False,  keyword_index=-1, is_stopword = False):
    """
    for each schema term (entity, entity attribute) calculates keyword's semantic relatedness with it

    based on:
    - similarity to schema terms (string similarity, language ontology [WordNet, ])
        relatedness to entity could also be based on it's fields (e.g. email being field of user,
        especially when other entities do not have such field)

    - semantic distance (google similarity distance) TODO: ref [11]

    later this could also include operators (last 24h, 2days, 10 largest datasets, aggreation and attribute selection)
    e.g. 'latest' would be closest to 'date', 'largest' -> size

    (TODO: ideally a separate method shall use schema ontology if exists to use relations between different entities)

    Unit tests:
    >>> keyword_schema_weights('time')[0][1] # shall be close to date
    u'date'
    >>> keyword_schema_weights('location')[0][1] # shall be close to site
    u'site'
    >>> keyword_schema_weights('configuration')[0][1] # shall be close to config
    u'config'

    # TODO: this weight shall be lower than the one below?
    >>> keyword_schema_weights('email')[0][1] # user contains email field
    u'user'
    >>> keyword_schema_weights('email', include_fields=True)[0][1] # user contains email field
    u'user.email'
    """

    # TODO: use fields to map to entities
    # TODO: operators
    # TODO: use IDF (some field subparts are very common, e.g. name)

    result =  []

    if not is_stopword:
        if include_fields:
            result = [(string_distance(keyword, entity, semantic=True), entity_long)
                       for (entity_long, entity) in entity_names.items()]
        else:
            if use_fields:
                # for now a very dumb matching, to both entity.name and entity by string comparison
                result = [(string_distance(keyword, entity_with_field, semantic=True), entity)
                           for (entity_with_field, entity) in entity_names.items()]

                result.extend([(string_distance(keyword, entity, semantic=True), entity)
                               for (entity_with_field, entity) in entity_names.items()])
            else:
                result =  [(string_distance(keyword, entity, semantic=True), entity)
                           for entity in search_field_names]


    # apply some simple patterns
    if keyword_index == 0:
        # TODO: we actually know even more: this is the result type
        if keyword == 'where':
            result.extend( [(0.6, 'site.name'), ])
        if keyword == 'who':
            result.extend( [(0.5, 'user.name'),])
        # TODO: how many [number]
        # TODO: how large/big/... [size (result attribute)]
        # when -- date


    result = filter(lambda item: item[0] > 0, result)

    result.sort(key=lambda item: item[0], reverse=True)
    return result

def keyword_value_weights(keyword, api_results_allowed=False):
    """
    for each attribute, calculates possibility that given keyword is a value of the attribute
    (we are mostly interested in API parameters, but TODO: this could be extended to API result fields also for post filtering)

    this is done by employing some of these methods:
    - string similarity to known (or historical) values
        (while positives confirm the belonging to certain field,
        the negatives shall not exclude such possibility  as historical values we have are not complete, )
    - matching to regexps defining the values
    """

    # TODO: we wish the generated query to match the APIs (so we promote such queries
    # even if combined queries were implemented/enabled, we still wish to match API parameters with higher priority

    scores_by_entity = {}
    for (score, matches_for_api) in keyword_regexp_weights(keyword):
        for m in matches_for_api:
            entity_matched = m['entity_long']

            # to minimize false positives, we exclude the fields from regexp matching
            # for which we have a list of possible values (the quite static ones)

            if entity_matched not in input_values_tracker.get_fields_tracked() and\
               score > scores_by_entity.get(entity_matched, 0):
                scores_by_entity[entity_matched] = (score, entity_matched)


    # check for matching of existing datasets
    # TODO: use instance from elsewhere (from web server if available)
    # TODO: check issue with Malik's datasets

    # TODO: we could actually get multiple interpretations (e.g. ambigous wildcard query)
    map_to_field, dataset_score, adj_keyword = match_value_dataset(keyword)
    if dataset_score:
        # TODO: shouldn't succesfull value lookup based score override the regexp based?
        scores_by_entity[map_to_field] = (dataset_score,  {'map_to': 'dataset.name', 'adjusted_keyword': adj_keyword})


    # check for matching fields those values are fairly static (site, release, ...)
    scores_by_entity.update(input_values_tracker.input_value_matches(keyword))



    #TODO: do we need partial matches?



    # finally convert back to a sorted list (for backward compatibility)
    scores = scores_by_entity.values()
    scores.sort(key=lambda item: item[0], reverse=True)
    return scores


def keyword_regexp_weights(keyword):
    # TODO: shall value match definitions of ALL APIs or some!? I'd say some is enough
    scores = []

    # TODO: define that is more restrictive regexp

    for (constraint, apis) in apis_by_their_input_contraints.items():
    #print (constraint, apis)


        # TODO: I've hacked file/dataset regexps to be more restrictive as these are well defined
        if not '^' in constraint and not '$' in constraint and apis[0]['key'] in ['dataset', 'file',]:
            constraint = '^' + constraint + '$'
        score = 0

        # We shall prefer non empty constraints
        # We may also have different weights for different types of regexps
        if re.search(constraint, keyword):
            #print apis

            if constraint.startswith('^') and  constraint.endswith('$'):
                score = 0.7
            elif constraint.startswith('^') or  constraint.endswith('$'):
                score = 0.6
            elif constraint != '':
                score = 0.5

            score = (score, apis)

            scores.append(score)

    scores.sort(key=lambda item: item[0], reverse=True)
    #print scores
    return scores


