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

        #if keyword == 'when':
        #    result.extend( [(0.5, 'date'),])
        # TODO: how many [number]
        # TODO: how large/big/... [size (result attribute)]
        # when -- date


    result = filter(lambda item: item[0] > 0, result)

    result.sort(key=lambda item: item[0], reverse=True)
    return result

