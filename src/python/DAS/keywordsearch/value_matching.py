__author__ = 'vidma'


import difflib
import math, re

# for handling semantic and string similarities
import jellyfish
from nltk.corpus import wordnet

from DAS.keywordsearch import input_values_tracker
from DAS.keywordsearch.das_schema_adapter import *

from DAS.keywordsearch.config import mod_enabled

#from nltk import stem
#stemmer = stem.PorterStemmer()


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

            if entity_matched not in input_values_tracker.get_fields_tracked() and \
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
        if not '^' in constraint and not '$' in constraint and apis[0]['key'] \
            in ['dataset', 'file', 'reco_status', 'run']:
            constraint = '^' + constraint + '$'
        score = 0


        #if apis[0]['key'] in ['reco_status']:
        #    print 'reco_status (any)'
        #    pprint.pprint((constraint, apis))


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
                #if apis[0]['key'] in ['reco_status']:
                #    print 'reco_status 0.5'
                #    pprint.pprint((constraint, apis))

            score = (score, apis)

            scores.append(score)

    scores.sort(key=lambda item: item[0], reverse=True)
    #print scores
    return scores


