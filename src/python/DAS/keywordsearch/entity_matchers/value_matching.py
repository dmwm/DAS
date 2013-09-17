"""
Value matching functions that evaluate if given input is similar to some of
the value terms in the underlying data integration system.

Also some CMS specific functions are used:
- dataset name matching
"""
__author__ = 'vidma'

import re

from  collections import defaultdict

# for handling semantic and string similarities
from DAS.keywordsearch.metadata import input_values_tracker
from DAS.keywordsearch.metadata.schema_adapter_factory import getSchema
from DAS.keywordsearch.entity_matchers.value_matching_dataset import match_value_dataset
from DAS.utils import regex

def keyword_value_weights(keyword):
    """
    for each attribute, calculates possibility that given keyword is a value of the attribute
    (we are mostly interested in API parameters, but TODO: this could be extended to API result fields also for post filtering)

    this is done by employing some of these methods:
    - string similarity to known (or historical) values
        (while positives confirm the belonging to certain field,
        the negatives shall not exclude such possibility  as historical values we have are not complete, )
    - matching to regexps defining the values
    """

    # to minimize false positives, we exclude the fields from regexp matching
    # for which we have a list of possible values (the quite static ones)
    fields_tracked = input_values_tracker.get_fields_tracked(only_stable=True)

    scores_dict = _select_best_scores(
        (score, field) for score, field in keyword_regexp_weights(keyword)
        if field not in fields_tracked)

    # check for matching of existing datasets, and override regexp based score
    # TODO: use instance from elsewhere (from web server if available)
    field, dataset_score, adj_kwd = match_value_dataset(keyword)
    if dataset_score:
        scores_dict[field] = (dataset_score,  {'map_to': 'dataset.name',
                                          'adjusted_keyword': adj_kwd})

    # check for matching fields those values are fairly static (site, release, ...)
    scores_dict.update(input_values_tracker.input_value_matches(keyword))

    return sorted(scores_dict.itervalues(),
                  key=lambda item: item[0], reverse=True)


# TODO: precompile the regexps, this could be slight optimized!
def keyword_regexp_weights(keyword):
    for constraint, apis in getSchema().apis_by_their_input_contraints.iteritems():

        # I've hacked file/dataset regexps to be more restrictive as these are well defined
        if not '^' in constraint and not '$' in constraint and \
                apis[0]['key'] in ['dataset', 'file', 'reco_status', 'run']:
            constraint = '^' + constraint + '$'

        # do not alow # in dataset TODO: shall be moved to API mappings
        if apis[0]['key'] == 'dataset' and '#' in keyword:
            continue

        score = 0

        # We shall prefer non empty constraints
        # We may also have different weights for different types of regexps
        if re.search(constraint, keyword):
            if constraint.startswith('^') and  constraint.endswith('$'):
                score = 0.7
            elif constraint.startswith('^') or  constraint.endswith('$'):
                score = 0.6
            elif constraint != '':
                score = 0.5

        if score:
            for api in apis:
                yield score, api['entity_long']

    # append date match...
    if regex.date_yyyymmdd_pattern.match(keyword):
        yield 0.95, 'date'


def  _select_best_scores(scores_iterator):
    """
    select only the best score, if multiple opts are available for the same item
    """
    scores = defaultdict(float)
    for score, field in scores_iterator:
        scores[field] = max(scores[field], score)

    for field, score in scores.iteritems():
        scores[field] = (score, field)

    return scores
