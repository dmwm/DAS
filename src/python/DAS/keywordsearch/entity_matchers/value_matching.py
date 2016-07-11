#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Value matching functions that evaluate if given input is similar to some of
the value terms in the underlying data integration system.

Also some CMS specific functions are used:
- dataset name matching
"""
import re
from collections import defaultdict

# for handling semantic and string similarities
from DAS.keywordsearch.metadata import input_values_tracker
from DAS.keywordsearch.metadata.schema_adapter_factory import get_schema
from DAS.keywordsearch.entity_matchers.value_matching_dataset \
    import match_value_dataset
from DAS.utils import regex


def keyword_value_weights(keyword):
    """
    for each attribute, calculates likelihood that given keyword is a value of
    the attribute  (we are mostly interested in API parameters, but
    """

    # to minimize false positives, we exclude the fields from regexp matching
    # for which we have a list of possible values (the quite static ones)
    fields_tracked = input_values_tracker.get_fields_tracked(only_stable=True)

    scores_dict = _select_best_scores(
        (score, field) for score, field in keyword_regexp_weights(keyword)
        if field not in fields_tracked)

    # check for matching of existing datasets, and override regexp based score
    dataset_score, data = match_value_dataset(keyword)
    if dataset_score:
        scores_dict['dataset.name'] = (dataset_score,  data)

    # check for matching fields those values are fairly static (site, release..)
    scores_dict.update(input_values_tracker.input_value_matches(keyword))

    return sorted(scores_dict.itervalues(),
                  key=lambda item: item[0], reverse=True)


def keyword_regexp_weights(keyword):
    """ evaluate keyword regexp matches """
    regexps = get_schema().compiled_input_regexps
    for re_compiled, constraint, apis in regexps:
        # do not allow # in dataset
        if '#' in keyword:
            apis = [api
                    for api in apis
                    if api['key'] != 'dataset']
            if not apis:
                continue

        score = 0
        # We shall prefer non empty constraints
        # We may also have different weights for different types of regexps
        if re.search(re_compiled, keyword):
            if constraint.startswith('^') and constraint.endswith('$'):
                score = 0.7
            elif constraint.startswith('^') or constraint.endswith('$'):
                score = 0.6
            elif constraint != '':
                score = 0.5

        if score:
            for api in apis:
                yield score, api['entity_long']

    # append date match...
    if regex.date_yyyymmdd_pattern.match(keyword):
        yield 0.95, 'date'


def _select_best_scores(scores_iterator):
    """
    select only the best score, if multiple opts are available for the same item
    """
    scores = defaultdict(float)
    for score, field in scores_iterator:
        scores[field] = max(scores[field], score)

    for field, score in scores.items():
        scores[field] = (score, field)

    return scores
