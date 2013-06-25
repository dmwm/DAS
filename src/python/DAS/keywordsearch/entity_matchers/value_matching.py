"""
Value matching functions that evaluate if given input is similar to some of
the value terms in the underlying data integration system.

Also some CMS specific functions are used:
- dataset name matching
"""
__author__ = 'vidma'

import re


from cherrypy import request

from   DAS.utils.regex import RE_3SLAHES


# for handling semantic and string similarities
from DAS.keywordsearch.metadata import input_values_tracker
from DAS.keywordsearch.metadata.das_schema_adapter import *

from DAS.core.das_process_dataset_wildcards import get_global_dbs_mngr




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

    # TODO: we wish the generated query to match the APIs (so we promote such queries
    # even if combined queries were implemented/enabled, we still wish to match API parameters with higher priority

    scores_by_entity = {}
    for (score, matches_for_api) in keyword_regexp_weights(keyword):
        for m in matches_for_api:
            entity_matched = m['entity_long']

            # to minimize false positives, we exclude the fields from regexp matching
            # for which we have a list of possible values (the quite static ones)

            if entity_matched not in input_values_tracker.get_fields_tracked(only_stable=True) and \
                            score > scores_by_entity.get(entity_matched, 0):
                scores_by_entity[entity_matched] = (score, entity_matched)

            # TODO: shall we add with very low score even other matches?


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

        # do not alow # in dataset TODO: shall be moved to API mappings
        if apis[0]['key'] == 'dataset' and '#' in keyword:
            continue


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




# TODO: move this to value matching?
def match_value_dataset(keyword):
    if hasattr(request, 'dbsmngr'):
        dbsmgr = request.dbsmngr
    else:
        dbsmgr = request.dbsmngr = get_global_dbs_mngr()

    print 'DBS mngr:', dbsmgr

    dataset_score = None
    upd_kwd = keyword
    # dbsmgr.find returns a generator, to check if it's non empty we have to access it's entities
    # TODO: check for full and partial match
    # e.g. /DoubleMu/Run2012A-Zmmg-13Jul2012-v1 --> /DoubleMu/Run2012A-Zmmg-13Jul2012-v1/*
    # DoubleMu -> *DoubleMu*
    # TODO: a dataset pattern could be even *Zmm* -- we need minimum length here!!

    if next(dbsmgr.find(pattern=keyword, limit=1), False):
        print 'Dataset matched by keyword %s' % keyword
        # TODO: if contains wildcards score shall be a bit lower
        if '*' in keyword and not '/' in keyword:
            dataset_score = 0.8
        elif '*' in keyword and '/' in keyword:
            dataset_score = 0.9
        elif not '*' in keyword and not '/' in keyword:
            if next(dbsmgr.find(pattern='*%s*' % keyword, limit=1), False):
                dataset_score = 0.7
                upd_kwd = '*%s*' % keyword
        else:
            dataset_score = 1.0

    # TODO: shall we check for unique matches?

    # it's better to add extra wildcard to make sure the query will work...
    if not RE_3SLAHES.match(upd_kwd):
        upd_kwd0 = upd_kwd

        if  not upd_kwd.startswith('*') and not upd_kwd.startswith('/'):
            upd_kwd =  '*' + upd_kwd

        if not upd_kwd.endswith('*') and \
            not (upd_kwd0.startswith('/') or upd_kwd0.startswith('*')):
            upd_kwd =  upd_kwd + '*'


    print 'dataset.name', dataset_score, upd_kwd


    return 'dataset.name', dataset_score, upd_kwd


