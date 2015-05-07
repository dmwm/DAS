#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Value matching functions that evaluate if given input is similar to some of
the value terms in the underlying data integration system.

Also some CMS specific functions are used:
- dataset name matching
"""
from __future__ import print_function

from cherrypy import request
from DAS.utils.regex import RE_3SLASHES
from DAS.keywordsearch.config import DEBUG
from DAS.web.dbs_daemon import find_datasets


def match_value_dataset(kwd, dbs_inst=None):
    """ return keyword matches to dataset values in dbsmanager """
    # if no specific dbs_inst passed, get the current one from request
    if not dbs_inst:
        if not hasattr(request, 'dbs_inst'):
                return None, None
        dbs_inst = request.dbs_inst

    dataset_score = None

    # make sure the kwd is unicode
    if not isinstance(kwd, unicode) and isinstance(kwd, str):
        kwd = unicode(kwd)

    upd_kwd = kwd

    # dbsmgr.find returns a generator, check if it's non empty
    match = find_datasets(kwd, dbs_inst, limit=1)
    if next(match, False):
        if DEBUG:
            print('Dataset matched by keyword %s' % kwd)
        # if kw contains wildcards the score shall be a bit lower
        if '*' in kwd and not '/' in kwd:
            dataset_score = 0.8
        elif '*' in kwd and '/' in kwd:
            dataset_score = 0.9
        elif not '*' in kwd and not '/' in kwd:
            if next(find_datasets('*%s*' % kwd, dbs_inst, limit=1), False):
                dataset_score = 0.7
                upd_kwd = '*%s*' % kwd
        else:
            dataset_score = 1.0

        # prevent number-only-keywords to be matched into datasets
        if kwd.isnumeric():
            dataset_score -= 0.3

    # add extra wildcard to make sure the query will work...
    if not RE_3SLASHES.match(upd_kwd):
        upd_kwd0 = upd_kwd
        if not upd_kwd.startswith('*') and not upd_kwd.startswith('/'):
            upd_kwd = '*' + upd_kwd
        if not upd_kwd0.endswith('*') or '*' not in upd_kwd0:
            upd_kwd += '*'

    return dataset_score, {'map_to': 'dataset.name',
                           'adjusted_keyword': upd_kwd}
