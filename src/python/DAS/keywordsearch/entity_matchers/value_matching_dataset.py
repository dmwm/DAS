"""
Value matching functions that evaluate if given input is similar to some of
the value terms in the underlying data integration system.

Also some CMS specific functions are used:
- dataset name matching
"""
__author__ = 'vidma'

from cherrypy import request

from DAS.utils.regex import RE_3SLAHES
from DAS.core.das_process_dataset_wildcards import get_global_dbs_mngr
from DAS.keywordsearch.config import DEBUG

def match_value_dataset(keyword):
    if hasattr(request, 'dbsmngr'):
        dbsmgr = request.dbsmngr
    else:
        dbsmgr = request.dbsmngr = get_global_dbs_mngr()

    #print 'DBS mngr:', dbsmgr

    dataset_score = None
    upd_kwd = keyword

    # dbsmgr.find returns a generator, to check if it's non empty we have to access it's entities
    # TODO: a dataset pattern could be even *Zm* -- we need minimum length!!

    if next(dbsmgr.find(pattern=keyword, limit=1), False):
        if DEBUG: print 'Dataset matched by keyword %s' % keyword
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

        # prevent number-only-keywords to be matched into datasets with high score
        if keyword.isnumeric():
            dataset_score -= 0.3

    # TODO: shall we check for unique matches?

    # it's better to add extra wildcard to make sure the query will work...
    if not RE_3SLAHES.match(upd_kwd):
        upd_kwd0 = upd_kwd

        if  not upd_kwd.startswith('*') and not upd_kwd.startswith('/'):
            upd_kwd =  '*' + upd_kwd

        if not upd_kwd0.endswith('*') or '*' not in upd_kwd0:
            upd_kwd += '*'


    return 'dataset.name', dataset_score, upd_kwd


