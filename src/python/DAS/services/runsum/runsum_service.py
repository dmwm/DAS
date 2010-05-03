#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
RunSummary service
"""
__revision__ = "$Id: runsum_service.py,v 1.1 2009/03/09 19:43:34 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService

class RunSummaryService(DASAbstractService):
    """
    Helper class to provide RunSummary service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'runsum', config)
        self.results = []

    def api(self, query, cond_dict=None):
        """
        Invoke RunSummary API to execute given query.
        Return results as a list of dict, e.g.
        [{'run':1,'dataset':/a/b/c'}, ...]
        """
        # TODO: change to real stuff
        # mimic results, we take input query, strip off find and where, extract
        # keys and make a result list with value 1 for each key
        skeys = query.replace('find ', '').split(' where ')[0].split(',')
        res = {}
        for key in skeys:
            res[key] = 1
        results = [res]
        return results


