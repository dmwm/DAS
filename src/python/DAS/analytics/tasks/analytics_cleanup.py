#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=R0903
"""
File: AnalyticsCleanup.py
Author: Gordon Ball <gordon.ball@cern.ch>
Description: A simple analytics task, intended for 
             infrequent running to clean up old summaries
"""

class AnalyticsCleanup(object):
    """
    "Analyzer" intended for infrequent (~weekly) running, which cleans up
    analysis summaries (from any analyser) older than "cutoff" seconds.
    """
    task_options = [{'name':'cutoff', 'type':'int', 
                     'default':60*60*24*60,
    'help':'Delete analysis summaries older than this many seconds.'}]
    
    
    def __init__(self, **kwargs):
        self.logger = kwargs['logger']
        self.das = kwargs['DAS']
        self.cutoff = kwargs.get('cutoff', 60*60*24*60) #2 months
        
    def __call__(self):
        self.logger.info("Cleaning summaries older than %d days", 
                         (self.cutoff/86400))
        
        self.das.analytics.col.remove({'analyzer': {'$exists': True}, 
                                       'finish': {'$lte': self.cutoff}})

