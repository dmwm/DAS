#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=R0921,R0903
"""
Report associated with the lifetime task, to show how often data
actually changes for different services (and hence whether we should
trust their stated expiry times)
"""

from DAS.analytics.utils import Report

class LifetimeReport(Report):
    "Lifetime report class skeleton"
    def __call__(self):
        raise NotImplementedError
