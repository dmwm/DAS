#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : das_exceptions.py
Author     : Valentin Kuznetsov <vkuznet@gmail.com>
Description: DAS exceptions module
"""

class WildcardMatchingException(Exception):
    """
    a base exception class for cases when wildcard matching
     needs further user's intervention
    """
    def __init__(self, *args, **kwargs):
        super(WildcardMatchingException, self).__init__(*args, **kwargs)

class WildcardMultipleMatchesException (WildcardMatchingException):
    """
    When a windcard query may have multiple interpretations, we use this
     to communicate these interpretations to the user.
    """
    def __init__(self, message, options = None):
        super(WildcardMultipleMatchesException, self).__init__(message)
        self.message =  message

        if options is None:
            options = {}
        self.options = options

    def __unicode__(self):
        return self.message + '\n'.join(self.options)
    __str__ = __unicode__
