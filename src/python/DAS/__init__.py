#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=C0103

"""
DAS modules

.. moduleauthor:: Valentin Kuznetsov <vkuznet@gmail.com>
"""
__author__ = "Valentin Kuznetsov"
version = "development"

import sys
vinfo = sys.version_info[:2]
DAS_SERVER = 'das-server/%s::python/%s.%s' % (version, vinfo[0], vinfo[-1])
