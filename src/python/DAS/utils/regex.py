#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=C0103

"""
Regular expression patterns
"""

__revision__ = "$Id: regex.py,v 1.3 2010/04/13 14:58:15 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Valentin Kuznetsov"

# system modules
import re

ip_address_pattern = \
    re.compile(r"^([0-9]{1,3}\.){3,3}[0-9]{1,3}$")
last_time_pattern = \
    re.compile('^[0-9][0-9](h|m)$') # 24h or 12m
date_yyyymmdd_pattern = \
    re.compile('[0-2]0[0-9][0-9][0-1][0-9][0-3][0-9]')
key_attrib_pattern = \
    re.compile(r"^([a-zA-Z_]+\.?)+(=[a-zA-Z0-9]*)?$") # match key.attrib
#    re.compile(r"^([a-zA-Z_]+\.?)+$") # match key.attrib
cms_tier_pattern = \
    re.compile('T[0-9]_') # T1_CH_CERN
float_number_pattern = \
    re.compile(r'(^[-]?\d+\.\d*$|^\d*\.{1,1}\d+$)')
int_number_pattern = \
    re.compile(r'(^[0-9-]$|^[0-9-][0-9]*$)')
phedex_tier_pattern = \
    re.compile('^T[0-9]_[A-Z]+(_)[A-Z]+') # T2_UK_NO
se_pattern = \
    re.compile('[a-z]+(\.)[a-z]+(\.)') # a.b.c
site_pattern = \
    re.compile('^[A-Z]+') 
web_arg_pattern = \
    re.compile('^[+]?\d*$') # used in web form, e.g idx=0
number_pattern = \
    re.compile('^[-]?[0-9][0-9\.]*$') # -123
dataset_path = \
    re.compile('/[a-zA-Z0-9\-]+/[a-zA-Z0-9\-]+')
last_key_pattern = \
    re.compile('date\s+last')
unix_time_pattern = \
    re.compile('[0-9]{10}')

# To be used in web module
RE_DBSQL_0 = re.compile(r"^find")
RE_DBSQL_1 = re.compile(r"^find\s+(\w+)")
RE_DBSQL_2 = \
    re.compile(r"^find\s+(\w+)\s+where\s+([\w.]+)\s*(=|in|like)\s*(.*)")
RE_DATASET = re.compile(r"^/\w+")
RE_SITE = re.compile(r"^T[0123]_")
RE_SUBKEY = re.compile(r"^([a-z_]+\.[a-zA-Z_]+)")
RE_KEYS = re.compile(r"""\
([a-z_]+)\s?(?:=|in|between|last)\s?(".*?"|'.*?'|[^\s]+)|([a-z_]+)""")
RE_COND_0 = re.compile(r"^([a-z_]+)")
RE_HASPIPE = re.compile(r"^.*?\|")
RE_PIPECMD = re.compile(r"^.*?\|\s*(\w+)$")
RE_AGGRECMD = re.compile(r"^.*?\|\s*(\w+)\(([\w.]+)$")
RE_FILTERCMD = re.compile(r"^.*?\|\s*(\w+)\s+(?:[\w.]+\s*,\s*)*([\w.]+)$")
RE_K_SITE = re.compile(r"^s")
RE_K_FILE = re.compile(r"^f")
RE_K_PR_DATASET = re.compile(r"^pr")
RE_K_PARENT = re.compile(r"^pa")
RE_K_CHILD = re.compile(r"^ch")
RE_K_CONFIG = re.compile(r"^co")
RE_K_GROUP = re.compile(r"^g")
RE_K_DATASET = re.compile(r"^d")
RE_K_BLOCK = re.compile(r"^b")
RE_K_RUN = re.compile(r"^ru")
RE_K_RELEASE = re.compile(r"^re")
RE_K_TIER = re.compile(r"^t")
RE_K_MONITOR = re.compile(r"^m")
RE_K_JOBSUMMARY = re.compile(r"^j")
