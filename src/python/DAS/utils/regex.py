#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=C0103

"""
Regular expression patterns
"""

__author__ = "Valentin Kuznetsov"

# system modules
import re
import calendar

def word_chars(word, equal=True):
    """
    Creates a pattern of given word as series of its characters, e.g.
    for given word dataset I'll get
    '^d$|^da$|^dat$|^data$|^datas$|^datase$|^dataset$'
    which can be used later in regular expressions
    """
    pat = r'|'.join(['^%s$' % word[:x+1] for x in xrange(len(word))])
    if  equal:
        pat += '|^%s=' % word
    return pat

days = [d for d in calendar.day_abbr if d]
months = [m for m in calendar.month_abbr if m]
pattern = r'(%s), \d\d (%s) 20\d\d \d\d:\d\d:\d\d GMT' \
        % ('|'.join(days), '|'.join(months))
http_ts_pattern = re.compile(pattern)

# HTTP header message
pat_http_msg = re.compile(r'HTTP\/\S*\s*\d+\s*(.*?)\s*$')
# HTTP header Expires message
pat_expires  = re.compile(r'Expires:')

http_pattern = \
    re.compile(r"http://.*|https://.*")
ip_address_pattern = \
    re.compile(r"^([0-9]{1,3}\.){3,3}[0-9]{1,3}$")
last_time_pattern = \
    re.compile(r'^[0-9][0-9](h|m)$') # 24h or 12m
rr_time_pattern = \
    re.compile(r'^[A-Za-z]{3} [0-9]{2}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$')
# das time should be in isoformat, see datetime.isoformat()
das_time_pattern = \
    re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$')
date_yyyymmdd_pattern = \
    re.compile(r'[0-2]0[0-9][0-9][0-1][0-9][0-3][0-9]')
key_attrib_pattern = \
    re.compile(r"^([a-zA-Z_]+\.?)+(=[a-zA-Z0-9]*)?$") # match key.attrib
#    re.compile(r"^([a-zA-Z_]+\.?)+$") # match key.attrib
cms_tier_pattern = \
    re.compile(r'T[0-9]_') # T1_CH_CERN
float_number_pattern = \
    re.compile(r'(^[-]?\d+\.\d*$|^\d*\.{1,1}\d+$)')
int_number_pattern = \
    re.compile(r'(^[0-9-]$|^[0-9-][0-9]*$)')
phedex_node_pattern = \
    re.compile(r'^T[0-9]_[A-Z]+(_)[A-Z]+') # T2_UK_NO
se_pattern = \
    re.compile(r'[a-z]+(\.)[a-z]+(\.)') # a.b.c
site_pattern = \
    re.compile(r'^[A-Z]+')
web_arg_pattern = \
    re.compile(r'^[+]?\d*$') # used in web form, e.g idx=0
number_pattern = \
    re.compile(r'^[-]?[0-9][0-9\.]*$') # -123
dataset_path = \
    re.compile(r'/[a-zA-Z0-9\-]+/[a-zA-Z0-9\-]+')
last_key_pattern = \
    re.compile(r'date\s+last')
unix_time_pattern = \
    re.compile(r'^\d{10}$|^\d{10}\.\d+$')

# To be used in web module
RE_DBSQL_0 = re.compile(r"^find")
RE_DBSQL_1 = re.compile(r"^find\s+(\w+)")
RE_DBSQL_2 = \
    re.compile(r"^find\s+(\w+)\s+where\s+([\w.]+)\s*(=|in|like)\s*(.*)")
RE_DATASET = re.compile(r"^/\w+")
RE_3SLASHES = re.compile(r"/[^/]+/[^/]+/[^/]+$")
RE_SITE = re.compile(r"^T[0123]_")
RE_SUBKEY = re.compile(r"^([a-z_]+\.[a-zA-Z_]+)")
RE_KEYS = re.compile(r"""\
([a-z_]+)\s?(?:=|between|last)\s?(".*?"|'.*?'|[^\s]+)|([a-z_]+)""")
RE_COND_0 = re.compile(r"^([a-z_]+)")
RE_HASPIPE = re.compile(r"^.*?\|")
RE_PIPECMD = re.compile(r"^.*?\|\s*(\w+)$")
RE_AGGRECMD = re.compile(r"^.*?\|\s*(\w+)\(([\w.]+)$")
RE_FILTERCMD = re.compile(r"^.*?\|\s*(\w+)\s+(?:[\w.]+\s*,\s*)*([\w.]+)$")
RE_K_SITE = re.compile(word_chars("site"))
RE_K_FILE = re.compile(word_chars("file"))
RE_K_PR_DATASET = re.compile(word_chars("primary_dataset"))
RE_K_PARENT = re.compile(word_chars("parent"))
RE_K_CHILD = re.compile(word_chars("child"))
RE_K_CONFIG = re.compile(word_chars("config"))
RE_K_GROUP = re.compile(word_chars("group"))
RE_K_DATASET = re.compile(word_chars("dataset"))
RE_K_BLOCK = re.compile(word_chars("block"))
RE_K_STATUS = re.compile(word_chars("status"))
RE_K_RUN = re.compile(word_chars("run"))
RE_K_RELEASE = re.compile(word_chars("release"))
RE_K_TIER = re.compile(word_chars("tier"))
RE_K_MONITOR = re.compile(word_chars("monitor"))
RE_K_JOBSUMMARY = re.compile(word_chars("jobsummary"))

# concrete patterns for CMS data
PAT_SLASH = re.compile(r'^/.*')
PAT_BLOCK = re.compile(r'^/.*/.*/.*\#.*')
PAT_RUN  = re.compile(r'^[0-9]{3,10}')
PAT_FILE = re.compile(r'^/.*\.root$')
PAT_RELEASE = \
    re.compile(r'^CMSSW_|^[0-9]_$|^[0-9]_[0-9]|' + word_chars('CMSSW_'))
PAT_SITE = re.compile(r'^T[0-3]')
PAT_SE = re.compile(r'([a-zA-Z0-9-_]+\\.){1,4}')
PAT_DATATYPE = re.compile(r'^mc$|^calib$|^data$|^raw$|^cosmic$', re.I)
PAT_TIERS = \
    re.compile(r'gen|sim|raw|digi|reco|alcoreco|hlt|fevt|alcaprompt|dqm', re.I)

# slashes handling in dataset Wildcard queries
# allowed characters: letters, numbers, dashes and obviously  *
DATASET_FORBIDDEN_SYMBOLS = re.compile(r'[^a-zA-Z0-9_\-*/]*')


# rules for rewriting little ambiguous input into DASQL
DATASET_SYMBOLS = r'[a-zA-Z0-9_\-*]'
NON_AMBIGUOUS_INPUT_PATTERNS = [
    (name, re.compile(rule)) for name, rule in [
        ('dataset', '^(/%s+){3}$' % DATASET_SYMBOLS),  # no #
        ('block', r'^/.+/.+/.+#.+'),
        ('file', r'^/.*\.root$'),
        ('release', r'^CMSSW_'),
        ('site', r'^T[0-3]_')]
]

# slash followed by not # and not '.root'
DATASET_PATTERN_RELAXED = re.compile(r'^/[^#]+(?<!\.root)$')
