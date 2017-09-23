#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Description: maintains a list of the allowed input values for certain DAS keys
by querying certain web-services.
"""
from __future__ import print_function

# system modules
import re
import time
import json
import urllib
import urllib2
import itertools

# jsonpath
from jsonpath_rw import parse

# MongoDB modules
from pymongo.errors import InvalidOperation
from pymongo import ASCENDING

# DAS modules
from DAS.utils.utils import dastimestamp
from DAS.utils.das_db import get_db_uri, db_connection, create_indexes
from DAS.utils.utils import get_key_cert
from DAS.utils.url_utils import HTTPSClientAuthHandler
from DAS.utils.das_config import das_readconfig
from DAS.core.das_mapping_db import DASMapping

from DAS.keywordsearch.config import DEBUG


# Shall we keep existing records on server restart (very useful for debuging)
KEEP_EXISTING_RECORDS_ON_RESTART = 1
SKIP_UPDATES = 0
# if values matches fields' regexp but the field is stable, the KWS will ignore
# this match
STABLE_FIELDS = ['site.name', 'tier.name', 'datatype.name', 'status.name',
                 'group.name']


def get_collection_name(field_name):
    """ gets name of collection where to store data """
    return field_name.replace('.', '_')


class InputValuesTracker(object):
    """
    InputValuesTracker fetches a list of known input values and stores them
     in a separate collection to be used by keyword search and auto-completion.
    """

    def __init__(self, cfg):
        config = das_readconfig().get('inputvals', {})

        self.dburi = get_db_uri()
        self.dbcoll = get_collection_name(cfg['input'])
        self.dbname = config.get('dbname', config.get('DBNAME', 'inputvals'))

        self.cfg = cfg
        self.cache_size = config.get('cache_size', 1000)
        self.expire = config.get('expire', 3600)
        self.write_hash = config.get('write_hash', False)

        self.init()

    @property
    def col(self):
        """Return MongoDB collection object"""
        conn = db_connection(self.dburi)
        col = conn[self.dbname][self.dbcoll]
        return col

    def init(self):
        """
        Init db connection and check that it is alive
        """
        try:
            indexes = [('value', ASCENDING), ('ts', ASCENDING)]
            create_indexes(self.col, indexes)

            if not KEEP_EXISTING_RECORDS_ON_RESTART:
                self.col.remove()
        except Exception as exc:
            print(dastimestamp(), exc)

    def update(self):
        """
        Update some the input values collection for current input field
        """
        if SKIP_UPDATES:
            return None

        time0 = time.time()
        values = self.fetch_values()
        #print gen
        if not self.col.count():
            try:  # perform bulk insert operation
                self.col.insert(
                    itertools.islice(values, self.cache_size))
                #   break
            except InvalidOperation:
                pass
        else:  # we already have records, update their ts
            for val in values:
                spec = dict(value=val['value'])
                self.col.update(spec, {'$set': {'ts': time0}}, upsert=True)
                # remove records with old ts
        self.col.remove({'ts': {'$lt': time0 - self.expire}})
        print("%s InputValuesTracker updated" \
              " %s collection in %s sec, nrec=%s" \
              % (dastimestamp(), self.dbcoll, time.time() - time0,
                 self.col.count()))

    def find(self, pattern, idx=0, limit=10):
        """
        Find values for a given pattern. The idx/limit parameters
        control number of retrieved records (aka pagination). The
        limit=-1 means no pagination (get all records).
        """
        try:
            if len(pattern) > 0 and pattern[0] != '*' and pattern[0] != '^':
                pattern = '^%s' % pattern

            if pattern.find('*') != -1:
                pattern = pattern.replace('*', '.*')
            pat = re.compile('%s' % pattern, re.I)
            spec = {'value': pat}
            if limit == -1:
                for row in self.col.find(spec):
                    yield row['value']
            else:
                for row in self.col.find(spec).skip(idx).limit(limit):
                    yield row['value']
        except Exception:
            pass

    def fetch_values(self):
        """ fetch the data from providers and select the final values
         with jsonpath rules """
        # use grid-proxy for authentication
        ckey, cert = get_key_cert()

        handler = HTTPSClientAuthHandler(ckey, cert)
        opener = urllib2.build_opener(handler)
        urllib2.install_opener(opener)

        # request list of possible values
        params = {}
        encoded_data = urllib.urlencode(params, doseq=True)

        service = self.cfg
        url = service['url'] + encoded_data
        print(str(url))
        req = urllib2.Request(url)

        # ensure we get json (sitedb is messed up and randomly returns xml)
        if service['jsonpath_selector']:
            req.add_header('Accept', 'application/json')
            #print req.get_full_url()

        stream = urllib2.urlopen(req)

        if service['jsonpath_selector']:
            response = json.load(stream)
            jsonpath_expr = parse(service['jsonpath_selector'])
            results = jsonpath_expr.find(response)
            stream.close()

            return ({'value': v.value} for v in results)

        return []


TRACKERS = {}


def init_trackers():
    """ initialization """
    # get list of trackers
    mapping = DASMapping(config=das_readconfig())
    for provider in mapping.inputvalues_uris():
        TRACKERS[provider['input']] = InputValuesTracker(provider)


def get_fields_tracked(only_stable=False):
    """ list of fields available """
    if only_stable:
        return STABLE_FIELDS
    return list(TRACKERS.keys())


def get_tracker(field):
    """ get the instance of tracker of given field """
    return TRACKERS[field]


def need_value_bootstrap():
    """
    bootstrap is needed if some of fields were not loaded...
    """
    return any(not next(x.find('*'), False)
               for x in TRACKERS.values())


def check_unique_match(tracker, field, keyword):
    """
    returns a value if only one exists, otherwise a boolean value
    whether the keyword is matched by multiple values in a given field
    """
    matches_maybe = tracker.find(keyword, limit=2)
    match = next(matches_maybe, False)
    if match:
        match2 = next(matches_maybe, False)
        if match2:
            if DEBUG:
                print('non-unique match of %(keyword)s into %(match)s' \
                      ' and %(match2)s' % locals())
            return True, None
        else:
            # there's no second item -- it's unique
            if DEBUG:
                print('unique match of %(keyword)s into %(match)s' % locals())
            return True, match

    else:
        return False, None  # no match at all


def input_value_matches(kwd):
    """ return keyword matches to values """
    scores_by_entity = {}
    for field in get_fields_tracked():
        tracker = get_tracker(field)

        if '*' in kwd:
            if next(tracker.find(kwd, limit=2), False):
                scores_by_entity[field] = (0.7, {'map_to': field, })

            match, unique = check_unique_match(tracker, field, '^' + kwd + '$')
            if unique:
                scores_by_entity[field] = (0.8, {'map_to': field,
                                                 'adjusted_keyword': unique})

        else:
            # 1) check for exact-match (ignoring case)
            match = next(tracker.find('^' + kwd + '$', limit=2), False)
            # does it match case?
            if match:
                scores_by_entity[field] = (match == kwd and 1.0 or 0.95,
                                           {'map_to': field,
                                           'adjusted_keyword':  match})
            # 2) partial match
            elif len(kwd) >= 2:
                # wildcard on both sides
                match, unique = check_unique_match(tracker, field, '^*' + kwd + '*$')
                if match:
                    kwd_new = ('*' + kwd + '*').replace('**', '*')
                    if unique:
                        kwd_new = unique  # in unique match, modify kwd
                    scores_by_entity[field] = \
                        (0.6, {'map_to': field,
                               'adjusted_keyword': kwd_new})

    return scores_by_entity


def test(tracker):
    """ Test function """
    tracker.update()
    idx = 0
    limit = 10
    print('matching {0:s} in {1:s}:'.format(tracker.cfg['test'],
                                            tracker.cfg['input']))
    for row in tracker.find(tracker.cfg['test'], idx, limit):
        print(row)


def test_all():
    """ test all providers """
    init_trackers()
    for tracker in TRACKERS.values():
        test(tracker)


if __name__ == '__main__':
    test_all()
