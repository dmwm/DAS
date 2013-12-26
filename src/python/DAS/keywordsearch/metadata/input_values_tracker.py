#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Description: maintains a list of the allowed input values for certain DAS keys
by querying certain web-services.
"""

# system modules
import re
import time
import urllib
import urllib2
import itertools

# jsonpath
from jsonpath_rw import parse

# MongoDB modules
from pymongo.errors import InvalidOperation
from pymongo import ASCENDING

# DAS modules
from DAS.utils import jsonwrapper as json
from DAS.utils.utils import dastimestamp
from DAS.utils.das_db import get_db_uri, db_connection, create_indexes
from DAS.utils.utils import get_key_cert
from DAS.utils.thread import start_new_thread
from DAS.utils.url_utils import HTTPSClientAuthHandler
from DAS.utils.das_config import das_readconfig

from DAS.keywordsearch.config import DEBUG


# Shall we keep existing Datasets on server restart (very useful for debuging)
KEEP_EXISTING_RECORDS_ON_RESTART = 1
SKIP_UPDATES = 0
VALUES_PROVIDERS = [
    {'field': 'site.name',
     'url': 'https://cmsweb.cern.ch/sitedb/data/prod/site-names',
     'jsonpath': "$.result[*][2]",
     'test': 'T1*'},
    {'field': 'tier.name',
     'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatiers',
     'jsonpath': '$[*].data_tier_name',
     'test': '*RECO*'},
    {'field': 'datatype.name',
     'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatypes',
     'jsonpath': '$[*].data_type',
     'test': 'mc'},
    {'field': 'status.name',
     'url':
        'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datasetaccesstypes',
     'jsonpath': "$[0]['dataset_access_type'][*]",
     'test': 'valid'},
    {'field': 'release.name',
     # DEV URL was:
     # https://cmsweb-testbed.cern.ch/dbs/dev/global/DBSReader/releaseversions
     'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/releaseversions',
     'jsonpath': "$[0]['release_version'][*]",
     'test': 'CMSSW_4_*'},

    {'field': 'primary_dataset.name',
     'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/primarydatasets',
     'jsonpath': '$[*].primary_ds_name',
     'test': 'RelVal160pre4Z-TT'},

    {'field': 'group.name',
     'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/physicsgroups',
     'jsonpath': '$[*].physics_group_name',
     'test': 'analysis'},
]

STABLE_FIELDS = ['site.name', 'tier.name', 'datatype.name', 'status.name',
                 'group.name']


def fieldname(field):
    """ gets name of collection where to store data """
    return field.replace('.', '_')


class InputValuesTracker(object):
    """
    InputValuesTracker fetches a list of known input values and stores them
     in a separate collection to be used by keyword search and auto-completion.
    """

    def __init__(self, field_data):
        config = das_readconfig().get('inputvals', {})

        self.dburi = get_db_uri()
        self.dbcoll = fieldname(field_data['field'])
        self.dbname = config.get('dbname', config.get('DBNAME', 'inputvals'))

        self.field_data = field_data
        self.cache_size = config.get('cache_size', 1000)
        self.expire = config.get('expire', 3600)
        self.write_hash = config.get('write_hash', False)

        self.init()

    @property
    def col(self):
        "Return MongoDB collection object"
        conn = db_connection(self.dburi)
        dbc  = conn[self.dbname]
        col  = dbc[self.dbcoll]
        return col

    def init(self):
        """
        Init db connection and check that it is alive
        """
        try:
            conn = db_connection(self.dburi)
            indexes = [('value', ASCENDING), ('ts', ASCENDING)]
            create_indexes(self.col, indexes)

            if not KEEP_EXISTING_RECORDS_ON_RESTART:
                self.col.remove()
        except Exception as exc:
            print dastimestamp(), exc

    def update(self):
        """
        Update DBS collection with a fresh copy of datasets
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
        print "%s InputValuesTracker updated" \
              " %s collection in %s sec, nrec=%s" \
              % (dastimestamp(), self.dbcoll, time.time() - time0,
                 self.col.count())

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
        except:
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

        service = self.field_data
        url = service['url'] + encoded_data
        print str(url)
        req = urllib2.Request(url)

        # ensure we get json (sitedb is messed and randomly returns xml)
        if service['jsonpath']:
            req.add_header('Accept', 'application/json')
            #print req.get_full_url()

        stream = urllib2.urlopen(req)

        if service['jsonpath']:
            response = json.load(stream)
            jsonpath_expr = parse(service['jsonpath'])
            results = jsonpath_expr.find(response)
            stream.close()

            return ({'value': v.value} for v in results)

        return []


TRACKERS = {}


def init_trackers():
    """ initialization """
    for provider in VALUES_PROVIDERS:
        TRACKERS[provider['field']] = InputValuesTracker(provider)


def get_fields_tracked(only_stable=False):
    """ list of fields available """
    if only_stable:
        return STABLE_FIELDS
    return TRACKERS.keys()


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
                print 'non-unique match of %(keyword)s into %(match)s' \
                      ' and %(match2)s' % locals()
            return True, None
        else:
            # there's no second item -- it's unique
            if DEBUG:
                print 'unique match of %(keyword)s into %(match)s' % locals()
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


def test(service):
    """ Test function """
    mgr = InputValuesTracker(service)
    mgr.update()
    idx = 0
    limit = 10
    print 'trying %s on %s' % (service['test'], service['field'])
    for row in mgr.find(service['test'], idx, limit):
        print row


def test_all():
    """ test all providers """
    init_trackers()
    for provider in VALUES_PROVIDERS:
        test(provider)


if __name__ == '__main__':
    test_all()
