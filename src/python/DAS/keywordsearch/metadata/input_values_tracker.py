#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=R0913,W0703,W0702
"""
File: dbs_daemon.py
Author: Valentin Kuznetsov <vkuznet@gmail.com>
Description: DBS daemon, which update DAS cache with DBS datasets
"""

# system modules
import re
import time
import urllib
import urllib2
import itertools

# MongoDB modules
from pymongo.errors import InvalidOperation
from pymongo import ASCENDING

# DAS modules
import DAS.utils.jsonwrapper as json
from DAS.utils.utils import dastimestamp
from DAS.utils.das_db import db_connection, is_db_alive, create_indexes
from DAS.utils.das_db import db_monitor
from DAS.utils.utils import get_key_cert
from DAS.utils.thread import start_new_thread
from DAS.utils.url_utils import HTTPSClientAuthHandler


from jsonpath import jsonpath



# Shall we keep existing Datasets on server restart (very useful for debuging)
KEEP_EXISTING_RECORDS_ON_RESTART = 1
SKIP_UPDATES = 0


DBNAME = 'inputvals'
# TODO: We could even encapsulate DBS tracker here...

def fieldname(field):
    return field.replace('.', '_')

# TODO: are wildcards always allowed?
service_input_value_providers = [
    {'field': 'site.name',
     'url': 'https://cmsweb.cern.ch/sitedb/data/prod/site-names',
     'jsonpath': "$.result[*]..[2]",
     'test': 'T1*'},
    {'field': 'tier.name',
     'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatiers',
     'jsonpath': '$..data_tier_name',
     'test': '*RECO*' },
    {'field': 'datatype.name',
     'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatypes',
     'jsonpath': '$..data_type',
     'test': 'mc'},
    {'field': 'status.name',
     'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datasetaccesstypes',
     'jsonpath': "$[0]['dataset_access_type'][*]",
     'test':'valid'}, # TODO: is it case sensitive?!
    {'field': 'release.name',

    # https://cmsweb.cern.ch/dbs/prod/global/DBSReader/releaseversions
     'url': 'https://cmsweb-testbed.cern.ch/dbs/dev/global/DBSReader/releaseversions',
     'jsonpath': "$[0]['release_version'][*]",
     'test':'CMSSW_4_*'},

    # TODO: this also have potentially useful primary_ds_type': 'mc' and creation date
    # TODO: better to store in MongoDB?
    #'primary_dataset': {'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/primarydatasets',                            'jsonpath': '$..primary_ds_name' },
    {'field': 'group.name',
     'url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/physicsgroups',
     'jsonpath': '$..physics_group_name',
     'test':'analysis'},
]

stable_fields = ['site.name', 'tier.name', 'datatype.name', 'status.name', 'group.name']
# release?

class InputValuesTracker(object):
    """
    InputValuesTracker fetches a list of known input values and stores them
     in a separate collection to be used by keyword search and auto-completion.
    """

    def __init__(self, field_config, dburi, config=None):
        if  not config:
            config = {}
        self.dburi = dburi
        self.field_config = field_config
        self.dbcoll = fieldname(field_config['field'])
        #self.dbs_url    = dbs_url
        self.dbname = config.get('dbname', DBNAME)
        self.cache_size = config.get('cache_size', 1000)
        self.expire = config.get('expire', 3600)
        self.write_hash = config.get('write_hash', False)
        self.col = None # to be defined in self.init

        self.init()
        # Monitoring thread which performs auto-reconnection to MongoDB
        start_new_thread('inputval_monitor', db_monitor, (dburi, self.init))

    def init(self):
        """
        Init db connection and check that it is alive
        """
        try:
            conn = db_connection(self.dburi)
            self.col = conn[self.dbname][self.dbcoll]
            indexes = [('value', ASCENDING), ('ts', ASCENDING)]
            create_indexes(self.col, indexes)

            if not KEEP_EXISTING_RECORDS_ON_RESTART:
                self.col.remove()
        except Exception as _exp:
            self.col = None
        if  not is_db_alive(self.dburi):
            self.col = None

    def update(self):
        """
        Update DBS collection with a fresh copy of datasets
        """
        if SKIP_UPDATES:
            return None

        if  self.col:
            time0 = time.time()
            values = self.fetch_values()
            #print gen
            if  not self.col.count():
                try: # perform bulk insert operation
                   self.col.insert(
                       itertools.islice(values, self.cache_size))
                    #   break
                except InvalidOperation:
                    pass
            else: # we already have records, update their ts
                for val in values:
                    spec = dict(value=val['value'])
                    self.col.update(spec, {'$set': {'ts': time0}}, upsert=True)
                # remove records with old ts
            self.col.remove({'ts': {'$lt': time0 - self.expire}})
            print "%s InputValuesTracker updated %s collection in %s sec, nrec=%s"\
                  % (
            dastimestamp(), self.dbcoll, time.time() - time0, self.col.count())

    def find(self, pattern, idx=0, limit=10):
        """
        Find values for a given pattern. The idx/limit parameters
        control number of retrieved records (aka pagination). The
        limit=-1 means no pagination (get all records).
        """
        if  self.col:
            try:
                if  len(pattern) > 0 and pattern[0] != '*' and pattern[0] != '^':
                    pattern = '^%s' % pattern

                if  pattern.find('*') != -1:
                    pattern = pattern.replace('*', '.*')
                pat = re.compile('%s' % pattern, re.I)
                spec = {'value': pat}
                if  limit == -1:
                    for row in self.col.find(spec):
                        yield row['value']
                else:
                    for row in self.col.find(spec).skip(idx).limit(limit):
                        yield row['value']
            except:
                pass




    # for dataset (dataset listing + wildcard) and block (regexp) we have special classes
    # TODO: support wildcard for other values (site, )


    def fetch_values(self):
        # use grid-proxy for authentication
        ckey, cert = get_key_cert()

        handler = HTTPSClientAuthHandler(ckey, cert)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)

        # request list of possible values
        params = {}
        encoded_data = urllib.urlencode(params, doseq=True)

        service = self.field_config
        url =  service['url'] + encoded_data
        print str(url)
        req = urllib2.Request(url)

        # ensure we get json (sitedb is messed and randomly returns xml)
        if service['jsonpath']:
            req.add_header('Accept', 'application/json')
            #print req.get_full_url()



        stream = urllib2.urlopen(req)
        #stream = urllib2.urlopen(url)

        # parse the response, TODO: JSON only so far...

        # TODO:  sitedb2 is already tracked...

        if service['jsonpath']:
            response = json.load(stream)
            results = jsonpath(response, service['jsonpath'])
            stream.close()
            return [{'value': v} for v in results]
            #return results

        return []


trackers = {}

def init_trackers():
    dburi = 'mongodb://localhost:8230'

    for s in service_input_value_providers:
        trackers[s['field']] = InputValuesTracker(s, dburi)

# TODO: thread-unsafe!!!
init_trackers()

def get_fields_tracked(only_stable=False):
    if only_stable:
        return stable_fields

    return trackers.keys()

def get_tracker(field):
    return trackers[field]


def check_for_unique_match(t, field, keyword):
    """
    returns a value if only one exists, othwerwise a boolean value
    whether the keyword is matched by multiple values in a given field
    """
    it = t.find(keyword, limit=2)
    match = next(it, False)
    if match:
        match2 = next(it, False)
        if match2:
            print 'non-unique match of %(keyword)s into %(match)s and %(match2)s' % locals()
            return True, None
        else:
            # there's no second item -- it's unique
            print 'unique match of %(keyword)s into %(match)s' % locals()
            return True, match

    else:
        return False, None # no match at all



def input_value_matches(keyword):
    # TODO: support wildcards for site
    # TODO: store everything at mongoDB
    scores_by_entity = {}

    for field in get_fields_tracked():
        t = get_tracker(field)

        if '*' in keyword:
            if next(t.find(keyword, limit=2), False):
                scores_by_entity[field] = (0.7, {'map_to': field, })

            (match, unique) = check_for_unique_match(t, field,'^'+keyword+'$')
            if unique:
                scores_by_entity[field] = (0.8, {'map_to': field,
                                                 'adjusted_keyword': unique})

        else:
            # 1) check for exact-match (ignoring case)
            match = next(t.find('^'+keyword+'$', limit=2), False)
            # does it match case?
            if match:
                scores_by_entity[field] = (match == keyword
                                           and 1.0 or 0.95, {'map_to': field, 'adjusted_keyword': match})
            # 2) TODO: partial match if no wildcard?
            elif len(keyword) >= 2:
                # first try adding wildcard only to the end
                # (match, unique) = check_for_unique_match(t, field, '^'+keyword+'*$')
                #
                # if match:
                #     kwd_new = (keyword+'*').replace('**', '*')
                #
                #     # a unique match
                #     if unique:
                #         kwd_new = unique
                #
                #     scores_by_entity[field] = \
                #         (0.65, {'map_to': field,
                #                 'adjusted_keyword':  kwd_new})
                #
                # otherwise, try wildcard on both sides
                (match, unique) = check_for_unique_match(t, field,'^*'+keyword+'*$')

                if match:
                    kwd_new = ('*'+keyword+'*').replace('**', '*')

                    # a unique match
                    if unique:
                        kwd_new = unique

                    scores_by_entity[field] = \
                        (0.6, {'map_to': field,
                                'adjusted_keyword': kwd_new})



    return scores_by_entity




def test(service):
    """Test function"""
    dburi = 'mongodb://localhost:8230'

    mgr = InputValuesTracker(service, dburi)
    mgr.update()
    idx = 0
    limit = 10
    print 'trying %s on %s' % (service['test'], service['field'])
    for row in mgr.find(service['test'], idx, limit):
        print row


def test_all():
    for s in service_input_value_providers:
        test(s)


if __name__ == '__main__':
    #test(service_input_value_providers[0])
    test_all()
