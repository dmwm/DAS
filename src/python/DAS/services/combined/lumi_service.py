#!/usr/bin/python
#-*- coding: ISO-8859-1 -*-
"""
DAS DBS/Lumi combined service to get luminosity information
about datasets.
"""

# system modules
import cherrypy
import itertools

# DAS modules
from   DAS.utils.url_utils import getdata_urllib as getdata
from   DAS.web.tools import exposejson
from   DAS.web.utils import db_monitor
from   DAS.utils.utils import qlxml_parser
from   DAS.utils.thread import start_new_thread
from   DAS.utils.utils import get_key_cert
from   DAS.core.das_mapping_db import DASMapping
from   DAS.utils.das_config import das_readconfig
import DAS.utils.jsonwrapper as json

def lumilist(ilumi):
    """
    Convert input list into list of ranges.
    http://stackoverflow.com/questions/4628333/converting-a-list-of-integers-into-range-in-python
    """
    # right now just sort input list and return it
    ilumi.sort()
    res = [[t[0][1], t[-1][1]] for t in \
            (tuple(g[1]) for g in \
                itertools.groupby(enumerate(ilumi), lambda (i, x): i - x))]
    return res

def parse_run_dict(rdict):
    "Parser input run dict and normalize lumi lists"
    for key, val in rdict.items():
        rdict[key] = lumilist(val)

def run_lumis(url, dataset, ckey, cert):
    """
    Retrieve list of run/lumis from DBS for a given dataset
    """
    if  url.find('servlet') != -1: # DBS2 url
        res = run_lumis_dbs2(url, dataset, ckey, cert)
    elif url.find('cmsweb') != -1 and url.find('DBSReader') != -1:
        res = run_lumis_dbs3(url, dataset, ckey, cert)
    else:
        raise Exception('Unsupport DBS URL, url=%s' % url)
    parse_run_dict(res)
    return res

def run_lumis_dbs2(url, dataset, ckey, cert):
    "Retrive list of run/lumis from DBS2 for a given dataset"
    query    = "find run, lumi where dataset=%s" % dataset
    params   = dict(api='executeQuery', apiversion='DBS_2_0_9', query=query)
    data, _  = getdata(url, params, ckey=ckey, cert=cert, system='combined')
    prim_key = 'run'
    res = {} # output result
    for row in qlxml_parser(data, prim_key):
        run  = row['run']['run']
        lumi = row['run']['lumi']
        res.setdefault(run, []).append(lumi)
    return res

def run_lumis_dbs3(url, dataset, ckey, cert):
    "Retrive list of run/lumis from DBS2 for a given dataset"
    res      = {} # output result
    api_url  = url + '/blocks'
    params   = {'dataset': dataset}
    data, _  = getdata(api_url, params, ckey=ckey, cert=cert, system='combined')
    for row in json.load(data):
        api_url = url + '/filelumis'
        params = {'block_name': row['block_name']}
        data, _  = \
            getdata(api_url, params, ckey=ckey, cert=cert, system='combined')
        for rec in json.load(data):
            run  = rec['run_num']
            lumi = rec['lumi_section_num']
            res.setdefault(run, []).append(lumi)
    return res

class LumiService(object):
    """LumiService"""
    def __init__(self, config=None):
        super(LumiService, self).__init__()
        if  not config:
            config   = {}
        self.dasconfig = das_readconfig()
        self.service_name = config.get('name', 'combined')
        self.service_api  = config.get('api', 'combined_lumi4dataset')
        self.uri       = self.dasconfig['mongodb']['dburi']
        self.urls      = None # defined at run-time via self.init()
        self.expire    = None # defined at run-time via self.init()
        self.coll      = None # defined at run-time via self.init()
        self.ckey, self.cert = get_key_cert()
        self.init()

        # Monitoring thread which performs auto-reconnection
        thname = 'lumi_service'
        start_new_thread(thname, db_monitor, (self.uri, self.init, 5))

    def init(self):
        "Takes care of MongoDB connection since DASMapping requires it"
        try:
            dasmapping  = DASMapping(self.dasconfig)
            mapping     = dasmapping.servicemap(self.service_name)
            self.urls   = mapping[self.service_api]['services']
            self.expire = mapping[self.service_api]['expire']
        except Exception, _exp:
            self.coll = None
    @cherrypy.expose
    def index(self):
        "Default path"
        msg = 'LumiService, URLs=%s, expire=%s' % (self.urls, self.expire)
        return msg

    @exposejson
    def lumi(self, dataset):
        "Return luminosity info for a given dataset"
        res = run_lumis(self.urls['dbs'], dataset, self.ckey, self.cert)
        # call conddb to get int.lumi or run lumiCalc2.py script
        int_lumi = 1
        cherrypy.lib.caching.expires(secs=self.expire, force = True)
        data = {'lumi' : {'integrated': int_lumi, 'runlumis': res},
                'dataset': {'name': dataset}}
        return data
        
def test():
    """Test main function"""
    config = {'name': 'combined', 'api': 'combined_lumi4dataset'}
    cherrypy.quickstart(LumiService(config), '/')

if __name__ == '__main__':
    test()
