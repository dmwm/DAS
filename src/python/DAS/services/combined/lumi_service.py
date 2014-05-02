#!/usr/bin/python
#-*- coding: ISO-8859-1 -*-
"""
DAS DBS/Lumi combined service to get luminosity information
about datasets.
"""

# system modules
import cherrypy

# DAS modules
from   DAS.utils.url_utils import getdata_urllib as getdata
from   DAS.web.tools import exposejson
from   DAS.utils.utils import qlxml_parser, convert2ranges
from   DAS.utils.utils import get_key_cert
from   DAS.core.das_mapping_db import DASMapping
from   DAS.utils.das_config import das_readconfig
import DAS.utils.jsonwrapper as json

def parse_run_dict(rdict):
    "Parser input run dict and normalize lumi lists"
    for key, val in rdict.items():
        rdict[key] = convert2ranges(val)

def run_lumis(url, dataset, ckey, cert):
    """
    Retrieve list of run/lumis from DBS for a given dataset
    """
    res = run_lumis_dbs(url, dataset, ckey, cert)
    parse_run_dict(res)
    return res

def run_lumis_dbs(url, dataset, ckey, cert):
    "Retrive list of run/lumis from DBS for a given dataset"
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
        self.ckey, self.cert = get_key_cert()
        self.init()

    def init(self):
        "Takes care of MongoDB connection since DASMapping requires it"
        try:
            dasmapping  = DASMapping(self.dasconfig)
            mapping     = dasmapping.servicemap(self.service_name)
            self.urls   = mapping[self.service_api]['services']
            self.expire = mapping[self.service_api]['expire']
        except Exception, _exp:
            pass

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
