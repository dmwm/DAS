#!/usr/bin/python
#-*- coding: ISO-8859-1 -*-
"""
DAS DBS/RunRegistry combined service to get luminosity information
about datasets.
"""
from __future__ import print_function

# DAS modules
from   DAS.utils.url_utils import getdata_urllib as getdata
from   DAS.utils.utils import qlxml_parser
from   DAS.utils.utils import get_key_cert
from   DAS.services.runregistry.runregistry_service \
        import collect_lumis, rr_worker
import DAS.utils.jsonwrapper as json

def rr_query(rlist):
    "Generate RunRegistry query for given runlist/generator"
#    query = '>= %s and <= %s' % (min(rlist), max(rlist))
    query = ' or '.join([str(r) for r in rlist])
    return dict(runNumber=query)

def lumis4dataset(kwds):
    "Gen lumi info from DBS and RunRegistry data-services"
    # first check if we get all arguments in a valid form
    must_have = ['dbs_url', 'rr_url', 'ckey', 'cert', 'dataset']
    for key in must_have:
        val = kwds.get(key, None)
        if  not val:
            raise Exception('Missing value for key=%s' % key)
        if  not isinstance(val, basestring):
            raise TypeError('Invalid type, key=%s, val=%s, type=%s' \
                    % (key, val, str(type(val))))
    dbs_url = kwds.get('dbs_url', None)
    rr_url  = kwds.get('rr_url', None)
    ckey    = kwds.get('ckey', None)
    cert    = kwds.get('cert', None)
    dataset = kwds.get('dataset', None)
    gen = runs_dbs(dbs_url, dataset, ckey, cert)
    rlist = [r for r in gen]
    if  not rlist:
        return
    query = rr_query(rlist)
    gen   = rr_worker(rr_url, query, 'runlumis')
    for row in collect_lumis(gen):
        yield row

def runs_dbs(url, dataset, ckey, cert):
    "Retrive list of run/lumis from DBS2 for a given dataset"
    api_url  = url + '/runs'
    params   = {'dataset': dataset}
    data, _  = getdata(api_url, params, ckey=ckey, cert=cert, system='combined')
    for row in json.load(data):
        run  = row['run']['run_num']
        yield run

def test():
    """Test main function"""
    dbs_url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
    dbs_url = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
    rr_url = 'http://localhost:8081/runregistry'
    ckey, cert = get_key_cert()
    dataset = '/DoubleElectron/Run2012A-13Jul2012-v1/AOD'
    kwds = dict(dbs_url=dbs_url, rr_url=rr_url, ckey=ckey, cert=cert,
            dataset=dataset)
    for row in lumis4dataset(kwds):
        print(row)

if __name__ == '__main__':
    test()
