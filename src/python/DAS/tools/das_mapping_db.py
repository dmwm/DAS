#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_mapping_db.py,v 1.7 2009/10/02 15:13:12 valya Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "Valentin Kuznetsov"

import os
import sys
from optparse import OptionParser
from DAS.core.das_core import DASCore
from DAS.utils.utils import dump
from DAS.core.das_mapping_db import DASMapping
from DAS.utils.logger import DASLogger

import sys
if sys.version_info < (2, 6):
    raise Exception("DAS requires python 2.6 or greater")

class DASOptionParser: 
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                                          type="int", default=None, 
                                          dest="debug",
             help="verbose output")
#        self.parser.add_option("--db", action="store", type="string", 
#             default='das_mapping.db', dest="dbfile",
#             help="specify DB file to use.")
        self.parser.add_option("--host", action="store", type="string",
             default="localhost", dest="host", help="specify MongoDB hostname")
        self.parser.add_option("--port", action="store", type="int",
             default=27017 , dest="port", help="specify MongoDB port number")
        self.parser.add_option("--db", action="store", type="string",
             default="mapping" , dest="db", help="specify MongoDB db name")
        self.parser.add_option("--system", action="store", type="string",
             default=None , dest="system", help="specify DAS sub-system")
        self.parser.add_option("--list-apis", action="store_true", 
             dest="listapis", help="return a list of APIs")
        self.parser.add_option("--list-daskeys", action="store_true", 
             dest="listkeys", help="return a list of DAS keys")
    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

def nooutput(results):
    """Just iterate over generator, but don't print it out"""
    for i in results:
        a = 1
#
# main
#
if __name__ == '__main__':
    optManager  = DASOptionParser()
    (opts, args) = optManager.getOpt()

    logger = DASLogger(verbose=opts.debug, stdout=opts.debug)
    config = dict(logger=logger, verbose=opts.debug,
        mapping_dbhost=opts.host, mapping_dbport=opts.port, mapping_dbname=opts.db)

    mgr = DASMapping(config)

    if  opts.listapis:
        apis = mgr.list_apis(opts.system)
        print apis
        sys.exit(0)

    if  opts.listkeys:
        keys = mgr.list_daskeys(opts.system)
        print keys
        sys.exit(0)
    # daskeys defines a mapping between daskey used in DAS-QL and DAS record key
    # e.g. block means block.name, the pattern defines regular expression to which
    # daskey should satisfy
    #
    # api2das defines mapping between API input parameter and DAS-QL key,
    # dict(api_param='storage_element_name', das_key='site', 
    #      pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
    # here pattern is applied to passed api_param

    # clean-up existing Mapping DB
    mgr.delete_db()
    mgr.create_db()

    ##### DBS
    apiversion = 'DBS_2_0_8'
    system = 'dbs'

    # listDatasetPaths
    api = 'listDatasetPaths'
    params = {'apiversion':apiversion}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='dataset', map='dataset.name', pattern='')],
        'api2das' : []
    }
    mgr.add(rec)

    # listAlgorithms
    api = 'listAlgorithms'
    params = {'apiversion':apiversion,
              'app_version': '*', 'app_family_name': '*', 
              'app_executable_name': '*', 'ps_hash':'*'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='algo', map='algo.name', pattern='')],
        'api2das' : [dict(api_param='app_executable_name', das_key='exe', pattern='')]
    }
    mgr.add(rec)

    # listPrimaryDatasets API
    api = 'listPrimaryDatasets' 
    params = {'apiversion':apiversion, 'pattern':'*'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='primds', map='primary_dataset.name', pattern='')],
        'api2das' : [
                dict(api_param='pattern', das_key='primds', pattern=""),
        ]
    }
    mgr.add(rec)

    # listBlocks API
    api = 'listBlocks'
    params = {'apiversion':apiversion,
              'block_name':'*', 'storage_element_name':'*',
              'user_type':'NORMAL'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='block', map='block.name', pattern='')],
        'api2das' : [
                dict(api_param='block_name', das_key='block', pattern=''),
                dict(api_param='storage_element_name', das_key='site', 
                        pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
        ]
    }
    mgr.add(rec)

    # listFiles API
    api = 'listFiles'
    params = {'apiversion':apiversion,
              'path' : '',
              'primary_dataset' : '',
              'processed_dataset' : '',
              'data_tier_list' : '',
              'analysis_dataset_name' : '',
              'block_name' : '',
              'other_detail' : 'True',
              'run_number' : '',
              'pattern_lfn' : '',
              'detail' : 'True',
              'retrive_list' : ''}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='file', map='file.name', pattern='')],
        'api2das' : [
                dict(api_param='path', das_key='dataset', pattern=''),
                dict(api_param='block_name', das_key='block', pattern=""),
                dict(api_param='run_number', das_key='run', pattern=""),
                dict(api_param='pattern_lfn', das_key='file', pattern=""),
        ]
    }
    mgr.add(rec)

    # listFileLumis
#    api = 'listFileLumis'
#    params = {'apiversion':apiversion, 'lfn': 'required'}
#    daskeys = dict(file='file.name')
#    api2das = [('lfn', 'file', ''), ('lfn', 'file.name', '')]
#    mgr.add_api(system, api, params, daskeys, api2das)
    # listProcessedDatasets API
    api = 'listProcessedDatasets' 
    params = {'apiversion':apiversion,
              'primary_datatset_name_pattern' : '*',
              'data_tier_name_pattern' : '*',
              'processed_datatset_name_pattern' : '*',
              'app_version' : '*',
              'app_family_name' : '*',
              'app_executable_name' : '*',
              'ps_hash' : '*'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='procds', map='processed_dataset.name', pattern='')],
        'api2das' : [
                dict(api_param='primary_datatset_name_pattern', das_key='primds', pattern=""),
                dict(api_param='processed_datatset_name_pattern', das_key='procds', pattern=""),
                dict(api_param='app_executable_name', das_key='exe', pattern=""),
        ]
    }
    mgr.add(rec)

    # listDatasetSummary API
    api = 'listDatasetSummary'
    params = {'apiversion':apiversion, 'path': 'required'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='dataset', map='dataset.summary', pattern='')],
        'api2das' : [
                dict(api_param='path', das_key='dataset', pattern=""),
        ]
    }
    mgr.add(rec)


    # listRuns API
    api = 'listRuns'
    params = { 'apiversion':apiversion, 'path' : 'required'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='run', map='run.run_number', pattern='')],
        'api2das' : [
                dict(api_param='path', das_key='dataset', pattern=""),
        ]
    }
    mgr.add(rec)

    # insert DAS notations for DBS apis
    notations = [
        dict(api_param='creation_date', das_name='creation_time'),
        dict(api_param='last_modification_date', das_name='modification_time'),
        dict(api_param='app_family_name', das_name='name'),
        dict(api_param='app_executable_name', das_name='executable'),
        dict(api_param='path', das_name='dataset'),
        dict(api_param='storage_element', das_name='se'),
        dict(api_param='storage_element_name', das_name='se'),
        dict(api_param='number_of_files', das_name='nfiles'),
        dict(api_param='number_of_events', das_name='nevents'),
        dict(api_param='number_of_lumi_sections', das_name='nlumis'),
        dict(api_param='total_luminosity', das_name='totlumi'),
        dict(api_param='lfn', das_name='name'),
    ]
    mgr.add(dict(system=system, notations=notations))

#    print "DBS map"
#    print mgr.servicemap(system, implementation='javaservlet')
#    print "daskeys", mgr.list_daskeys(system)
#    print "api2das, block_name", mgr.api2das(system, 'block_name')
#    print "das2api, block", mgr.das2api(system, 'block')
#    print "notation2das", mgr.notation2das(system, 'last_modification_date')
    ##### END OF DBS

    # Phedex
    system = 'phedex'
    # blockReplicas API
    api = 'blockReplicas'
    params = {'se':'*', 'block':'*', 'node':'*'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='block', map='block.name', pattern='')],
        'api2das' : [
                dict(api_param='node', das_key='site', pattern="re.compile('^T[0-3]_')"),
                dict(api_param='node', das_key='site.name', pattern="re.compile('^T[0-3]_')"),
                dict(api_param='se', das_key='site', pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
                dict(api_param='se', das_key='site.se', pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
                dict(api_param='block', das_key='block', pattern=""),
        ]
    }
    mgr.add(rec)

    # fileReplicas API
    api = 'fileReplicas'
    params = {'se':'*', 'block':'required', 'node':'*'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='file', map='file.name', pattern='')],
        'api2das' : [
                dict(api_param='node', das_key='site', pattern="re.compile('^T[0-3]_')"),
                dict(api_param='node', das_key='site.name', pattern="re.compile('^T[0-3]_')"),
                dict(api_param='se', das_key='site', pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
                dict(api_param='se', das_key='site.se', pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
        ]
    }
    mgr.add(rec)

    # nodes API
    api = 'nodes'
    params = {'node':'*', 'noempty':''}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='site', map='site.name', pattern="re.compile('^T[0-3]_')")],
        'api2das' : [
                dict(api_param='node', das_key='site', pattern="re.compile('^T[0-3]_')"),
                dict(api_param='node', das_key='site.name', pattern="re.compile('^T[0-3]_')"),
        ]
    }
    mgr.add(rec)

    # lfn2pfn API
    api = 'lfn2pfn'
    params = {'node':'required', 'lfn':'required', 'destination':'', 'protocol':'srmv2'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='file', map='file.name', pattern='')],
        'api2das' : [
                dict(api_param='node', das_key='site', pattern="re.compile('^T[0-3]_')"),
                dict(api_param='node', das_key='site.name', pattern="re.compile('^T[0-3]_')"),
                dict(api_param='lfn', das_key='file', pattern=""),
        ]
    }
    mgr.add(rec)

    notations = [
        dict(api_param='time_create', das_name='creation_time'),
        dict(api_param='time_update', das_name='modification_time'),
        dict(api_param='bytes', das_name='size'),
        dict(api_param='node', das_name='site'),
        dict(api_param='files', das_name='nfiles'),
        dict(api_param='events', das_name='nevents'),
        dict(api_param='lfn', das_name='name'),
        dict(api_param='node', das_name='site'),
    ]
    mgr.add(dict(system=system, notations=notations))
    ##### END OF PHEDEX

    # SiteDB
    system = 'sitedb'
    api = 'CMSNametoAdmins'
    params = {'name':''}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='admin', map='email', pattern='')],
        'api2das' : [
                dict(api_param='name', das_key='admin', pattern=""),
        ]
    }
    mgr.add(rec)

    api = 'SEtoCMSName'
    params = {'name':''}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='site', map='site.se', pattern="re.compile('([a-zA-Z0-9]+\.){2}')")],
        'api2das' : [
                dict(api_param='name', das_key='site', pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
        ]
    }
    mgr.add(rec)

    api = 'CMStoSAMName'
    params = {'name':''}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='site', map='site.name', pattern="re.compile('^T[0-3]_')")],
        'api2das' : [
                dict(api_param='name', das_key='site', pattern="re.compile('^T[0-3]_')"),
        ]
    }
    mgr.add(rec)

    api = 'CMStoSiteName'
    params = {'name':''}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='site', map='site.name', pattern="re.compile('^T[0-3]_')")],
        'api2das' : [
                dict(api_param='name', das_key='site', pattern="re.compile('^T[0-3]_')"),
        ]
    }
    mgr.add(rec)

    api = 'CMSNametoCE'
    params = {'name':'required'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='site', map='site.name', pattern="re.compile('^T[0-3]_')")],
        'api2das' : [
                dict(api_param='name', das_key='site', pattern="re.compile('^T[0-3]_')"),
        ]
    }
    mgr.add(rec)

    api = 'CMSNametoSE'
    params = {'name':''}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='site', map='site.name', pattern="re.compile('^T[0-3]_')")],
        'api2das' : [
                dict(api_param='name', das_key='site', pattern="re.compile('^T[0-3]_')"),
        ]
    }
    mgr.add(rec)

#    api = 'SiteStatus'
#    params = {'cms_name':'required'}
#    daskeys = dict(site='site.name')
#    api2das = [('cms_name', 'site', "re.compile('^T[0-3]_')")]
#    mgr.add_api(system, api, params, daskeys, api2das)

    notations=[dict(api_param='cmsname', das_name='name')]
    mgr.add(dict(system=system, notations=notations))
    ##### END OF SITEDB

    # DQ
    system = 'dq'
    api = 'listRuns4DQ'
    params = {'api': 'listRuns4DQ', 'DQFlagList': 'list', 'dataset': 'string'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='dq', map='dq', pattern='')],
        'api2das' : [
                dict(api_param='DQFlagList', das_key='dq', pattern=""),
        ]
    }
    mgr.add(rec)
    
    api = 'listSubSystems'
    params = {'api': 'listSubSystems'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='dqsystems', map='dqsystems', pattern='')],
        'api2das' : []
    }
    mgr.add(rec)

    ##### END OF DQ

    # RunSummary
    system = 'runsum'
    api = 'runsum'
    params  = {'DB':'cms_omds_lb', 'FORMAT':'XML', 'RUN':'required'}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='run', map='run.run_number', pattern='')],
        'api2das' : [
                dict(api_param='RUN', das_key='run', pattern="re.compile('[1-9][0-9]{4,5}')"),
        ]
    }
    mgr.add(rec)

    notations = [
        dict(api_param='bField', das_name='bfield'),
        dict(api_param='hltKey', das_name='hlt'),
        dict(api_param='runNumber', das_name='run_number'),
    ]
    mgr.add(dict(system=system, notations=notations))

    ##### END OF RunSummary

    # Dashboard
    system = 'dashboard'
    api = 'jobsummary-plot-or-table'
    params = {
        'user': '',
        'site': '',
        'ce': '',
        'submissiontool': '',
        'dataset': '',
        'application': '',
        'rb': '',
        'activity': '',
        'grid': '',
        'date1': '',
        'date2': '',
        'jobtype': '',
        'tier': '',
        'check': 'submitted',
    }
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [
            dict(key='jobsummary', map='jobsummary', pattern=''),
#            dict(key='site', map='jobsummary.ce', pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
#            dict(key='site', map='jobsummary.site', pattern="re.compile('^T[0-3]_')"),
        ],
        'api2das' : [
            dict(api_param='date1', das_key='date', pattern=""),
            dict(api_param='date2', das_key='date', pattern=""),
            dict(api_param='ce', das_key='site', pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
            dict(api_param='ce', das_key='site.ce', pattern="re.compile('([a-zA-Z0-9]+\.){2}')"),
            dict(api_param='site', das_key='site', pattern="re.compile('^T[0-3]_')"),
            dict(api_param='site', das_key='site.name', pattern="re.compile('^T[0-3]_')"),
        ]
    }
    mgr.add(rec)

    ##### END OF Dashboard

    # lumidb
    system = 'lumidb'
    api = 'findTrgPathByRun'
    params = {'run_number':''}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='trgpath', map='trigpath', pattern='')],
        'api2das' : [
                dict(api_param='run_number', das_key='run', pattern=""),
        ]
    }
    mgr.add(rec)

    api = 'findIntegratedLuminosity'
    params = {'run_number':'', 'tag':'', 'hlt_path':''}
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [dict(key='intlumi', map='intlumi', pattern='')],
        'api2das' : [
                dict(api_param='run_number', das_key='run', pattern=""),
                dict(api_param='hlt_path', das_key='trigpath', pattern=""),
        ]
    }
    mgr.add(rec)

    notations = [
        dict(api_param='run_number', das_name='run'),
        dict(api_param='int_lumi', das_name='intlumi'),
        dict(api_param='hlt_path', das_name='trigpath'),
    ]
    mgr.add(dict(system=system, notations=notations))

    ##### END OF lumidb

    # monitor
    system = 'monitor'
    api = '/plotfairy/phedex/prod/link-rate:plot'
    params = {
        'session': 'kkk777',
        'version': '1224790775',
        'span': 'hour',
        'start': '',
        'end': '1224792000',
        'qty': 'destination',
        'grouping': 'node',
        'src-grouping': 'same',
        'links': 'nomss',
        'from': '',
        'to': '',
        'type': 'json',
    }
    rec = {'system' : system, 
        'api' : dict(name=api, params=params),
        'daskeys' : [
                dict(key='monitor', map='monitor', pattern=''),
#                dict(key='monitor.key', map='monitor', pattern=''),
#                dict(key='monitor.country', map='monitor', pattern=''),
#                dict(key='monitor.node', map='monitor', pattern=''),
#                dict(key='monitor.region', map='monitor', pattern=''),
#                dict(key='monitor.tier', map='monitor', pattern=''),
        ],
        'api2das' : [
                dict(api_param='start', das_key='date', pattern=""),
                dict(api_param='end', das_key='date', pattern=""),
        ]
    }
    mgr.add(rec)
    ##### END OF monitor

    ##### Add web UI mapping
    rec = {'presentation': {
                'block': [
                        {'das':'block.name', 'ui':'BlockName'}, 
                        {'das':'block.size', 'ui':'BlockSize'},
                         ],
                'site': [
                        {'das':'site.name', 'ui':'CMSName'}, 
                        {'das':'site.se', 'ui':'StorageElement'}, 
                        {'das':'site.samname', 'ui':'SAMName'}, 
                        {'das':'site.sitename', 'ui':'Site'}
                        ]}
          }
    mgr.add(rec)
    ##### End of web UI mapping
    print "New DAS Mapping DB has been created"
