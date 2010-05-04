#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_mapping_db.py,v 1.1 2009/09/01 01:42:47 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import sys
from optparse import OptionParser
from DAS.core.das_core import DASCore
from DAS.utils.utils import dump
from DAS.core.das_mapping import DASMapping
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
        self.parser.add_option("--db", action="store", type="string", 
             default='das_mapping.db', dest="dbfile",
             help="specify DB file to use.")
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
        mapping_db_engine= 'sqlite:///%s' % opts.dbfile)
    mgr = DASMapping(config)

    if  opts.listapis:
        apis = mgr.list_apis(opts.system)
        print apis
        sys.exit(0)

    if  opts.listkeys:
        keys = mgr.list_daskeys(opts.system)
        print keys
        sys.exit(0)

    # add DAS systems
    systems = ['dbs', 'phedex', 'sitedb', 'dq', 
                'runsum', 'dashboard', 'lumidb', 'monitor']
    for system in systems:
        try:
            mgr.add_system(system)
        except:
            pass

    # add DAS keys which are not part of any API
    mgr.add_daskey('date')
    mgr.add_daskey('system')

    # add APIs
    ##### DBS
    apiversion = 'DBS_2_0_8'
    system = 'dbs'
    # listDatasetPaths
    api = 'listDatasetPaths'
    params = {'apiversion':apiversion}
    daskeys = dict(dataset='dataset.name')
    api2das = []
    mgr.add_api(system, api, params, daskeys, api2das)
    # listAlgorithms
    api = 'listAlgorithms'
    params = {'apiversion':apiversion,
              'app_version': '*', 'app_family_name': '*', 
              'app_executable_name': '*', 'ps_hash':'*'}
    daskeys = dict(algo='algo.name')
    api2das = [('app_executable_name', 'exe', ''),
              ]
    mgr.add_api(system, api, params, daskeys, api2das)

    # listPrimaryDatasets API
    api = 'listPrimaryDatasets' 
    params = {'apiversion':apiversion, 'pattern':'*'}
    daskeys = dict(primary_dataset='primary_dataset.name')
    api2das = [('pattern', 'primary_dataset', ''),
               ('pattern', 'primary_dataset.name', ''),
              ]
    mgr.add_api(system, api, params, daskeys, api2das)
    # listBlocks API
    api = 'listBlocks'
    params = {'apiversion':apiversion,
              'block_name':'*', 'storage_element_name':'*',
              'user_type':'NORMAL'}
    daskeys = dict(block='block.name')
    api2das = [('block_name', 'block', ''),
               ('block_name', 'block.name', ''),
               ('storage_element_name', 'site', "re.compile('([a-zA-Z0-9]+\.){2}')"),
               ('storage_element_name', 'site.se', "re.compile('([a-zA-Z0-9]+\.){2}')"),
              ]
    mgr.add_api(system, api, params, daskeys, api2das)
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
    daskeys = dict(file='file.name')
    api2das = [('path', 'dataset', ''),
               ('path', 'dataset.name', ''),
               ('block_name', 'block.name', ''),
               ('run_number', 'run', ''),
               ('pattern_lfn', 'file', ''),
              ]
    mgr.add_api(system, api, params, daskeys, api2das)
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
    daskeys = dict(processed_dataset='processed_dataset.name')
    api2das = [('primary_datatset_name_pattern', 'primary_datatset', ''),
               ('primary_datatset_name_pattern', 'primary_datatset.name', ''),
               ('processed_datatset_name_pattern', 'processed_datatset', ''),
               ('processed_datatset_name_pattern', 'processed_datatset.name', ''),
               ('app_executable_name', 'exe', ''),
               ('app_executable_name', 'exe.name', ''),
              ]
    mgr.add_api(system, api, params, daskeys, api2das)
    # listDatasetSummary API
    api = 'listDatasetSummary'
    params = {'apiversion':apiversion, 'path': 'required'}
    daskeys = dict(dataset='dataset.summary')
    api2das = [('path', 'dataset', ''),
               ('path', 'dataset.name', ''),
              ]
    mgr.add_api(system, api, params, daskeys, api2das)

    # insert DAS notations for DBS apis
    mgr.add_notation(system, 'creation_date', 'creation_time')
    mgr.add_notation(system, 'last_modification_date', 'modification_time')
    mgr.add_notation(system, 'app_family_name', 'name')
    mgr.add_notation(system, 'app_executable_name', 'executable')
    mgr.add_notation(system, 'path', 'dataset')
    mgr.add_notation(system, 'storage_element', 'se')
    mgr.add_notation(system, 'storage_element_name', 'se')
    mgr.add_notation(system, 'number_of_files', 'nfiles')
    mgr.add_notation(system, 'number_of_events', 'nevents')
#    mgr.add_notation(system, 'block_name', 'block.name')
    mgr.add_notation(system, 'lfn', 'name')

#    print "DBS map"
#    print mgr.servicemap(system, implementation='javaservlet')
#    print "daskeys", mgr.list_daskeys(system)
#    print "primary key for block", mgr.primary_key(system, 'block')
#    print "api2das, block_name", mgr.api2das(system, 'block_name')
#    print "das2api, block", mgr.das2api(system, 'block')
#    print "notation2das", mgr.notation2das(system, 'last_modification_date')
    ##### END OF DBS

    # Phedex
    system = 'phedex'
    # blockReplicas API
    api = 'blockReplicas'
    params = {'se':'*', 'block':'*', 'node':'*'}
    daskeys = dict(block='block.name')
    api2das = [('se', 'site', "re.compile('^T[0-3]_')"),
               ('node', 'site', "re.compile('([a-zA-Z0-9]+\.){2}')"),
               ('block', 'block', ''),
              ]
    mgr.add_api(system, api, params, daskeys, api2das)
    # fileReplicas API
    api = 'fileReplicas'
    params = {'se':'*', 'block':'required', 'node':'*'}
    daskeys = dict(file='file.name')
    api2das = [('se', 'site', "re.compile('^T[0-3]_')"),
               ('node', 'site', "re.compile('([a-zA-Z0-9]+\.){2}')"),
              ]
    mgr.add_api(system, api, params, daskeys, api2das)
    # nodes API
    api = 'nodes'
    params = {'node':'*', 'noempty':''}
    daskeys = dict(site='site.name', node='site.name')
    api2das = [('node', 'site', "re.compile('([a-zA-Z0-9]+\.){2}')")]
    mgr.add_api(system, api, params, daskeys, api2das)
    # lfn2pfn API
    api = 'lfn2pfn'
    params = {'node':'required', 'lfn':'required', 'destination':'', 'protocol':'srmv2'}
    daskeys = dict(file='file.name')
    api2das = [('node', 'site', "re.compile('([a-zA-Z0-9]+\.){2}')"),
               ('lfn', 'file', '')]
    mgr.add_api(system, api, params, daskeys, api2das)
    
    mgr.add_notation(system, 'time_create', 'creation_time')
    mgr.add_notation(system, 'time_update', 'modification_time')
    mgr.add_notation(system, 'bytes', 'size')
    mgr.add_notation(system, 'node', 'site')
    mgr.add_notation(system, 'files', 'nfiles')
    mgr.add_notation(system, 'events', 'nevents')
    mgr.add_notation(system, 'lfn', 'name')
    ##### END OF PHEDEX

    # SiteDB
    system = 'sitedb'
    api = 'CMSNametoAdmins'
    params = {'name':''}
    daskeys = dict(admin='email')
    api2das = [('name', 'admin', '')]
    mgr.add_api(system, api, params, daskeys, api2das)

    api = 'SEtoCMSName'
    params = {'name':''}
    daskeys = dict(site='site.name')
    api2das = [('name', 'site', "re.compile('([a-zA-Z0-9]+\.){2}')")]
    mgr.add_api(system, api, params, daskeys, api2das)

    api = 'CMStoSAMName'
    params = {'name':''}
    daskeys = dict(site='site.name')
    api2das = [('name', 'site', "re.compile('^T[0-3]_')")]
    mgr.add_api(system, api, params, daskeys, api2das)

    api = 'CMStoSiteName'
    params = {'name':''}
    daskeys = dict(site='site.name')
    api2das = [('name', 'site', "re.compile('^T[0-3]_')")]
    mgr.add_api(system, api, params, daskeys, api2das)

    api = 'CMSNametoCE'
    params = {'name':'required'}
    daskeys = dict(site='site.name')
    api2das = [('name', 'site', "re.compile('([a-zA-Z0-9]+\.){2}')")]
    mgr.add_api(system, api, params, daskeys, api2das)

    api = 'CMSNametoSE'
    params = {'name':''}
    daskeys = dict(site='site.name')
    api2das = [('name', 'site', "re.compile('^T[0-3]_')")]
    mgr.add_api(system, api, params, daskeys, api2das)

#    api = 'SiteStatus'
#    params = {'cms_name':'required'}
#    daskeys = dict(site='site.name')
#    api2das = [('cms_name', 'site', "re.compile('^T[0-3]_')")]
#    mgr.add_api(system, api, params, daskeys, api2das)

    ##### END OF SITEDB

    # DQ
    system = 'dq'
    api = 'listRuns4DQ'
    params = {'api': 'listRuns4DQ', 'DQFlagList': 'list', 'dataset': 'string'}
    daskeys = dict(dq='dq')
    api2das = [('DQFlagList', 'dq', '')]
    mgr.add_api(system, api, params, daskeys, api2das)
    
    api = 'listSubSystems'
    params = {'api': 'listSubSystems'}
    daskeys = dict(dqsystems='dqsystems')
    api2das = []
    mgr.add_api(system, api, params, daskeys, api2das)

    ##### END OF DQ

    # RunSummary
    system = 'runsum'
    api = ''
    params  = {'DB':'cms_omds_lb', 'FORMAT':'XML'}
    daskeys = dict(run='runnumber', bfield='bfield', hlt='hlt')
    api2das = []
    mgr.add_api(system, api, params, daskeys, api2das)

    mgr.add_notation(system, 'bField', 'bfield')
    mgr.add_notation(system, 'hltKey', 'hlt')

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
    daskeys = dict(jobsummary='jobsummary')
    api2das = [('date1', 'date', ''), ('date2', 'date', ''), 
               ('ce', 'site', "re.compile('([a-zA-Z0-9]+\.){2}')")]
    mgr.add_api(system, api, params, daskeys, api2das)

    ##### END OF Dashboard

    # lumidb
    system = 'lumidb'
    api = 'findTrgPathByRun'
    params = {'run_number':''}
    daskeys = dict(trigpath='trigpath')
    api2das = [('run_number', 'run', '')]
    mgr.add_api(system, api, params, daskeys, api2das)

    api = 'findIntegratedLuminosity'
    params = {'run_number':'', 'tag':'', 'hlt_path':''}
    daskeys = dict(intlumi='intlumi')
    api2das = [('run_number', 'run', ''), ('hlt_path', 'trigpath', '')]
    mgr.add_api(system, api, params, daskeys, api2das)

#    api = 'findAvgIntegratedLuminosity' : {
#        'keys': ['avglumi'],
#        'params' : {'run_number':'', 'tag':'', 'hlt_path':''}
#    },
#    api = 'findIntRawLumi' : {
#        'keys': ['intrawlumi'],
#        'params' : {'run_number':''}
#    },
#    api = 'findL1Counts' : {
#        'keys': ['L1counts'],
#        'params' : {'run_number':'', 'cond_name':''}
#    },
#    api = 'findHLTCounts' : {
#        'keys': ['HLTcounts'],
#        'params' : {'run_number':'', 'path_name':'', 'count_type':''}
#    },
#    api = 'findRawLumi' : {
#        'keys': ['rawlumi'],
#        'params' : {'run_number':'', 'tag':''}
#    },
#    api = 'listLumiByBunch' : {
#        'keys': ['lumibybunch'],
#        'params' : {'run_number':'', 'lumi_section_number':'', 
#                    'option':''}
#    },
#    api = 'listLumiSummary' : {
#        'keys': ['lumisummary'],
#        'params' : {'run_number':'', 'lumi_section_number':'', 
#                    'version':'current'}
#    },
    mgr.add_notation(system, 'run_number', 'run')
    mgr.add_notation(system, 'int_lumi', 'intlumi')
    mgr.add_notation(system, 'hlt_path', 'trigpath')

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
    daskeys = {'monitor':'monitor', 
               'monitor.site':'monitor.site', 
               'monitor.country':'monitor.country',
               'monitor.node':'monitor.node', 
               'monitor.region':'monitor.region',
               'monitor.tier':'monitor.tier'}
    api2das = [('start', 'date', ''), ('end', 'date', '')]
    mgr.add_api(system, api, params, daskeys, api2das)
    ##### END OF monitor

