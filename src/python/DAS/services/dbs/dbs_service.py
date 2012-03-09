#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS service
"""
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, xml_parser, qlxml_parser
from DAS.utils.utils import dbsql_opt_map, convert_datetime

def convert_dot(row, key, attrs):
    """Convert dot notation key.attr into storage one"""
    for attr in attrs:
        if  row.has_key(key) and row[key].has_key(attr):
            name = attr.split('.')[-1]
            row[key][name] = row[key][attr]
            del row[key][attr]

class DBSService(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dbs', config)
        self.reserved = ['api', 'apiversion']
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)
        self.prim_instance = config['dbs']['dbs_global_instance']
        self.instances = config['dbs']['dbs_instances']

    def url_instance(self, url, instance):
        """
        Adjust URL for a given instance
        """
        if  instance in self.instances:
            return url.replace(self.prim_instance, instance)
        return url

    def adjust_params(self, api, kwds, inst=None):
        """
        Adjust DBS2 parameters for specific query requests
        """
        if  api == 'fakeStatus':
            val = kwds['status']
            if  val:
                kwds['query'] = \
                'find dataset.status where dataset.status=%s' % val
            else:
                kwds['query'] = 'find dataset.status'
            val = kwds['dataset']
            if  val:
                if  kwds['query'].find(' where ') != -1:
                    kwds['query'] += ' and dataset=%s' % val
                else:
                    kwds['query'] += ' where dataset=%s' % val
            kwds.pop('status')
        if  api == 'listPrimaryDatasets':
            pat = kwds['pattern']
            if  pat[0] == '/':
                kwds['pattern'] = pat.split('/')[1]
        if  api == 'listProcessedDatasets':
            pat = kwds['processed_datatset_name_pattern']
            if  pat[0] == '/':
                try:
                    kwds['processed_datatset_name_pattern'] = pat.split('/')[2]
                except:
                    pass
        if  api == 'fakeReleases':
            val = kwds['release']
            if  val != 'required':
                kwds['query'] = 'find release where release=%s' % val
            else:
                kwds['query'] = 'required'
            kwds.pop('release')
        if  api == 'fakeRelease4File':
            val = kwds['file']
            if  val != 'required':
                kwds['query'] = 'find release where file=%s' % val
            else:
                kwds['query'] = 'required'
            kwds.pop('file')
        if  api == 'fakeRelease4Dataset':
            val = kwds['dataset']
            if  val != 'required':
                kwds['query'] = 'find release where dataset=%s' % val
            else:
                kwds['query'] = 'required'
            kwds.pop('dataset')
        if  api == 'fakeConfig':
            val = kwds['dataset']
            sel = 'config.name, config.content, config.version, config.type, \
 config.annotation, config.createdate, config.createby, config.moddate, \
 config.modby'
            if  val != 'required':
                kwds['query'] = 'find %s where dataset=%s' % (sel, val)
            else:
                kwds['query'] = 'required'
            kwds.pop('dataset')
        if  api == 'fakeSite4Dataset' and inst and inst != self.prim_instance:
            val = kwds['dataset']
            if  val != 'required':
                kwds['query'] = "find site where dataset=%s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('dataset')
        if  api == 'fakeListDataset4File':
            val = kwds['file']
            if  val != 'required':
                kwds['query'] = "find dataset, count(block), count(file.size), \
  sum(block.size), sum(block.numfiles), sum(block.numevents), dataset.status \
  where file=%s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('file')
        if  api == 'fakeListDataset4Block':
            val = kwds['block']
            if  val != 'required':
                kwds['query'] = "find dataset, count(block), count(file.size), \
  sum(block.size), sum(block.numfiles), sum(block.numevents) \
  where block=%s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('block')
        if  api == 'fakeRun4Run':#runregistry don't support 'in'
            val = kwds['run']
            if  val != 'required':
                if  isinstance(val, dict):
                    min_run = 0
                    max_run = 0
                    if  val.has_key('$lte'):
                        max_run = val['$lte']
                    if  val.has_key('$gte'):
                        min_run = val['$gte']
                    if  min_run and max_run:
                        val = "run >=%s and run <= %s" % (min_run, max_run)
                    elif val.has_key('$in'):
                        arr = [r for r in val['$in']]
                        val = "run >=%s and run <= %s" % (arr[0], arr[-1])
                elif isinstance(val, int):
                    val = "run = %d" % val
                kwds['query'] = "find run where %s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('run')
        if  api == 'fakeGroup4Dataset':
            val = kwds['dataset']
            if  val != 'required':
                val = "dataset = %s" % val
                kwds['query'] = "find phygrp where %s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('dataset')
        if  api == 'fakeChild4File':
            val = kwds['file']
            if  val != 'required':
                val = "file = %s" % val
                kwds['query'] = "find file.child where %s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('file')
        if  api == 'fakeChild4Dataset':
            val = kwds['dataset']
            if  val != 'required':
                val = "dataset = %s" % val
                kwds['query'] = "find dataset.child where %s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('dataset')
        if  api == 'fakeDataset4Run':#runregistry don't support 'in'
            val = kwds['run']
            qlist = []
            if  val != 'required':
                if  isinstance(val, dict):
                    min_run = 0
                    max_run = 0
                    if  val.has_key('$lte'):
                        max_run = val['$lte']
                    if  val.has_key('$gte'):
                        min_run = val['$gte']
                    if  min_run and max_run:
                        val = "run >=%s and run <= %s" % (min_run, max_run)
                    elif val.has_key('$in'):
                        arr = [r for r in val['$in']]
                        val = "run >=%s and run <= %s" % (arr[0], arr[-1])
                elif isinstance(val, int):
                    val = "run = %d" % val
                if  kwds.has_key('dataset') and kwds['dataset']:
                    val += ' and dataset=%s' % kwds['dataset']
                kwds['query'] = \
                "find dataset where %s and dataset.status like VALID*" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('run')
            kwds.pop('dataset')
        if  api == 'fakeRun4File':
            val = kwds['file']
            if  val != 'required':
                kwds['query'] = "find run where file = %s" % val
            else:
                kwds['query'] = 'required'
            kwds.pop('file')
        if  api == 'fakeFiles4DatasetRunLumis':
            cond = ""
            val = kwds['dataset']
            if  val and val != 'required':
                cond = " and dataset=%s" % val
                kwds.pop('dataset')
            val = kwds['run']
            if  val and val != 'required':
                cond += " and run=%s" % val
                kwds.pop('run')
            val = kwds['lumi']
            if  val and val != 'required':
                cond += " and lumi=%s" % val
                kwds.pop('lumi')
            if  cond:
                kwds['query'] = "find file.name where %s" % cond[4:]
            else:
                kwds['query'] = 'required'
        if  api == 'fakeDatasetSummary':
            value = ""
            for key, val in kwds.iteritems():
                if  key == 'dataset' and val:
                    value += ' and dataset=%s' % val
                if  key == 'primary_dataset' and val:
                    value += ' and primds=%s' % val
                if  key == 'release' and val:
                    value += ' and release=%s' % val
                if  key == 'tier' and val:
                    value += ' and tier=%s' % val
                if  key == 'phygrp' and val:
                    value += ' and phygrp=%s' % val
                if  key == 'datatype' and val:
                    value += ' and datatype=%s' % val
                if  key == 'status':
                    if  val:
                        value += ' and dataset.status=%s' % val
                    else:
                        value += ' and dataset.status like VALID*'
            keys = ['dataset', 'release', 'primary_dataset', 'tier', \
                'phygrp', 'datatype', 'status']
            for key in keys:
                try:
                    del kwds[key]
                except:
                    pass
            if  value:
                kwds['query'] = "find dataset, datatype, dataset.status, \
procds.createdate, procds.createby, procds.moddate, procds.modby, \
sum(block.numfiles), sum(block.numevents), count(block), sum(block.size) \
where %s" % value[4:]
            else:
                kwds['query'] = 'required'
        if  api == 'fakeListDatasetbyDate':
            value = ''
            if  kwds['status']:
                value = ' and dataset.status=%s' % kwds['status']
            else:
                value = ' and dataset.status like VALID*'
#           20110126/{'$lte': 20110126}/{'$lte': 20110126, '$gte': 20110124} 
            query_for_single = "find dataset, datatype, dataset.status, \
  count(block), sum(block.size), sum(block.numfiles), sum(block.numevents), \
  dataset.createdate where dataset.createdate %s %s " + value
            query_for_double = "find dataset, datatype, dataset.status, \
  count(block), sum(block.size), sum(block.numfiles), sum(block.numevents), \
  dataset.createdate where dataset.createdate %s %s \
  and dataset.createdate %s %s " + value
            val = kwds['date']
            qlist = []
            query = ""
            if val != "required":
                if isinstance(val, dict):
                    for opt in val:
                        nopt = dbsql_opt_map(opt)
                        if nopt == ('in'):
                            self.logger.debug(val[opt])
                            nval = [convert_datetime(x) for x in val[opt]]
                        else:
                            nval = convert_datetime(val[opt])
                        qlist.append(nopt)
                        qlist.append(nval)
                    if len(qlist) == 4:
                        query = query_for_double % tuple(qlist)
                    else:
                        msg = "dbs_services::fakeListDatasetbyDate \
 wrong params get, IN date is not support by DBS2 QL"
                        self.logger.info(msg)
                elif isinstance(val, int):
                    val = convert_datetime(val)
                    query = query_for_single % ('=', val)
                kwds['query'] = query
            else:
                kwds['query'] = 'required'
            kwds.pop('date')
            
    def parser(self, dasquery, dformat, source, api):
        """
        DBS data-service parser.
        """
        query = dasquery.mongo_query
        if  api == 'listBlocks':
            prim_key = 'block'
        elif api == 'listBlocks4path':
            api = 'listBlocks'
            prim_key = 'block'
        elif api == 'listBlockProvenance':
            prim_key = 'block'
        elif api == 'listBlockProvenance4child':
            prim_key = 'block'
        elif api == 'listFiles':
            prim_key = 'file'
        elif api == 'listLFNs':
            prim_key = 'file_lfn'
        elif api == 'listFileLumis':
            prim_key = 'file_lumi_section'
        elif api == 'listFileProcQuality':
            prim_key = 'file_proc_quality'
        elif api == 'listFileParents':
            prim_key = 'file_parent'
        elif api == 'listTiers':
            prim_key = 'data_tier'
        elif api == 'listDatasetParents':
            prim_key = 'processed_dataset_parent'
        elif api == 'listPrimaryDatasets':
            prim_key = 'primary_dataset'
        elif api == 'listProcessedDatasets':
            prim_key = 'processed_dataset'
        elif api == 'fakeReleases':
            prim_key = 'release'
        elif api == 'listRuns':
            prim_key = 'run'
        elif  api == 'fakeRelease4File':
            prim_key = 'release'
        elif  api == 'fakeRelease4Dataset':
            prim_key = 'release'
        elif  api == 'fakeGroup4Dataset':
            prim_key = 'group'
        elif  api == 'fakeConfig':
            prim_key = 'config'
        elif  api == 'fakeListDataset4Block':
            prim_key = 'dataset'
        elif  api == 'fakeListDataset4File':
            prim_key = 'dataset'
        elif  api == 'fakeListDatasetbyDate':
            prim_key = 'dataset'
        elif  api == 'fakeDatasetSummary':
            prim_key = 'dataset'
        elif  api == 'fakeDataset4Run':
            prim_key = 'dataset'
        elif  api == 'fakeRun4File':
            prim_key = 'run'
        elif  api == 'fakeRun4Run':
            prim_key = 'run'
        elif api == 'fakeChild4File':
            prim_key = 'child'
        elif api == 'fakeChild4Dataset':
            prim_key = 'child'
        elif api == 'fakeSite4Dataset':
            prim_key = 'site'
        elif api == 'fakeStatus':
            prim_key = 'status'
        elif api == 'fakeFiles4DatasetRunLumis':
            prim_key = 'file'
        else:
            msg = 'DBSService::parser, unsupported %s API %s' \
                % (self.name, api)
            raise Exception(msg)
        if  api.find('fake') != -1:
            gen = qlxml_parser(source, prim_key)
        else:
            gen = xml_parser(source, prim_key)
        useless_run_atts = ['number_of_events', 'number_of_lumi_sections', \
                'id', 'total_luminosity', 'store_number', 'end_of_run', \
                'start_of_run']
        for row in gen:
            if  not row:
                continue
            if  row.has_key('status') and \
                row['status'].has_key('dataset.status'):
                row['status']['name'] = row['status']['dataset.status']
                del row['status']['dataset.status']
            if  row.has_key('dataset') and \
                row['dataset'].has_key('dataset.status'):
                row['dataset']['status'] = row['dataset']['dataset.status']
                del row['dataset']['dataset.status']
            if  row.has_key('file_lumi_section'):
                row['lumi'] = row['file_lumi_section']
                del row['file_lumi_section']
            if  row.has_key('algorithm'):
                del row['algorithm']['ps_content']
            if  row.has_key('processed_dataset') and \
                row['processed_dataset'].has_key('path'):
                if  isinstance(row['processed_dataset']['path'], dict) \
                and row['processed_dataset']['path'].has_key('dataset_path'):
                    path = row['processed_dataset']['path']['dataset_path']
                    del row['processed_dataset']['path']
                    row['processed_dataset']['name'] = path
            # case for fake apis
            # remove useless attribute from results
            if  row.has_key('dataset'):
                if  row['dataset'].has_key('count_file.size'):
                    del row['dataset']['count_file.size']
                if  row['dataset'].has_key('dataset'):
                    name = row['dataset']['dataset']
                    del row['dataset']['dataset']
                    row['dataset']['name'] = name
            if  row.has_key('child') and row['child'].has_key('dataset.child'):
                row['child']['name'] = row['child']['dataset.child']
                del row['child']['dataset.child']
            if  row.has_key('child') and row['child'].has_key('file.child'):
                row['child']['name'] = row['child']['file.child']
                del row['child']['file.child']
            if  row.has_key('block') and query.get('fields') == ['parent']:
                row['parent'] = row['block']
                del row['block']
            if  row.has_key('block') and query.get('fields') == ['child']:
                row['child'] = row['block']
                del row['block']
            if  row.has_key('run') and row['run'].has_key('run'):
                row['run']['run_number'] = row['run']['run']
                del row['run']['run']
            if  row.has_key('release') and row['release'].has_key('release'):
                row['release']['name'] = row['release']['release']
                del row['release']['release']
            if  row.has_key('site'):
                row['site']['se'] = row['site']['site']
                del row['site']['site']
            attrs = ['config.name', 'config.content', 'config.version', \
                     'config.type', 'config.annotation', 'config.createdate', \
                     'config.createby', 'config.moddate', 'config.modby']
            convert_dot(row, 'config', attrs)
            convert_dot(row, 'file', ['file.name'])
            # remove DBS2 run attributes (to be consistent with DBS3 output)
            # and let people extract this info from CondDB/LumiDB.
            if  row.has_key('run'):
                for att in useless_run_atts:
                    try:
                        del row['run'][att]
                    except:
                        pass
            yield row
