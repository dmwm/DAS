#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DBS service
"""
__revision__ = "$Id: dbs_service.py,v 1.24 2010/04/09 19:41:23 valya Exp $"
__version__ = "$Revision: 1.24 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, xml_parser

class DBSService(DASAbstractService):
    """
    Helper class to provide DBS service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'dbs', config)
        self.reserved = ['api', 'apiversion']
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def parser(self, query, dformat, source, api):
        """
        DBS data-service parser.
        """
        if  api == 'listBlocks':
            prim_key = 'block'
        elif api == 'listBlockProvenance':
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
        elif api == 'listDatasetSummary':
            prim_key = 'processed_dataset'
        elif api == 'listDatasetParents':
            prim_key = 'processed_dataset_parent'
        elif api == 'listPrimaryDatasets':
            prim_key = 'primary_dataset'
        elif api == 'listProcessedDatasets':
            prim_key = 'processed_dataset'
        elif api == 'listAlgorithms':
            prim_key = 'algorithm'
        elif api == 'listRuns':
            prim_key = 'run'
        else:
            msg = 'DBSService::parser, unsupported %s API %s' \
                % (self.name, api)
            raise Exception(msg)
        gen = xml_parser(source, prim_key)
        for row in gen:
            if  row.has_key('algorithm'):
                del row['algorithm']['ps_content']
            if  row.has_key('processed_dataset') and \
                row['processed_dataset'].has_key('path'):
                    if  isinstance(row['processed_dataset']['path'], dict) \
                    and row['processed_dataset']['path'].has_key('dataset_path'):
                        path = row['processed_dataset']['path']['dataset_path']
                        del row['processed_dataset']['path']
                        row['processed_dataset']['name'] = path
            yield row
