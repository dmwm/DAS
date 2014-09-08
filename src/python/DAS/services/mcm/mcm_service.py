#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable=W0703,R0913,R0914

"""
MCM data-service plugin.
"""
__author__ = "Valentin Kuznetsov"

# DAS modules
from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator, json_parser
from DAS.utils.url_utils import getdata

class MCMService(DASAbstractService):
    """
    Helper class to provide MCM data-service
    """
    def __init__(self, config):
        DASAbstractService.__init__(self, 'mcm', config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)

    def getdata(self, url, params, expire, headers=None, post=None):
        """URL call wrapper"""
        if  not headers:
            headers =  {'Accept': 'application/json' }
        # MCM uses rest API
        if  'dataset' in params:
            url  = '%s%s' % (url, params.get('dataset'))
        elif 'mcm' in params:
            url = '%s/%s' % (url, params.get('mcm'))
        elif 'prepid' in params:
            url = '%s/%s' % (url, params.get('prepid'))
        else:
            return {}
        params = {}
        result = getdata(url, params, headers, expire, post,
                self.error_expire, self.verbose, self.ckey, self.cert,
                doseq=False, system=self.name)
        return result

    def parser(self, dasquery, dformat, data, api):
        "DAS parser for MCM data-service"
        prim_key  = self.dasmapping.primary_key(self.name, api)
        gen       = json_parser(data, self.logger)
        counter   = 0
        for rec in gen:
            if  'results' in rec:
                row = rec['results']
            else:
                row = rec
            for key in ['_id', '_rev']:
                if  key in row:
                    del row[key]
            if  row:
                if  api == 'dataset4mcm':
                    for val in row.values():
                        if  isinstance(val, basestring):
                            yield {'dataset':{'name': val}}
                        elif isinstance(val, list):
                            for vvv in val:
                                yield {'dataset':{'name': vvv}}
                else:
                    yield {'mcm':row}
            counter += 1
        msg  = "api=%s, format=%s " % (api, dformat)
        msg += "prim_key=%s yield %s rows" % (prim_key, counter)
        self.logger.info(msg)
