#!/usr/bin/env python
"""
LumiDB service
"""
__revision__ = "$Id: lumidb_service.py,v 1.8 2009/09/01 01:42:46 valya Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator 

class LumiDBService(DASAbstractService):
    def __init__(self, config):
        DASAbstractService.__init__(self, 'lumidb', config)
        self.map = self.dasmapping.servicemap(self.name, 'javaservlet')
        map_validator(self.map)
