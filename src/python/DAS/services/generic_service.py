#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
Generic interface for data-service
"""
__revision__ = "$Id: generic_service.py,v 1.1 2010/02/03 16:53:40 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

from DAS.services.abstract_service import DASAbstractService
from DAS.utils.utils import map_validator

class GenericService(DASAbstractService):
    """
    Helper class to provide access to generic data-service
    """
    def __init__(self, name, config):
        DASAbstractService.__init__(self, name, config)
        self.map = self.dasmapping.servicemap(self.name)
        map_validator(self.map)
