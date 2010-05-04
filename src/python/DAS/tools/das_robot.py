#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS Robot
"""
__revision__ = "$Id: das_robot.py,v 1.1 2009/09/18 14:08:12 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import sys
if  sys.version_info < (2, 6):
    raise Exception("DAS requires python 2.6 or greater")

from optparse import OptionParser
from DAS.core.das_robot import Robot

class DASOptionParser: 
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-q", "--query", action="store", type="string", 
                                          default=False, dest="query",
             help="specify query for your request.")
        self.parser.add_option("-s", "--sleep", action="store", type="int", 
                                          default=600, dest="sleep",
             help="specify sleep time for DAS populator")
        self.parser.add_option("--start", action="store_true", dest="start",
             help="start DAS populator")
        self.parser.add_option("--stop", action="store_true", dest="stop",
             help="stop DAS populator")
        self.parser.add_option("--restart", action="store_true", dest="restart",
             help="restart DAS populator")
        self.parser.add_option("--status", action="store_true", dest="status",
             help="status of DAS populator")
    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()
#
# main
#
if __name__ == '__main__':
    optManager  = DASOptionParser()
    (opts, args) = optManager.getOpt()

    robot = Robot(opts.query, opts.sleep)
    if  opts.start:
        robot.start()
    elif opts.stop:
        robot.stop()
    elif opts.restart:
        robot.restart()
    elif  opts.status:
        robot.status()
        sys.exit(0)
    else:
        print "Unknown operation, please use --start|stop|restart|status options"

