#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Read profile.dat file and dump its content
"""
__revision__ = "$Id: read_profile.py,v 1.1 2009/03/18 19:17:49 valya Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Valentin Kuznetsov"

import hotshot.stats             # profiler statistics
from optparse import OptionParser

def profiler(fname):
    stats = hotshot.stats.load(fname)
    stats.sort_stats('time', 'calls')
    stats.print_stats()

class MyOptionParser: 
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-f", "--file", action="store", 
                                          type="string", default="profile.dat", 
                                          dest="file",
             help="specify input file which contains profile data")
    def getOpt(self):
        """
        Returns parse list of options
        """
        return self.parser.parse_args()

#
# main
#
if __name__ == '__main__':
    optManager  = MyOptionParser()
    (opts, args) = optManager.getOpt()
    
    fname = opts.file
    profiler(fname)

