#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=C0301,C0103

"""
Read profile.dat file and dump its content
"""
__author__ = "Valentin Kuznetsov"

import pstats   # profiler statistics
from optparse import OptionParser

def profiler(fname, sort=None, strip=False):
    "Print profiler stats"
    stats = pstats.Stats(fname)
    if  sort:
        stats.sort_stats(sort)
    else:
        stats.sort_stats('time', 'calls')
    if  strip:
        stats.strip_dirs()
    stats.print_stats()

class MyOptionParser(object):
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-f", "--file", action="store",\
             type="string", default="profile.dat", dest="file",\
             help="specify input file which contains profile data")
        self.parser.add_option("-s", "--sort", action="store",\
             type="string", default="time", dest="sort",\
             help="specify sorting, e.g. time, calls, cumulative, file")
        self.parser.add_option("--srip", action="store_true",\
             default="False", dest="strip",\
             help="strip leading path from file names")
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
    profiler(opts.file, opts.sort, opts.strip)
