#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=C0301,C0103

"""
DAS config generator
"""
from __future__ import print_function
__author__ = "Valentin Kuznetsov"

# system modules
import os
import ConfigParser
from optparse import OptionParser

# DAS modules
from DAS.utils.das_config import DAS_OPTIONS, das_configfile

class ConfigOptionParser(object):
    "option parser"
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-i", "--in", action="store", type="string", \
            default="", dest="uinput", help="input file, default $DAS_CONFIG")
        self.parser.add_option("-o", "--out", action="store", type="string", \
            default="", dest="output", help="output file")

    def get_opt(self):
        "Returns parse list of options"
        return self.parser.parse_args()

def main():
    "Main function"
    optmgr = ConfigOptionParser()
    opts, _ = optmgr.get_opt()
    _ = opts.uinput if opts.uinput else das_configfile()
    outconfig = opts.output if opts.output else \
                os.path.join(os.getcwd(), 'das_cms.cfg')
    config = ConfigParser.ConfigParser()
    for option in DAS_OPTIONS:
        option.write_to_configparser(config, use_default=False)
    config.write(open(outconfig, 'wb'))
    print("Created DAS configuration file", outconfig)

if __name__ == '__main__':
    main()
