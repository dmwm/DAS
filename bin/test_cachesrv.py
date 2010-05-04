#!/usr/bin/env python

import sys
from subprocess import call

if  len(sys.argv) != 2:
    print "Usage: test_cachesrv.py <queries.txt>"
    sys.exit(1)

with open(sys.argv[1]) as queries:
    for query in queries:
        query = query.replace('\n', '')
        if  not query or query[0] == '#':
            continue
        cmd = "das_cacheclient --query=\"%s\" --request=POST" % query
        print cmd
        try:
            retcode = call(cmd, shell=True)
            if retcode < 0:
                print >> sys.stderr, "Child was terminated by signal", -retcode
            else:
                print >> sys.stderr, "Child returned", retcode
        except OSError, e:
            print >> sys.stderr, "Execution failed:", e
