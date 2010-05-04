#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
DAS command line interface
"""
__revision__ = "$Id: das_cli.py,v 1.9 2009/05/19 12:43:11 valya Exp $"
__version__ = "$Revision: 1.9 $"
__author__ = "Valentin Kuznetsov"

import time
from optparse import OptionParser
from DAS.core.das_core import DASCore
from DAS.utils.utils import dump

import sys
if sys.version_info < (2, 5):
    raise Exception("DAS requires python 2.5 or greater")

class DASOptionParser: 
    """
    DAS cli option parser
    """
    def __init__(self):
        self.parser = OptionParser()
        self.parser.add_option("-v", "--verbose", action="store", 
                                          type="int", default=0, 
                                          dest="verbose",
             help="verbose output")
        self.parser.add_option("--profile", action="store_true", 
                                          dest="profile",
             help="profile output")
        self.parser.add_option("--input", action="store", type="string", 
                                          default=False, dest="input",
             help="specify input for your request.")
        self.parser.add_option("--services", action="store_true", 
                                          dest="services",
             help="return a list of supported data services")
        self.parser.add_option("--keys", action="store", 
                                          dest="service",
             help="return set of keys for given data service")
        self.parser.add_option("--view", action="store", 
                                          dest="view",
             help="return view definition in DAS, use --view=all to list all views")
        self.parser.add_option("--create-view", action="store", 
                                          dest="createview",
             help="create a new view in DAS, e.g. --create-view=name,query")
        self.parser.add_option("--update-view", action="store", 
                                          dest="updateview",
             help="update a view in DAS, e.g. --update-view=name,query")
        self.parser.add_option("--delete-view", action="store", 
                                          dest="deleteview",
             help="delete a view in DAS, e.g. --delete-view=name")
        self.parser.add_option("--no-format", action="store_true", 
                                          dest="plain",
             help="return unformatted output, useful for scripting")
        self.parser.add_option("--limit", action="store", type="int", 
                                          default=0, dest="limit",
             help="limit number of returned results")
        self.parser.add_option("--no-output", action="store_true", 
                                          dest="nooutput",
             help="run DAS but don't print results")
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

    t0 = time.time()
    query = opts.input
    if  opts.verbose:
        debug = opts.verbose
    else:
        debug = 0
    DAS = DASCore(debug=debug)
    sdict = DAS.keys()
    if  opts.services:
        msg = "DAS services:" 
        print msg
        print "-"*len(msg)
        keys = sdict.keys()
        keys.sort()
        for key in keys:
            print key
    elif  opts.service:
        msg = "DAS service %s:" % opts.service
        print msg
        print "-"*len(msg)
        keys = sdict[opts.service]
        keys.sort()
        for key in keys:
            print key
    elif  opts.view:
        if  opts.view == 'all':
            view = None
        else:
            view = opts.view
        for name, query in DAS.get_view(view).items():
            print 'view name: %s' % name
            print 'DAS query: %s' % query
    elif  opts.createview:
        vlist = opts.createview.split(',')
        name  = vlist[0]
        query = ','.join(vlist[1:])
        print "Creating a view '%s' with query '%s'" % (name, query)
        DAS.create_view(name, query)
    elif  opts.updateview:
        vlist = opts.updateview.split(',')
        name  = vlist[0]
        query = ','.join(vlist[1:])
        print "Updating a view '%s' with query '%s'" % (name, query)
        DAS.update_view(name, query)
    elif  opts.deleteview:
        name = opts.deleteview
        DAS.delete_view(name)
        print "View '%s' has been deleted" % name
    elif query:

        if  opts.profile:
            import hotshot                   # Python profiler
            import hotshot.stats             # profiler statistics
            print "Start DAS in profile mode"
            profiler = hotshot.Profile("profile.dat")
            profiler.run("DAS.result(query)")
            profiler.close()
            stats = hotshot.stats.load("profile.dat")
            stats.sort_stats('time', 'calls')
            stats.print_stats()
        else:
            results = DAS.result(query)
            if  not opts.nooutput:
                if  opts.plain:
                    for item in results:
                        print item
                else:
                    dump(results, limit=opts.limit)
    else:
        print
        print "DAS CLI interface, no actions found,"
        print "please use --help for more options."
    timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime())
    if  debug:
        for key, val in DAS.timer.timer.items():
            if  len(val) > 1:
                print "DAS execution time (%s) %s sec" % (key, val[-1] - val[0])
    print "DAS execution time %s sec, %s" % ((time.time()-t0), timestamp)


