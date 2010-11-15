#!/usr/bin/env python

import urllib
import urllib2
import json
import optparse
import sys
import pprint
from DAS.analytics.analytics_utils import parse_time

"""
DAS analytics CLI tool. Accesses a running web-enabled analytics
instance to set or retrieve information.
"""

if __name__ == '__main__':
    parser = optparse.OptionParser()
    
    group_host = optparse.OptionGroup(parser, "Host")
    group_host.add_option("--host",
                          help="Analytics host including protocol, port and path",
                          default="http://localhost:8213/analytics")
    parser.add_option_group(group_host)
    
    group_task = optparse.OptionGroup(parser, "New Task")
    group_task.add_option("--name",
                          help="Name of the new task")
    group_task.add_option("--class", dest="klass",
                          help="Class of the new task")
    group_task.add_option("--interval", type="int", default=3600,
                          help="Interval of the new task")
    group_task.add_option("--query", metavar='STR/JSON',
                          help="Query (convenience)")
    group_task.add_option("--key", metavar='STR/JSON',
                          help="Key (convenience)")
    group_task.add_option("--once", action="store_true",
                          help="Only run once")
    group_task.add_option("--runs", metavar="N", type="int",
                          help="Only run N times")
    group_task.add_option("--before", metavar="TIMEISH",
                          help="Only run before this time")
    group_task.add_option("--options", metavar="JSON",
                          help="JSON options dictionary")
    parser.add_option_group(group_task)
    
    group_info = optparse.OptionGroup(parser, "Information")
    group_info.add_option("--schedule", action="store_true",
                          help="Print the current schedule")
    group_info.add_option("--results", action="store_true",
                          help="Print the list of available results")
    group_info.add_option("--result", metavar="rID",
                          help="Print the details of result ID")
    group_info.add_option("--task", metavar="tID",
                          help="Print the details of task ID")
    parser.add_option_group(group_info)
    
    group_mod = optparse.OptionGroup(parser, "Modification")
    group_mod.add_option("--remove", action="store_true",
                         help="With --task, request descheduling")
    group_mod.add_option("--reschedule", metavar="TIMEISH",
                         help="With --task, request rescheduling")
    parser.add_option_group(group_mod)
    
    group_time = optparse.OptionGroup(parser, "Time",
                                      "TIMEISH accepts the following\n"+\
                                      "number - GMT UNIX timestamp\n"+\
                                      "+number[smhd] - offset from now\n"+\
                                      "    in secs/mins/hours/days\n"+\
                                      "HH:MM[:SS] - localtime (tomorrow\n"+\
                                      "    if this is in the past)\n"+\
                                      "HH:MM[:SS]gmt|utc|z - as above but GMT")
    parser.add_option_group(group_time)
    
    opts, args = parser.parse_args()
    
    host = opts.host
    if not host[-1] == '/':
        host += '/'
    
    def get_json(path, opts=None):
        url = host + path
        if opts:
            url += '?%s' % urllib.urlencode(opts)
        req = urllib2.Request(url, headers={"Accept": "application/json"})
        try:
            result = urllib2.urlopen(req).read()
        except Exception, e:
            print "There was an error fetching data from %s" % (host+path)
            print "Error was:"
            print e
            sys.exit(0)
        try:
            return json.loads(result)
        except:
            print "There was an error decoding the response as JSON"
            print "Response was:"
            print result
    
    
    if opts.name:
        if not opts.klass:
            print "You must supply a classname"
            sys.exit(0)
        
        kwargs = {}
        if opts.options:
            try:
                kwargs = json.loads(opts.options)
            except:
                print "There was an error loading options=%s" % opts.options
                sys.exit(0)
        query = opts.query
        key = opts.key
        if opts.query:
            if '{' in opts.query and '}' in opts.query:
                print "Assuming that query should be interpreted as JSON"
                try:
                    query = json.loads(opts.query)
                    kwargs['query'] = query
                except:
                    print "There was an error loading query=%s" % opts.query
            else:
                print "Assuming that query should be interpreted as a string"
                kwargs['query'] = query
        if opts.key:
            if '{' in opts.key and '}' in opts.key:
                print "Assuming that key should be interpreted as JSON"
                try:
                    key = json.loads(opts.key)
                    kwargs['key'] = key
                except:
                    print "There was an error loading key=%s" % opts.key
            else:
                print "Assuming that key should be interpreted as a string"
                kwargs['key'] = key
        
        only_once = opts.once
        max_runs = opts.runs
        only_before = opts.before
        form =  {'name': opts.name,
                 'classname': opts.klass,
                 'interval': opts.interval}
        if opts.once:
            form['only_once'] = json.dumps(opts.only_once)
        if opts.runs:
            form['max_runs'] = opts.runs
        if opts.before:
            form['only_before'] = parse_time(opts.before)
        if kwargs:
            form['kwargs'] = json.dumps(kwargs)
        
        pprint.pprint(get_json('add_task', form))
            
    elif opts.schedule:
        pprint.pprint(get_json('schedule'))
            
    elif opts.results:
        pprint.pprint(get_json('results'))
        
    elif opts.result:
        pprint.pprint(get_json('result', {'id': opts.result}))
        
    elif opts.task and opts.remove:
        pprint.pprint(get_json('remove_task', {'id': opts.task}))
        
    elif opts.task and opts.reschedule:
        pprint.pprint(get_json('reschedule_task',
                               {'id': opts.task,
                                'at': parse_time(opts.reschedule)}))
    
    elif opts.task:
        pprint.pprint(get_json('task', {'id': opts.task}))
        
    else:
        print "No action requested. Game over."
    
        
        
        
            
