#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=C0301

"""
DAS benchmark tool
"""
from __future__ import print_function
import multiprocessing

import os
import sys
import re
import copy
import math
import string
import random
import urllib
import urllib2
import traceback
import time

from json import JSONDecoder
from random import Random
from optparse import OptionParser
from multiprocessing import Process
from itertools import chain, ifilter

try:
    import matplotlib.pyplot as plt
except Exception as e:
    print(e)


class NClientsOptionParser:
    """client option parser"""

    def __init__(self):
        self.parser = OptionParser(
            usage="./das_bench.py [options]\n\n"
                  "To benchmark keyword search use:\n"
                        "python ./das_bench.py "
                        "--url=https://cmsweb-testbed.cern.ch/das/kws_async "
                        "--nclients=5 "
                        "--dasquery=\"/DoubleMu/A/RAW-RECO magnetic field and run number\""
                        " --accept=text/html"
        )
        self.parser.add_option("-v", "--verbose", action="store",
                               type="int", default=0, dest="debug",
                               help="verbose output")
        self.parser.add_option("--url", action="store", type="string",
                               default="", dest="url",
                               help="specify URL to test, e.g. http://localhost:8211/rest/test")
        self.parser.add_option("--accept", action="store", type="string",
                               default="application/json", dest="accept",
                               help="specify URL Accept header, default application/json")
        self.parser.add_option("--idx-bound", action="store", type="long",
                               default=0, dest="idx",
                               help="specify index bound, by default it is 0")
        self.parser.add_option("--logname", action="store", type="string",
                               default='spammer', dest="logname",
                               help="specify log name prefix where results of N client \
                test will be stored")
        self.parser.add_option("--nclients", action="store", type="int",
                               default=10, dest="nclients",
                               help="specify max number of clients")
        self.parser.add_option("--minclients", action="store", type="int",
                               default=1, dest="minclients",
                               help="specify min number of clients, default 1")
        self.parser.add_option("--repeat", action="store", type="int",
                               default=1, dest="repeat",
                               help="repeat each benchmark multiple times")
        self.parser.add_option("--dasquery", action="store", type="string",
                               default="dataset", dest="dasquery",
                               help="specify DAS query to test, e.g. dataset")
        self.parser.add_option("--output", action="store", type="string",
                               default="results.pdf", dest="filename",
                               help="specify name of output file for matplotlib output, \
                default is results.pdf, can also be file.png etc")

    def get_opt(self):
        """Returns parse list of options"""
        return self.parser.parse_args()


### Natural sorting utilities


def try_int(sss):
    """Convert to integer if possible."""
    try:
        return int(sss)
    except ValueError:
        return sss


def natsort_key(sss):
    """Used internally to get a tuple by which s is sorted."""
    return map(try_int, re.findall(r'(\d+|\D+)', sss))


def natcmp(aaa, bbb):
    """Natural string comparison, case sensitive."""
    return cmp(natsort_key(aaa), natsort_key(bbb))


def natcasecmp(aaa, bbb):
    """Natural string comparison, ignores case."""
    return natcmp(aaa.lower(), bbb.lower())


def natsort(seq, icmp=natcmp):
    """In-place natural string sort."""
    seq.sort(icmp)


def natsorted(seq, icmp=natcmp):
    """Returns a copy of seq, sorted by natural string sort."""
    temp = copy.copy(seq)
    natsort(temp, icmp)
    return temp


def gen_passwd(length=8, chars=string.letters + string.digits):
    """
    Random string generator, code based on
    http://code.activestate.com/recipes/59873-random-password-generation/
    """
    return ''.join(Random().sample(chars, length))


def random_index(bound):
    """Generate random number for given upper bound"""
    return long(random.random() * bound)


### URL utilities


class UrlRequest(urllib2.Request):
    """
    URL requestor class which supports all RESTful request methods.
    It is based on urllib2.Request class and overwrite request method.
    Usage: UrlRequest(method, url=url, data=data), where method is
    GET, POST, PUT, DELETE.
    """

    def __init__(self, method, *args, **kwargs):
        self._method = method
        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        """Return request method"""
        return self._method


def urlrequest(stream, url, headers, write_lock, debug=0):
    """URL request function"""
    if debug:
        print("Input for urlrequest", url, headers, debug)
    req = UrlRequest('GET', url=url, headers=headers)
    if debug:
        hdlr = urllib2.HTTPHandler(debuglevel=1)
        opener = urllib2.build_opener(hdlr)
    else:
        opener = urllib2.build_opener()

    time0 = time.time()
    fdesc = opener.open(req)
    data = fdesc.read()
    ctime = time.time() - time0

    fdesc.close()

    # just use elapsed time if we use html format
    if headers['Accept'] == 'text/html':
        response = {'ctime': str(ctime)}
    else:
        decoder = JSONDecoder()
        response = decoder.decode(data)

    if isinstance(response, dict):
        write_lock.acquire()
        stream.write(str(response) + '\n')
        stream.flush()
        write_lock.release()


def spammer(stream, host, method, params, headers, write_lock, debug=0):
    """start new process for each request"""
    path = method
    if host.find('http://') == -1 and host.find('https://') == -1:
        host = 'http://' + host
    encoded_data = urllib.urlencode(params, doseq=True)
    if encoded_data:
        url = host + path + '?%s' % encoded_data
    else:
        url = host + path
    proc = Process(target=urlrequest,
                   args=(stream, url, headers, write_lock, debug))
    proc.start()
    return proc


def runjob(nclients, host, method, params, headers, idx, limit,
           debug=0, logname='spammer', dasquery=None):
    """
    Run spammer for provided number of parralel clients, host name
    and method (API). The output data are stored into lognameN.log,
    where logname is an optional parameter with default as spammer.
    """
    stream = open('%s%s.log' % (logname, nclients), 'a')

    # write-lock needed as times are written to files within multiprocessing
    w_lock = multiprocessing.Lock()

    processes = []
    for _ in range(0, nclients):
        if dasquery:
            ### REPLACE THIS PART with your set of parameter
            if dasquery.find('=') == -1:
                query = '%s=/%s*' % (dasquery, gen_passwd(1, string.letters))
            else:
                query = dasquery
            params = {'query': query, 'idx': random_index(idx), 'limit': limit}
            if method == '/rest/testmongo':
                params['collection'] = 'das.merge'

        # keyword search
        if method == '/das/kws_async':
            params = {'input': dasquery or 'Zmm number of events',
                      'instance': 'prod/global'}
            headers = {'Accept': 'text/html'}

        ###
        proc = spammer(stream, host, method, params, headers, w_lock, debug)
        processes.append(proc)
    while True:
        if not processes:
            break
        for proc in processes:
            if proc.exitcode is not None:
                processes.remove(proc)
                break
    # an iteration is done
    stream.close()


def avg_std(input_file):
    """Calculate average and standard deviation"""
    count = 0
    total = 0
    arr = []
    with open(input_file) as input_data:
        for line in input_data.readlines():
            if not line:
                continue
            data = {}
            try:
                data = eval(line.replace('\n', ''))
            except:
                print("In file '%s' fail to process line='%s'" \
                      % (input_file, line))
                traceback.print_exc()
                continue
            if 'ctime' in data:
                res = float(data['ctime'])
                total += res
                count += 1
                arr.append(res)
    if count:
        mean = total / count
        std2 = 0
        for item in arr:
            std2 += (mean - item) ** 2
        return mean, math.sqrt(std2 / count)
    else:
        msg = 'Unable to count results'
        raise Exception(msg)


def make_plot(xxx, yyy, std=None, name='das_cache.pdf',
              xlabel='Number of clients', ylabel='Time/request (sec)',
              yscale=None, title=''):
    """Make standard plot time vs nclients using matplotlib"""
    plt.plot(xxx, yyy, 'ro-')
    plt.grid(True)
    if yscale:
        plt.yscale('log')
    plt.axis([min(xxx) - 1, max(xxx) + 5, min(yyy) - 5, max(yyy) + 5])
    if std:
        plt.errorbar(xxx, yyy, yerr=std)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    frmt = name.split('.')[-1] or 'pdf'
    plt.savefig(name, format=frmt, transparent=True)
    plt.close()


def main():
    """Main routine"""
    mgr = NClientsOptionParser()
    (opts, args) = mgr.get_opt()

    url = opts.url.replace('?', ';').replace('&amp;', ';').replace('&', ';')
    logname = opts.logname
    dasquery = opts.dasquery
    idx = opts.idx
    limit = 1
    nclients = opts.nclients
    minclients = opts.minclients
    debug = opts.debug
    headers = {'Accept': opts.accept}
    urlpath, args = urllib.splitattr(url)
    repeat = opts.repeat
    arr = urlpath.split('/')
    if arr[0] == 'http:' or arr[0] == 'https:':
        host = arr[0] + '//' + arr[2]
    else:
        msg = 'Provided URL="%s" does not contain http:// part' % opts.url
        raise Exception(msg)
    method = '/' + '/'.join(arr[3:])
    params = {}
    for item in args:
        key, val = item.split('=')
        params[key] = val

    # do clean-up
    for filename in os.listdir('.'):
        if filename.find('.log') != -1 and filename.find(logname) != -1:
            os.remove(filename)

    # perform action
    array = []
    if nclients <= 10:
        array += range(1, nclients + 1)
    if 10 < nclients <= 100:
        array = chain(range(1, 10),
                      range(10, nclients + 1, 10))
    if 100 < nclients <= 1000:
        array = chain(range(1, 10),
                      range(10, 100, 10),
                      range(100, nclients + 1, 100))

    # allow to specify the starting nclients
    array = ifilter(lambda x: x >= minclients, array)

    for nclients in array:
        sys.stdout.write("Run job with %s clients" % nclients)
        for _ in range(repeat):
            runjob(nclients, host, method, params, headers, idx, limit,
                   debug, logname, dasquery)
            sys.stdout.write('.')
        print('')

    # analyze results
    file_list = []
    for filename in os.listdir('.'):
        if filename.find('.log') != -1:
            file_list.append(filename)
    xxx = []
    yyy = []
    std = []

    for ifile in natsorted(file_list):
        name, _ = ifile.split('.')
        # ignore non related .log files and .smf
        if not logname in name or not name:
            continue
        xxx.append(int(name.split(logname)[-1]))
        mean, std2 = avg_std(ifile)
        yyy.append(mean)
        std.append(std2)
    try:
        make_plot(xxx, yyy, std, opts.filename, title=dasquery)
    except Exception as e:
        print(e)
        print("xxx =", xxx)
        print("yyy =", yyy)
        print("std =", std)


if __name__ == '__main__':
    main()

