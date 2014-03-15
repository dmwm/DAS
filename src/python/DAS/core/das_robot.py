#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
DAS robot base class. Code based on
http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
"""

__revision__ = "$Id: das_robot.py,v 1.4 2010/03/05 18:14:46 valya Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "Valentin Kuznetsov"

import os
import sys
import time
import atexit
from signal import SIGTERM

# DAS modules
from DAS.utils.utils import genkey, getarg, print_exc
from DAS.core.das_core import DASCore

class Robot(object):
    """
    DAS Robot (daemon) class to fetch data from provided URL/API
    and store them into DAS cache.
    """
    def __init__(self, config=None, query=None, sleep=600):
        self.dascore = DASCore(config, nores=True)
        logdir       = getarg(config, 'logdir', '/tmp')
        self.pidfile = os.path.join(logdir, 'robot-%s.pid' % genkey(query))

        if (hasattr(os, "devnull")):
            devnull  = os.devnull
        else:
            devnull  = "/dev/null"

        self.stdin   = devnull # we do not read from stdinput
        self.stdout  = getarg(config, 'stdout', devnull)
        self.stderr  = getarg(config, 'stderr', devnull)
        self.query   = query
        self.sleep   = sleep

    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if  pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError as err:
            sys.stderr.write("fork #1 failed: %d (%s)\n" \
                % (err.errno, err.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.umask(0)
        os.setsid()

        # do second fork
        try:
            pid = os.fork()
            if  pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError as err:
            sys.stderr.write("fork #2 failed: %d (%s)\n" \
                % (err.errno, err.strerror))
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        stdi = file(self.stdin, 'r')
        stdo = file(self.stdout, 'a+')
        stde = file(self.stderr, 'a+', 0)
        os.dup2(stdi.fileno(), sys.stdin.fileno())
        os.dup2(stdo.fileno(), sys.stdout.fileno())
        os.dup2(stde.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write("%s\n" % pid)

    def delpid(self):
        """Delete PID file"""
        os.remove(self.pidfile)

    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pidf = file(self.pidfile,'r')
            pid  = int(pidf.read().strip())
            pidf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

        # Start the daemon
        self.daemonize()
        self.run()

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pidf = file(self.pidfile, 'r')
            pid = int(pidf.read().strip())
            pidf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError as err:
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print_exc(err)
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    def status(self):
        """
        Return status information about Robot instance.
        """
        # Get the pid from the pidfile
        try:
            pidf  = file(self.pidfile, 'r')
            pid = int(pidf.read().strip())
            pidf.close()
        except IOError:
            pid = None

        if  not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        print "DAS populator information"
        print "PID    :", pid
        print "pidfile:", self.pidfile
        print "stdin  :", self.stdin
        print "stdout :", self.stdout
        print "stderr :", self.stderr
        print "sleep  :", self.sleep
        print "query  :", self.query


    def run(self):
        """
        Method which will be called after the process has been
        daemonized by start() or restart().
        """
        if  not self.query:
            print "DAS query is not provided"
            sys.exit(1)

        while True:
            self.dascore.call(self.query)
            time.sleep(self.sleep)
