#!/usr/bin/env python
#pylint: disable-msg=c0301,c0103

"""
unit test for logger module
"""

import os
import unittest
from tempfile import NamedTemporaryFile
from DAS.utils.logger import DASLogger

class testDASLogger(unittest.TestCase):
    """
    A test class for the DAS logger module
    """
    def setUp(self):
        """
        set up DAS core module
        """
        self.verbose = 0
        fds = NamedTemporaryFile()
        self.logfile = fds.name
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        self.daslogger = DASLogger(logfile=self.logfile, 
                verbose=self.verbose, format=format)

    def tearDown(self):
        """
        clean-up
        """
        if  os.path.isfile(self.logfile):
            os.remove(self.logfile)

    def logcontent(self):
        """
        Return DAS log content
        """
        lines = open(self.logfile, 'r').readlines()[-1].replace('\n', '')
        return lines

    def test_logger(self):                          
        """test DAS logger methods"""
        self.daslogger.level(1)
        self.daslogger.info('test')
        result = self.logcontent().split()[2:]
        self.assertEqual('INFO', result[3])
        self.assertEqual('test', result[-1])

        self.daslogger.level(2)
        self.daslogger.info('test')
        result = self.logcontent().split()[2:]
        self.assertEqual('INFO', result[3])
        self.assertEqual('test', result[-1])

        self.daslogger.level(2)
        self.daslogger.debug('test')
        result = self.logcontent().split()[2:]
        self.assertEqual('DEBUG', result[3])
        self.assertEqual('test', result[-1])

        self.daslogger.level(0)
        self.daslogger.error('error test')
        result = self.logcontent().split()[2:]
        self.assertEqual('ERROR', result[3])
        self.assertEqual('test', result[-1])

        self.daslogger.level(2)
        self.daslogger.error('error test')
        result = self.logcontent().split()[2:]
        self.assertEqual('ERROR', result[3])
        self.assertEqual('test', result[-1])

        self.daslogger.level(1)
        self.daslogger.warning('test')
        result = self.logcontent().split()[2:]
        self.assertEqual('WARNING', result[3])
        self.assertEqual('test', result[-1])

        self.daslogger.level(1)
        self.daslogger.exception('test')
        result = self.logcontent().split()[2:]
        self.assertEqual('ERROR', result[3])
        self.assertEqual('test', result[-1])

        self.daslogger.level(1)
        self.daslogger.critical('test')
        result = self.logcontent().split()[2:]
        self.assertEqual('CRITICAL', result[3])
        self.assertEqual('test', result[-1])

#
# main
#
if __name__ == '__main__':
    unittest.main()
