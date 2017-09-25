#!/usr/bin/env python
#pylint: disable-msg=c0301,c0103

"""
unit test for logger module
"""

import sys
import unittest
try:
    import cStringIO as StringIO
except ImportError: # python3
    import io as StringIO
except:
    import StringIO
from DAS.utils.logger import PrintManager, funcname

class testDASLogger(unittest.TestCase):
    """
    A test class for PrintManager
    """
    def setUp(self):
        "set up"
        self.name = 'Logger'

    def test_funcname(self):
        "Test funcname"
        result = funcname()
        expect = 'test_funcname'
        self.assertEqual(expect, result)
        
    def test_error(self):
        "Test logger error method"
        old_stdout = sys.stdout
        logger = PrintManager(self.name) # verbose is irrelevant
        sys.stdout = StringIO.StringIO()
        logger.error('test')
        result = sys.stdout.getvalue()
        expect = 'ERROR %s:%s test\n' % (self.name, funcname())
        self.assertEqual(expect, result)
        sys.stdout = old_stdout

    def test_warning(self):
        "Test logger warning method"
        old_stdout = sys.stdout
        logger = PrintManager(self.name) # verbose is irrelevant
        sys.stdout = StringIO.StringIO()
        logger.warning('test')
        result = sys.stdout.getvalue()
        expect = 'WARNING %s:%s test\n' % (self.name, funcname())
        self.assertEqual(expect, result)
        sys.stdout = old_stdout

    def test_info(self):
        "Test logger info method"
        old_stdout = sys.stdout
        logger = PrintManager(self.name, verbose=1)
        sys.stdout = StringIO.StringIO()
        logger.info('test')
        result = sys.stdout.getvalue()
        expect = 'INFO %s:%s test\n' % (self.name, funcname())
        self.assertEqual(expect, result)
        sys.stdout = old_stdout

    def test_debug(self):
        "Test logger debug method"
        old_stdout = sys.stdout
        logger = PrintManager(self.name, verbose=2)
        sys.stdout = StringIO.StringIO()
        logger.debug('test')
        result = sys.stdout.getvalue()
        expect = 'DEBUG %s:%s test\n' % (self.name, funcname())
        self.assertEqual(expect, result)
        sys.stdout = old_stdout


#
# main
#
if __name__ == '__main__':
    unittest.main()
