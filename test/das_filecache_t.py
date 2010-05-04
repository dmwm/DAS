#!/usr/bin/env python
#pylint: disable-msg=C0301,C0103

"""
Unit test for DAS filecache class
"""

import os
import unittest
import tempfile
from DAS.core.das_core import DASCore
from DAS.core.das_filecache import DASFilecache, yyyymmdd

class testDASFilecache(unittest.TestCase):
    """
    A test class for the DAS filecache class
    """
    def setUp(self):
        """
        set up DAS core module
        """
        debug = 0
        self.das = DASCore(debug=debug)
        self.dir = tempfile.mkdtemp()
        self.das.filecache_dir = self.dir
        print "temp dir", self.dir
        self.dasfilecache = DASFilecache(self.das, idir=self.dir)

    def test_result(self):                          
        """test DAS filecache result method"""
        query  = "find site where site=T2_UK"
        result = self.dasfilecache.result(query)
        result.sort()
        expect = self.das.result(query)
        expect.sort()
        self.assertEqual(expect, result)
        # remove temp dir content
        for root, dirs, files in os.walk(self.dir):
            for filename in files:
                os.remove(os.path.join(root, filename))
            for dirname in dirs:
                for r, d, f in os.walk(os.path.join(root, dirname)):
                    for filename in f:
                        os.remove(os.path.join(r, filename))
                os.rmdir(os.path.join(root, dirname))
        os.rmdir(self.dir)
#
# main
#
if __name__ == '__main__':
    unittest.main()

