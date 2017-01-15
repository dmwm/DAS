#!/usr/bin/env python

"""
Standard python setup.py file for DAS
to build     : python setup.py build
to install   : python setup.py install --prefix=<some dir>
to clean     : python setup.py clean
to build doc : python setup.py doc
to run tests : python setup.py test
"""
from __future__ import print_function
__author__ = "Valentin Kuznetsov"

import sys
import os
import subprocess
from unittest import TextTestRunner, TestLoader
from glob import glob
from os.path import splitext, basename, join as pjoin, walk
from distutils.core import setup
from distutils.cmd import Command
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError
from distutils.errors import DistutilsPlatformError, DistutilsExecError
from distutils.core import Extension
from distutils.command.install import INSTALL_SCHEMES

import fnmatch


sys.path.append(os.path.join(os.getcwd(), 'src/python'))
from DAS import version as das_version

required_python_version = '2.7'

if sys.platform == 'win32' and sys.version_info > (2, 6):
   # 2.6's distutils.msvc9compiler can raise an IOError when failing to
   # find the compiler
   build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError,
                 IOError)
else:
   build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)

class TestCommand(Command):
    """
    Class to handle unit tests
    """
    user_options = [ ]

    def initialize_options(self):
        """Init method"""
        self._dir = os.getcwd()

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """
        Finds all the tests modules in test/, and runs them.
        """
        exclude = [pjoin(self._dir, 'test', 'cern_sso_auth_t.py')]
        testfiles = []
        for t in glob(pjoin(self._dir, 'test', '*_t.py')):
            if  not t.endswith('__init__.py') and t not in exclude:
                testfiles.append('.'.join(
                    ['test', splitext(basename(t))[0]])
                )
        testfiles.sort()
        try:
            tests = TestLoader().loadTestsFromNames(testfiles)
        except Exception as exc:
            print("\nFail to load unit tests", testfiles)
            # check which tests are failing to get imported
            for test in testfiles:
                try:
                    print("trying to import:",  test)
                    __import__(test)
                except Exception as import_err:
                    print("failed importing: ", test, import_err)
            print(exc)
            raise exc
        t = TextTestRunner(verbosity = 2)
        result = t.run(tests)
        # return a non-zero exit status on failure -- useful in CI
        if not result.wasSuccessful():
            sys.exit(1)

class CleanCommand(Command):
    """
    Class which clean-up all pyc files
    """
    user_options = [ ]

    def initialize_options(self):
        """Init method"""
        self._clean_me = [ ]
        for root, dirs, files in os.walk('.'):
            for fname in files:
                if  fname.endswith('.pyc') or fname. endswith('.py~') or \
                    fname.endswith('.rst~'):
                    self._clean_me.append(pjoin(root, fname))

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """Run method"""
        for clean_me in self._clean_me:
            try:
                os.unlink(clean_me)
            except:
                pass

class DocCommand(Command):
    """
    Class which build documentation
    """
    user_options = [ ]

    def initialize_options(self):
        """Init method"""
        pass

    def finalize_options(self):
        """Finalize method"""
        pass

    def run(self):
        """Run method"""
        cdir = os.getcwd()
        os.chdir(os.path.join(cdir, 'doc'))
        if  'PYTHONPATH' in os.environ:
            os.environ['PYTHONPATH'] = os.path.join(cdir, 'src/python') \
                + ':' + os.environ['PYTHONPATH']
        else:
            os.environ['PYTHONPATH'] = os.path.join(cdir, 'src/python')
        subprocess.call('make html', shell=True)
        os.chdir(cdir)

class BuildExtCommand(build_ext):
    """
    Allow C extension building to fail.
    The C extension speeds up DAS, but is not essential.
    """

    warning_message = """
**************************************************************
WARNING: %s could not
be compiled. No C extensions are essential for DAS to run,
although they do result in significant speed improvements.

%s
**************************************************************
"""

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError as e:
            print(e)
            print(self.warning_message % ("Extension modules",
                                          "There was an issue with your "
                                          "platform configuration - see above."))

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except build_errors:
            print(self.warning_message % ("The %s extension module" % ext.name,
                                          "Above is the ouput showing how "
                                          "the compilation failed."))

def dirwalk(relativedir):
    """
    Walk a directory tree and look-up for __init__.py files.
    If found yield those dirs. Code based on
    http://code.activestate.com/recipes/105873-walk-a-directory-tree-using-a-generator/
    """
    dir = os.path.join(os.getcwd(), relativedir)
    for fname in os.listdir(dir):
        fullpath = os.path.join(dir, fname)
        if  os.path.isdir(fullpath) and not os.path.islink(fullpath):
            for subdir in dirwalk(fullpath):  # recurse into subdir
                yield subdir
        else:
            initdir, initfile = os.path.split(fullpath)
            if  initfile == '__init__.py':
                yield initdir

def find_packages(relativedir):
    packages = [] 
    for dir in dirwalk(relativedir):
        package = dir.replace(os.getcwd() + '/', '')
        package = package.replace(relativedir + '/', '')
        package = package.replace('/', '.')
        packages.append(package)
    return packages

def datafiles(dir, pattern=None):
    """Return list of data files in provided relative dir"""
    files = []
    for dirname, dirnames, filenames in os.walk(dir):
        for subdirname in dirnames:
            files.append(os.path.join(dirname, subdirname))
        for filename in filenames:
            if  filename[-1] == '~':
                continue
            # match file name pattern (e.g. *.css) if one given
            if pattern and not fnmatch.fnmatch(filename, pattern):
                continue
            files.append(os.path.join(dirname, filename))
    return files
#    return [os.path.join(dir, f) for f in os.listdir(dir)]
    
version      = das_version
name         = "DAS"
description  = "CMS Data Aggregation System"
readme       ="""
DAS stands for Data Aggregation System
<https://twiki.cern.ch/twiki/bin/viewauth/CMS/DMWMDataAggregationService>
"""
author       = "Valentin Kuznetsov",
author_email = "vkuznet@gmail.com",
scriptfiles  = filter(os.path.isfile, ['etc/das.cfg'])
url          = "https://twiki.cern.ch/twiki/bin/viewauth/CMS/DMWMDataAggregationService",
keywords     = ["DAS", "Aggregation", "Meta-data"]
package_dir  = {'DAS': 'src/python/DAS', 'pyurlfetch': 'src/python/pyurlfetch'}
packages     = find_packages('src/python')
data_files   = [
                ('DAS/etc', ['etc/das.cfg']),
                ('DAS/test', datafiles('test')),
                ('DAS/services/maps', datafiles('src/python/DAS/services/maps')),
                ('DAS/services/cms_maps', datafiles('src/python/DAS/services/cms_maps')),
                ('DAS/services/bootstrap_queries', 
                                 datafiles('src/python/DAS/services/bootstrap_queries')),

                ('DAS/kws_data/db_dumps', datafiles('src/kws_data/db_dumps')),
                ('DAS/kws_data/kws_index', datafiles('src/kws_data/kws_index')),

                ('DAS/web/js', datafiles('src/js')),
                ('DAS/web/css', datafiles('src/css')),
                ('DAS/web/images', datafiles('src/images')),
                ('DAS/web/templates', datafiles('src/templates')),
                ('DAS/web/jinja_templates', datafiles('src/jinja_templates')),
               ]
license      = "CMS experiment software"
classifiers  = [
    "Development Status :: 3 - Production/Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: CMS/CERN Software License",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: POSIX",
    "Programming Language :: Python",
    "Topic :: Database"
]

def main():
    if sys.version < required_python_version:
        s = "I'm sorry, but %s %s requires Python %s or later. Found %s"
        print(s % (name, version, required_python_version, sys.version))
        sys.exit(1)

    # set default location for "data_files" to
    # platform specific "site-packages" location
    for scheme in INSTALL_SCHEMES.values():
        scheme['data'] = scheme['purelib']

    dist = setup(
        name                 = name,
        version              = version,
        description          = description,
        long_description     = readme,
        keywords             = keywords,
        packages             = packages,
        package_dir          = package_dir,
        data_files           = data_files,
        scripts              = datafiles('bin'),
        requires             = ['python (>=2.6)', 'pymongo (>=1.6)', 'ply (>=3.3)',
                                'sphinx (>=1.0.4)', 'cherrypy (>=3.1.2)',
                                'Cheetah (>=2.4)', 'yaml (>=3.09)',
                                # keyword search
                                'nltk', 'jsonpath_rw'],
        ext_modules          = [Extension('DAS.extensions.das_speed_utils',
                                   include_dirs=['extensions'],
                                   sources=['src/python/DAS/extensions/dict_handler.c']),
                               Extension('DAS.extensions.das_hash',
                                   include_dirs=['extensions'],
                                   sources=['src/python/DAS/extensions/das_hash.c']),

                               # fast_recursive_ranker is based on cython, but
                               # is distributed as .c source
                               # .pyx->.c to be compiled separately
                               #  so cython is not required for installation
                               Extension('DAS.extensions.fast_recursive_ranker',
                                   sources=['src/python/DAS/extensions/fast_recursive_ranker.c'])],
        classifiers          = classifiers,
        cmdclass             = {'build_ext': BuildExtCommand,
                                'test': TestCommand,
                                'doc': DocCommand,
                                'clean': CleanCommand},
        author               = author,
        author_email         = author_email,
        url                  = url,
        license              = license,
    )

if __name__ == "__main__":
    main()

