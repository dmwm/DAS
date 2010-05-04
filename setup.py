#!/usr/bin/env python

import sys
import os

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages, Feature
from distutils.cmd import Command
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError
from distutils.errors import DistutilsPlatformError, DistutilsExecError
from distutils.core import Extension
from distutils.command.install import INSTALL_SCHEMES

sys.path.append(os.path.join(os.getcwd(), 'src/python'))
from DAS import version as das_version

requirements = []
try:
    import xml.etree.cElementTree
except ImportError:
    try:
        import celementtree
    except ImportError:
        requirements.append("celementtree")
try:
    import pymongo
except ImportError:
    requirements.append("pymongo")

required_python_version = '2.6'
required_pymongo_version = '1.6'

if sys.platform == 'win32' and sys.version_info > (2, 6):
   # 2.6's distutils.msvc9compiler can raise an IOError when failing to
   # find the compiler
   build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError,
                 IOError)
else:
   build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)


class custom_build_ext(build_ext):
    """Allow C extension building to fail.
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
        except DistutilsPlatformError, e:
            print e
            print self.warning_message % ("Extension modules",
                                          "There was an issue with your "
                                          "platform configuration - see above.")

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except build_errors:
            print self.warning_message % ("The %s extension module" % ext.name,
                                          "Above is the ouput showing how "
                                          "the compilation failed.")

c_ext = Feature(
    "optional C extension",
    standard=True,
    ext_modules=[Extension('DAS.extensions.das_speed_utils',
                           include_dirs=['extensions'],
                           sources=['src/python/DAS/extensions/dict_handler.c'])])

if "--no_ext" in sys.argv:
    sys.argv = [x for x in sys.argv if x != "--no_ext"]
    features = {}
else:
    features = {"c-ext": c_ext}

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
package_dir  = {'DAS': 'src/python/DAS'}
package_data = {
    'src': ['python/DAS/services/maps/*.yml', 'python/DAS/web/css/*.css'],
}
#packages     = find_packages('src/python/DAS') 
packages     = find_packages('src/python/') 
packages    += ['src/css', 'src/js', 'src/templates', 'etc', 'bin', 'test', 'doc']
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
        s = "I'm sorry, but %s %s requires Python %s or later."
        print s % (name, version, required_python_version)
        sys.exit(1)

    if  pymongo.version < required_pymongo_version:
        s = "I'm sorry, but %s %s required pymongo %s or later."
        print s % (name, version, required_pymongo_version)
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
        package_dir          = package_dir,
        packages             = packages,
        package_data         = package_data,
#        data_files           = data_files,
        include_package_data = True,
        install_requires     = requirements,
#        scripts              = scriptfiles,
        features             = features,
        classifiers          = classifiers,
        cmdclass             = {"build_ext": custom_build_ext},
        author               = author,
        author_email         = author_email,
        url                  = url,
        license              = license,
    )

if __name__ == "__main__":
    main()

