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

readme = "DAS stands for Data Aggregation Service"
version = "1.1.1" # need to define it somehow

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
    ext_modules=[Extension('extensions.das_speed_utils',
                           include_dirs=['extensions'],
                           sources=['src/python/DAS/extensions/dict_handler.c'])])

if "--no_ext" in sys.argv:
    sys.argv = [x for x in sys.argv if x != "--no_ext"]
    features = {}
else:
    features = {"c-ext": c_ext}

setup(
    name="DAS",
    version=version,
    description="CMS Data Aggregation Service <https://twiki.cern.ch/twiki/bin/viewauth/CMS/DMWMDataAggregationService>",
    long_description=readme,
    author="Valentin Kuznetsov",
    author_email="vkuznet@gmail.com",
    url="https://twiki.cern.ch/twiki/bin/viewauth/CMS/DMWMDataAggregationService",
    keywords=["CMS", "DAS", "WM"],
    package_dir = {'DAS': 'src/python/DAS', 
                   'core': 'src/python/DAS/core',
                   'extensions': 'src/python/DAS/extensions',
                   'services': 'src/python/DAS/services',
                   'tools': 'src/python/DAS/tools',
                   'utils': 'src/python/DAS/utils',
                   'web': 'src/python/DAS/web'},
    packages = find_packages('src/python/DAS'),
    install_requires=requirements,
    features=features,
    license="CMS experiment software",
#    test_suite="nose.collector",
    classifiers=[
        "Development Status :: 3 - Production/Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: CMS/CERN Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Database"],
    cmdclass={"build_ext": custom_build_ext}
)
