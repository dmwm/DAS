# coding=utf-8
"""
Build the fast_recursive_ranker, which is implemented in cython,
into a python extension module.

NOTE: after running build.py one has to manually copy over .c to src/extensions
"""
from __future__ import print_function
import shutil
import os
from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize


def build_cython():
    """ run the build """
    ext = Extension("fast_recursive_ranker", ["fast_recursive_ranker.pyx"],
                    # no c++ needed yet
                    language='c')
    fast_ranker = cythonize(ext)
    setup(ext_modules=fast_ranker)


def copy_to_ext_dir():
    """
    copy the build result (.C file) into extensions dir
    """
    build_dir = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(build_dir, 'fast_recursive_ranker.c')
    dst = os.path.join(build_dir, '../../extensions/fast_recursive_ranker.c')
    print('copying result into extensions dir:' \
          'i.e. {0} into {1}...'.format(src, dst))
    shutil.copy(src, dst)

if __name__ == "__main__":
    build_cython()
    copy_to_ext_dir()
