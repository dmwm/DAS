from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize



ext = Extension("fast_recursive_ranker",
                ["fast_recursive_ranker.pyx"],
                language='c'
                # no c++ needed yet
                # language='c'
)

fast_ranker = cythonize(
        ext
      )

#,      cmdclass = {'build_ext': build_ext}
# TODO: how to build both cython and simple libs?
setup(ext_modules=fast_ranker)