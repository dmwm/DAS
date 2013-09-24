from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize



ext = Extension("fast_recursive_ranker",
                ["fast_recursive_ranker.pyx"],
                language='c'
                # no c++ needed yet
)

fast_ranker = cythonize(
        ext
      )

setup(ext_modules=fast_ranker)

# NOTE: after running build.py one has to manually copy over .c to
# /src/extensions directory