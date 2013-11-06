# coding=utf-8
# time declarations based on
#  https://groups.google.com/forum/#!topic/sage-devel/kSL7mRKM5HU

# Cython version of cputime():
cdef extern from "time.h" nogil:
    # TODO: do we need nogil here?!
    ctypedef unsigned long clock_t
    cdef clock_t clock()
    cdef enum:
        CLOCKS_PER_SEC

##### To use: #####
#
#    cdef clock_t start, end
#    cdef double cpu_time_used
#    start = clock()
#    # Do the work.
#    end = clock()
#    cpu_time_used = (<double> (end - start)) / CLOCKS_PER_SEC
#
###################

