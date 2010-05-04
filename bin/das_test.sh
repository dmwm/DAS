#!/bin/sh
find $DASHOME/test -name "*_t.py" | \
awk '{print "echo; echo \"running unit tests for "$0"\"; python "$0""}' | \
/bin/sh
