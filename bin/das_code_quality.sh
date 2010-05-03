#!/bin/sh

find $DASHOME -name "*.py" | \
awk '{print "echo; echo \"running pylint tests for "$0"\"; pylint "$0" | grep \"Your code\""}' | \
/bin/sh | awk '{split($0,a,"("); print a[1]}'
