#!/bin/bash
find $DASHOME -name "*.py" | grep -v __init__.py | grep -v _t.py | \
        awk '{print "pydoc -w "$0""}' | /bin/sh
mv -f *.html doc/html
