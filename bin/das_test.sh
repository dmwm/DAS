#!/bin/sh
#find $DAS_ROOT/test -name "*_t.py" | \
#awk '{print "echo; echo \"running unit tests for "$0"\"; python "$0""}' | \
#/bin/sh
tests=`find $DAS_ROOT/test -name "*_t.py"`
for t in $tests
do
    memcache=`echo $t | grep memcache`
    memprocess=`ps auxww | grep memcache | grep -v grep`
    couchcache=`echo $t | grep couchcache`
    couchprocess=`ps auxww | grep couchdb | grep -v grep`
    if [ "$memcache" != "" ] && [ "$memprocess" == "" ]; then
        echo "No memcached process found"
        echo "Skip test $t"
    elif [ "$couchcache" != "" ] && [ "$couchprocess" == "" ]; then
        echo "No couchdb process found"
        echo "Skip test $t"
    else
        echo $t | \
        awk '{print "echo; echo \"running unit tests for "$0"\"; python "$0""}'|\
        /bin/sh
    fi
done; 
