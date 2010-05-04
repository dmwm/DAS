#!/bin/bash

#find $DASHOME -name "*.py" | \
#awk '{print "echo; echo \"running pylint tests for "$0"\"; pylint "$0" | grep \"Your code\""}' | \
#/bin/sh | awk '{split($0,a,"("); print a[1]}'

if [ "$#" -gt 1 ]; then
    echo "Usage: das_code_quality.sh <optional threashold up to 10>"
    exit 1
fi 

if [ "$#" -eq 0 ]; then
thr=8
else
thr=$1
fi

echo "Run pylint with $thr/10 threashold level"
echo

files=`find $DASHOME -name "*.py"`
fail_files=0
for f in $files
do
    echo
    echo $f
    out=`pylint $f | grep "Your code"`
    msg=`echo $out | grep -v "No config" | awk '{split($0,a,"at "); split(a[2],b,"/"); split(b[1],c,".");if(c[1]<THR) print "FAIL, score "b[1]"/10"; else print "PASS"}' THR=$thr`
    if [ "$msg" == "PASS" ]; then
        echo "+++ PASS"
    else
        echo "--- $msg"
        fail_files=$(($fail_files+1))
    fi
done

if [ $fail_files != 0 ]; then
    echo "===== Total number of failures ====="
    echo "FAIL $fail_files times"
fi
