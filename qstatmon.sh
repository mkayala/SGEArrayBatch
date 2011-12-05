#!/bin/bash
# Matt Kayala 2011
# Simple script to wait until all jobs are complete.
# 
# If qstat is not available will exit 100 immediately.

usage="Usage: `basename $0` jobname1 [jobname2 ...]"

if [ -z "$1" ]
then
    echo $usage
    exit 1;
fi

cmd="grep -c"
for job in $@
do
    cmd="$cmd -e $job"
done

res=`qstat | $cmd`
if [ $? -ne 0 ]
then
    exit 100
fi

while [ $res -gt 0 ]
do
    sleep 10
    res=`qstat | $cmd`
done

exit 0

