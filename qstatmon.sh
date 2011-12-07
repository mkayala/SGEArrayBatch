#!/bin/bash
# Mkayala 2011
# Simple script to wait until all jobs sent in are complete.
# Assumes we are on a submit host (so qstat works!)
# Uses xml version of qstat output to ensure full jobnames
# are written out

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

echo "Going to run this command: qstat -xml | $cmd"

res=`qstat -xml | $cmd`
if [ $? -ne 0 ]
then
    exit 100
fi

while [ $res -gt 0 ]
do
    sleep 10
    res=`qstat -xml | $cmd`
done

exit 0

