#!/bin/bash
fcl=$1

if ! [ -e $fcl ];then
    echo "File $fcl Not Found! Aborting..."
    exit 1
fi

log=`basename $fcl|cut -f1 -d.`

transfer_driver 0 $fcl & PID0=$!
transfer_driver 1 $fcl & PID1=$!
wait $PID0
rc0=$?
wait $PID1
rc1=$?

exit $(( $rc0 + $rc1 ))

