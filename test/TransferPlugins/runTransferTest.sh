#!/bin/bash
fcl=$1

if ! [ -e $fcl ];then
    echo "File $fcl Not Found! Aborting..."
    exit 1
fi

log=`basename $fcl|cut -f1 -d.`

key=$(($RANDOM + 0xFEED0000))

transfer_driver 0 $key $fcl & PID0=$!
transfer_driver 1 $key $fcl & PID1=$!
wait $PID0
rc0=$?
wait $PID1
rc1=$?

exit $(( $rc0 + $rc1 ))

