#!/bin/bash
fcl=$1
nprocs=$2

if ! [ -e $fcl ];then
    echo "File $fcl Not Found! Aborting..."
    exit 1
fi

if [ $nprocs -lt 2 ]; then
    echo "Must use at least 2 processes!"
	exit 2
fi

log=`basename $fcl|cut -f1 -d.`

for ii in `seq $nprocs`;do
  rank=$(($ii - 1))
  transfer_driver $rank $fcl & PIDS[$ii]=$!
done

rc=0
for jj in ${PIDS[@]}; do
  wait $jj
  rc=$(($rc + $?))
done

exit $rc

