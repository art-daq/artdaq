#!/bin/bash

nprocs=4

for ii in `seq $nprocs`;do
  requestReceiver -c requestReceiver_r$ii.fcl & PIDS[$ii]=$!
done

requestSender -c requestSender_multi.fcl

rc=$?
for jj in ${PIDS[@]}; do
  wait $jj
  rc=$(($rc + $?))
done

exit $rc

