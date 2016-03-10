#!/bin/bash

hostname=`hostname`

echo "STARTING:$hostname:$1:${@:2}"
$1 -p ${@:2} #&> $1.$2
exitStatus=$?
echo "$hostname:$?:$1:${@:2} ended with status $exitStatus"

if [[ $exitStatus -ne 0 ]] && [[ -n "ARTDAQ_PROCESS_FAILURE_EXIT_DELAY" ]] ; then
    let totalDelay=$ARTDAQ_PROCESS_FAILURE_EXIT_DELAY
    if [[ $totalDelay > 0 ]]; then
        let delayInterval=$totalDelay/10;
        if [[ $delayInterval -lt 10 ]]; then let delayInterval=10; fi
        if [[ $delayInterval -gt 30 ]]; then let delayInterval=30; fi
        let delaySoFar=0
        while [[ $delaySoFar -lt $totalDelay ]]; do
            echo "***** Wrapper script for $hostname:$?:$1:${@:2} sleeping to allow cleanup"
            sleep $delayInterval
            let delaySoFar=$delaySoFar+$delayInterval
        done
    fi
fi

echo "EXITING:$hostname:$?:$1:${@:2}"
