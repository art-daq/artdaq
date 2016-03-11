#!/bin/bash

hostname=`hostname`

echo "STARTING:$hostname:$1:${@:2}"
$1 -p ${@:2} #&> $1.$2

# 11-Mar-2016, KAB: added an optional delay (of configurable length) to allow
# for graceful cleanup in the event of a crash of one of the artdaq processes.
# Basically, the idea is that, if env var ARTDAQ_PROCESS_FAILURE_EXIT_DELAY is
# set and contains a non-zero (positive) number of seconds, this script will
# wait that number of seconds before exiting. That should allow an external 
# control process (e.g. RunControl) to notice that an artdaq process has crashed
# and send the usual sequence of run stop commands (if a run is ongoing).  Sending
# the stop commands will hopefully get one or more EventBuilders to send their
# endSubRun messages to the DiskWriting Aggregator, and the stop message to the AG
# will prompt it to close the disk files cleanly.
# For reference, "export ARTDAQ_PROCESS_FAILURE_EXIT_DELAY=150" is the command to
# set the env var, and this can be done in the shell in which startXYZSystem.sh is run.
exitStatus=$?
echo "$hostname:$?:$1:${@:2} ended with status $exitStatus"
if [[ $exitStatus -ne 0 ]] && [[ -n "$ARTDAQ_PROCESS_FAILURE_EXIT_DELAY" ]] ; then
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
