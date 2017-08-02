#!/bin/bash

fclfile=$1
procs=$2

hydra_nameserver &
ns_pid=$!

mpirun -np $procs -nameserver localhost transfer_driver_mpi $fclfile
ret=$?
kill $ns_pid

exit $ret
