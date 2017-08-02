#!/bin/bash

fclfile=$1
procs=$2

hydra_nameserver &
ret=`mpirun -np $procs -nameserver localhost transfer_driver_mpi $fclfile`
killall hydra_nameserver

exit $ret
