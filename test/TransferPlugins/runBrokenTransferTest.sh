#!/bin/bash

if [ $# -lt 1 ];then
	echo "USAGE: $0 <fcl_file>"
	exit 1
fi


fcl=$1

if ! [ -e $fcl ];then
	echo "File $fcl Not Found! Aborting..."
	exit 1
fi

log=`basename $fcl|cut -f1 -d.`

# No concurrency of transfer_driver tests!
exec 200>/tmp/broken_transfer_driver_`basename $fcl`.lockfile
flock -e 200

broken_transfer_driver -c $fcl 

exit $?

