#!/bin/bash
# Determines the appropriate version of artdaq_mfextensions to install, and also fetches its dependencies.

if [ -e "./setups" ] && [ -d "ups" ]; then
    echo "UPS area detected!"
    if [ -z "${ARTDAQ_FQ_DIR-}" ]; then
	echo "Please have the desired version of ARTDAQ set up before running this script!"
        exit 1
    fi
else
    echo "This script must be run from a UPS product directory!"
    exit 1
fi

if [ -n ${os-} ]; then
    echo "Cloning cetpkgsupport to determine current OS"
    git clone http://cdcvs.fnal.gov/projects/cetpkgsupport
    os=`./cetpkgsupport/bin/get-directory-name os`
    rm -rf cetpkgsupport
fi

productDir=
os=
quals=
