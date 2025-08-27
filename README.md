# _artdaq_ Front Page

## About _artdaq_

The _artdaq_ toolkit is a data-acquisition framework designed for high-energy physics experiments. It provides a flexible, reliable backbone for data transfers and has several locations where users can perform custom analysis tasks using the _art_ framework.

The _artdaq_ suite consists of the following packages:

* [trace](../trace): High-performance message logging
* [artdaq-core](../artdaq-core): Data formats used by the artdaq toolkit
* [artdaq-utilities](../artdaq-utilities): Online tools, primarily metrics reporting
* [artdaq-mfextensions](../artdaq-mfextensions): Extensions to the MessageFacility product which are useful in DAQ context
* [artdaq](../artdaq): Application and data transfer framework
* [artdaq-core-demo](../artdaq-core-demo): Data formats used by the artdaq demonstration system
* [artdaq-demo](../artdaq-demo): "User" implementations for the artdaq demonstration system
* [artdaq-daqinterface](../artdaq-daqinterface): Command line run control and example configurations
* [artdaq-database](../artdaq-database): Bindings for MongoDB or local "filesystemdb" configuration databases
* [artdaq-epics-plugin](../artdaq-epics-plugin): Metric endpoint for the EPICS control system

## About this package

This package contains the implementations for the applications and data transfer protocol that form the backbone of the artdaq framework. It also contains several useful executables that can be used to test and debug _artdaq_ systems.
