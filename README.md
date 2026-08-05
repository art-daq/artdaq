# artdaq Front Page

## About the framework

The _artdaq_ toolkit is a data-acquisition framework designed for high-energy physics experiments. It provides a flexible, reliable backbone for data transfers and has several locations where users can perform custom analysis tasks using the _art_ framework.

The _artdaq_ suite consists of the following packages:

* [trace](https://art-daq.github.io/artdaq_doxygen/trace): High-performance message logging
* [artdaq-core](https://art-daq.github.io/artdaq_doxygen/artdaq-core): Data formats used by the artdaq toolkit
* [artdaq-utilities](https://art-daq.github.io/artdaq_doxygen/artdaq-utilities): Online tools, primarily metrics reporting
* [artdaq-mfextensions](https://art-daq.github.io/artdaq_doxygen/artdaq-mfextensions): Extensions to the MessageFacility product which are useful in DAQ context
* [artdaq](https://art-daq.github.io/artdaq_doxygen/artdaq): Application and data transfer framework
* [artdaq-core-demo](https://art-daq.github.io/artdaq_doxygen/artdaq-core-demo): Data formats used by the artdaq demonstration system
* [artdaq-demo](https://art-daq.github.io/artdaq_doxygen/artdaq-demo): "User" implementations for the artdaq demonstration system
* [artdaq-daqinterface](https://art-daq.github.io/artdaq_doxygen/artdaq-daqinterface): Command line run control and example configurations
* [artdaq-database](https://art-daq.github.io/artdaq_doxygen/artdaq-database): Bindings for MongoDB or local "filesystemdb" configuration databases
* [artdaq-epics-plugin](https://art-daq.github.io/artdaq_doxygen/artdaq-epics-plugin): Metric endpoint for the EPICS control system

## About this package

This package contains the implementations for the applications and data transfer protocol that form the backbone of the artdaq framework. It also contains several useful executables that can be used to test and debug _artdaq_ systems.

## Basic operation (SQA)

For first-time setup and a complete walk-through of downloading, building, and running a working system, use the [artdaq-demo](https://art-daq.github.io/artdaq_doxygen/artdaq-demo) instructions. Those instructions remain the recommended "getting started" path for basic _artdaq_ operation.

A typical operational cycle for _artdaq_ is:

1. Set up the runtime environment so required products are available.
2. Start the required _artdaq_ processes with a valid FHiCL configuration.
3. Initialize, start, and stop runs using your run-control tooling (for example, _artdaq-daqinterface_).
4. Review MessageFacility/TRACE logs to confirm healthy transitions and data flow.

## Common errors and troubleshooting (SQA)

Common operational errors are usually visible in process logs:

* **Missing environment setup / missing libraries**  
  Symptoms include startup failures, missing executable/library messages, or plugin-load errors.  
  Action: verify your product setup and environment variables, then restart processes.
* **Invalid FHiCL configuration**  
  Symptoms include configuration parsing exceptions or process failure during initialization.  
  Action: validate FHiCL syntax/parameter names and retry with corrected configuration.
* **Network or transfer-plugin connectivity issues**  
  Symptoms include timeout, socket, or connection-refused messages between _artdaq_ processes.  
  Action: verify hostnames, ports, firewall rules, and process ordering.
* **Stale shared-memory resources after abnormal termination**  
  Symptoms include failures when restarting data-receiver or event-builder processes after a crash/forced stop.  
  Action: clean up stale shared-memory resources and restart the affected processes.
* **Art process startup/runtime failures**  
  Symptoms include repeated restart attempts or "art processes have died" messages in logs.  
  Action: check PMT/MessageFacility logs for the first error, correct the underlying configuration/runtime issue, and restart.
  
## Software quality assurance (SQA)

_artdaq_ is maintained under the lab software quality assurance program and follows documented procedures for how software is designed, developed, reviewed, tested, and maintained.

For SQA process details, see:

* [artdaq Wiki](https://github.com/art-daq/artdaq/wiki)
* [Fermilab Software QA working documents (SharePoint)](https://fermipoint.fnal.gov/organization/cs/ocio/opm/QA/SoftwareQA/WorkingDocuments/Shared%20Documents/Forms/AllItems.aspx)

As _art_ SQA documentation is finalized, _artdaq_ should review and adopt those materials where applicable, and keep references current in the wiki and project documentation.
