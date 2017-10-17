#ifndef artdaq_Application_MPI2_DataLoggerCore_hh
#define artdaq_Application_MPI2_DataLoggerCore_hh

#include "fhiclcpp/ParameterSet.h"
#include "canvas/Persistency/Provenance/RunID.h"
#include "artdaq/Application/DataReceiverCore.hh"


namespace artdaq
{
	class DataLoggerCore;
}

/**
 * \brief DataLoggerCore implements the state machine for the DataLogger artdaq application.
 * DataLoggerCore processes incoming events in one of three roles: Data Logger, Online Monitor, or Dispatcher.
 */
class artdaq::DataLoggerCore : public DataReceiverCore
{
public:

	/**
	* \brief DataLoggerCore Constructor.
	* \param rank Rank of the DataLogger
	* \param name Friendly name for the DataLogger
	* \todo Make the global queue size configurable
	*/
	DataLoggerCore(int rank, std::string name);

	/**
	 * \brief Copy Constructor is deleted
	 */
	DataLoggerCore(DataLoggerCore const&) = delete;

	/**
	* Destructor.
	*/
	~DataLoggerCore();

	/**
	 * \brief Copy Assignment operator is deleted
	 * \return DataLoggerCore copy
	 */
	DataLoggerCore& operator=(DataLoggerCore const&) = delete;

	/**
	* \brief Processes the initialize request.
	* \param pset ParameterSet used to configure the DataLoggerCore
	* \return Whether the initialize attempt succeeded
	* 
	* \verbatim
	* DataLoggerCore accepts the following Parameters:
	* "daq" (REQUIRED): FHiCL table containing DAQ configuration
	*   "DataLogger" (REQUIRED): FHiCL table containing DataLogger paramters
	*     "expected_events_per_bunch" (REQUIRED): Number of events to collect before sending them to art
	*     "enq_timeout" (Default: 5.0): Maximum amount of time to wait while enqueueing events to the ConcurrentQueue
	*     "is_data_logger": True if the DataLogger is a Data Logger
	*     "is_online_monitor": True if the DataLogger is an Online Monitor. is_data_logger takes precedence
	*     "is_dispatcher": True if the DataLogger is a Dispatcher. is_data_logger and is_online_monitor take precedence
	*       NOTE: At least ONE of these three parameters must be specified.
	*     "xmlrpc_client_list" (Default: ""): List of XMLRPC addresses of other applications in the artdaq system
	*     "subrun_size_MB" (Default: 0): Maximum size for DataLogger-based subrun rollover
	*     "subrun_duration" (Dfeault: 0): Maximum time for DataLogger-based subrun rollover
	*     "subrun_event_count" (Default: 0): Maximum event count for DataLogger-based subrun rollover
	*     "inrun_recv_timeout_usec" (Default: 100000): Amount of time to wait for new events while running
	*     "endrun_recv_timeout_usec" (Default: 20000000): Amount of time to wait for additional events at EndOfRun
	*     "pause_recv_timeout_usec" (Default: 3000000): Amount of time to wait for additional events at PauseRun
	*     "onmon_event_prescale" (Default: 1): Only send 1/N events to art for online monitoring (requires is_data_logger: true)
	*     "filesize_check_interval_seconds" (Default: 20): Interval to check the file size when using DataLogger-based subrun rollover
	*     "filesize_check_interval_events" (Default: 20): Interval to check the file size when using DataLogger-based subrun rollover
	*   "metrics": FHiCL table containing configuration for MetricManager
	* "outputs" (REQUIRED): FHiCL table containing output parameters
	*   "normalOutput" (REQUIRED): FHiCL table containing default output parameters
	*     "fileName" (Default: ""): Name template of the output file. Used to determine output directory
	* \endverbatim
	*  Note that the "DataLogger" ParameterSet is also used to configure the EventStore. See that class' documentation for more information.
	*/
	bool initialize(fhicl::ParameterSet const& pset) override;
};

#endif

//  LocalWords:  ds