#ifndef proto_Config_hh
#define proto_Config_hh

#include <iostream>
#include <string>
#include "fhiclcpp/fwd.h"
#include "artdaq/DAQrate/quiet_mpi.hh"

// sources are first, sinks are second
// the offset_ is the index of the first sink
// the offset_ = sources_

// Define the environment variable ARTDAQ_DAQRATE_USE_ART to any value
// to set use_artapp_ to true.

namespace artdaq {

	/**
	 * \brief Helper class for configuring the builder test
	 */
	class Config
	{
	public:
		/**
		 * \brief String for command-line arguments
		 */
		static const char* usage;

		/**
		 * \brief Type of the builder instance
		 */
		enum TaskType : int
		{
			TaskSink = 0, ///< This Builder is a "Sink"
			TaskDetector = 2 ///< This Builder is a "Detector"
		};

		/**
		 * \brief Config Constructor
		 * \param rank Rank of this process
		 * \param nprocs Total number of processes in the test
		 * \param buffer_count Number of Fragments that may be in-flight simultaneously
		 * \param max_payload_size Maximum size of the Fragments
		 * \param argc Number of arguments
		 * \param argv Array of arguments, as strings
		 */
		Config(int rank, int nprocs, int buffer_count, size_t max_payload_size, int argc, char* argv[]);

		/**
		 * \brief Get the number of destinations for this process
		 * \return The number of destinations for this process
		 */
		int destCount() const;

		/**
		 * \brief Get the rank of the first destination for this process
		 * \return The rank of the first destination for this process
		 */
		int destStart() const;

		/**
		 * \brief Get the number of sources for this process
		 * \return The number of sources for this process
		 */
		int srcCount() const;

		/**
		 * \brief Get the rank of the first source for this process
		 * \return The rank of the first source for this process
		 */
		int srcStart() const;

		/**
		 * \brief Get the corresponding destination for this source
		 * \return The corresponding destiantion for this source
		 */
		int getDestFriend() const;

		/**
		 * \brief Get the corresponding source for this destination
		 * \return The corresponding source for this destination
		 */
		int getSrcFriend() const;

		/**
		 * \brief Gets the count of arguments after a -- delimiter
		 * \param argc Original argc
		 * \param argv Original argv
		 * \return argc of arguments after --
		 */
		int getArtArgc(int argc, char* argv[]) const;

		/**
		 * \brief Get the array of arguments after a -- delimiter
		 * \param argc Original argc
		 * \param argv Original argv
		 * \return argv of arguments after --
		 */
		char** getArtArgv(int argc, char* argv[]) const;

		/**
		 * \brief Get the name of the type of this process
		 * \return The name of the type of this process (one of "Sink", "Source" or "Detector")
		 */
		std::string typeName() const;

		/**
		 * \brief Write information about this Config class to a file
		 */
		void writeInfo() const;

		// input parameters
		int rank_; ///< Rank of this application
		int total_procs_; ///< Total number of processes

		int detectors_; ///< Count of detectors
		int sinks_; ///< Count of sinks
		int detector_start_; ///< Rank of first detector
		int sink_start_; ///< Rank of first sink

		int event_queue_size_; ///< Size of the Event Queue
		int run_; ///< Run Number

		int buffer_count_; ///< Maximum number of simulatneous Fragments
		size_t max_payload_size_; ///< Maximum size of Fragments to create/transfer

		// calculated parameters
		TaskType type_; ///< Type of this Builder application
		int offset_; ///< Offset from the start rank for this type
		std::string node_name_; ///< Name of this node, from MPI_Get_processor_name

		int art_argc_; ///< Count of arguments used for art
		char** art_argv_; ///< Arguments used for art
		bool use_artapp_; ///< Whether to use art

		/**
		 * \brief Dump configuration information (space delimited) to a stream
		 * \param ost Stream to dump configuration to
		 */
		void print(std::ostream& ost) const;

		/**
		 * \brief Write configuration parameter names to a stream
		 * \param ost Stream to write configuration parameter names to
		 */
		void printHeader(std::ostream& ost) const;

		/**
		 * \brief Write a ParameterSet using configuration
		 * \return A ParameterSet with sources and destinations blocks, as appropriate for the Task
		 */
		fhicl::ParameterSet makeParameterSet() const;

		/**
		 * \brief Get the ParameterSet to use to configure art
		 * \return The ParameterSet used to configure art
		 * 
		 * \verbatim
		 * The ParameterSet is read for the following Parameters:
		 * "daq" (REQUIRED): DAQ config FHiCL table
		 *   "buffer_count" (Default: buffer_count): Maximum number of simulatneous Fragments
		 *   "max_fragment_size_words" (Default: max_payload_size): Maximum size of Fragments to create/transfer
		 * \endverbatim
		 */
		fhicl::ParameterSet getArtPset();

		/**
		 * \brief Write the usage to cerr and throw an exception
		 * \param argv0 First parameter in argv
		 * \param msg Exception message
		 */
		static void throwUsage(char* argv0, const std::string& msg)
		{
			std::cerr << argv0 << " " << artdaq::Config::usage << "\n";
			throw msg;
		}

		/**
		 * \brief Get the detectors count from the command line
		 * \param argc Argument count
		 * \param argv Array of arguments as strings
		 * \return Detector count
		 */
		static double getArgDetectors(int argc, char* argv[])
		{
			if (argc < 2) { throwUsage(argv[0], "no detectors_per_node argument"); }
			return atof(argv[1]);
		}

		/**
		* \brief Get the sink count from the command line
		* \param argc Argument count
		* \param argv Array of arguments as strings
		* \return Sink count
		*/
		static double getArgSinks(int argc, char* argv[])
		{
			if (argc < 3) { throwUsage(argv[0], "no sinks_per_node argument"); }
			return atof(argv[2]);
		}

		/**
		* \brief Get the Queue Size from the command line
		* \param argc Argument count
		* \param argv Array of arguments as strings
		* \return Queue size
		*/
		static int getArgQueueSize(int argc, char* argv[])
		{
			if (argc < 4) { throwUsage(argv[0], "no event_queue_size argument"); }
			return atoi(argv[3]);
		}

		/**
		* \brief Get the Run number from the command line
		* \param argc Argument count
		* \param argv Array of arguments as strings
		* \return Run number
		*/
		static int getArgRun(int argc, char* argv[])
		{
			if (argc < 5) { throwUsage(argv[0], "no run argument"); }
			return atoi(argv[4]);
		}

		/**
		 * \brief Call MPI_Get_processor_name
		 * \return The result of MPI_Get_processor_name
		 */
		static std::string getProcessorName()
		{
			char buf[100];
			int sz = sizeof(buf);
			MPI_Get_processor_name(buf, &sz);
			return std::string(buf);
		}
	};

	/**
	 * \brief Stream a Config object to the given stream
	 * \param ost Stream to write to
	 * \param c Config to stream
	 * \return Stream with config printed to it
	 */
	inline std::ostream& operator<<(std::ostream& ost, Config const& c)
	{
		c.print(ost);
		return ost;
	}
}
#endif /* proto_Config_hh */
