#ifndef ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH
#define ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH

#include <boost/thread.hpp>
#include <mutex>

#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/ConfigurationTable.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQrate/RequestBuffer.hh"

namespace artdaq {
/// <summary>
/// Receive data requests and make them available to CommandableFragmentGenerator or other interested parties. Track received requests and report errors when inconsistency is detected.
/// </summary>
class RequestReceiver
{
public:
	/// <summary>
	/// Configuration of the RequestReceiver. May be used for parameter validation
	/// </summary>
	struct Config
	{
		/// "receive_requests" (Default: false): Whether this RequestReceiver will listen for requests
		fhicl::Atom<bool> receive_requests{fhicl::Name{"receive_requests"}, fhicl::Comment{"Whether this RequestReceiver will listen for requests"}, false};
		/// "request_port" (Default: 3001) : Port on which data requests will be received
		fhicl::Atom<int> request_port{fhicl::Name{"request_port"}, fhicl::Comment{"Port to listen for request messages on"}, 3001};
		/// "request_address" (Default: "227.128.12.26") : Address which CommandableFragmentGenerator will listen for requests on
		fhicl::Atom<std::string> request_addr{fhicl::Name{"request_address"}, fhicl::Comment{"Multicast address to listen for request messages on"}, "227.128.12.26"};
		/// "multicast_interface_ip" (Default: "0.0.0.0") : Use this hostname for multicast(to assign to the proper NIC)
		fhicl::Atom<std::string> output_address{fhicl::Name{"multicast_interface_ip"}, fhicl::Comment{"Use this hostname for multicast (to assign to the proper NIC)"}, "0.0.0.0"};
		/// "end_of_run_quiet_timeout_ms" (Default: 1000) : Time, in milliseconds, that the entire system must be quiet for check_stop to return true in request mode. **DO NOT EDIT UNLESS YOU KNOW WHAT YOU ARE DOING!**
		fhicl::Atom<size_t> end_of_run_timeout_ms{fhicl::Name{"end_of_run_quiet_timeout_ms"}, fhicl::Comment{"Amount of time (in ms) to wait for no new requests when a Stop transition is pending"}, 1000};
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
		 * \brief RequestReceiver Default Constructor
		 */
	RequestReceiver();

	/**
		 * \brief RequestReceiver Constructor 
		 * \param ps ParameterSet used to configure RequestReceiver. See artdaq::RequestReceiver::Config
		 * \param output_buffer Pointer to RequestBuffer where Requests should be stored
		 */
	RequestReceiver(const fhicl::ParameterSet& ps, std::shared_ptr<RequestBuffer> output_buffer);

	/**
	 * \brief RequestReceiver Destructor
	 */
	virtual ~RequestReceiver();

	/**
		* \brief Opens the socket used to listen for data requests
		*/
	void setupRequestListener();

	/**
		* \brief Disables (stops) the reception of data requests
		* \param force Whether to suppress any error messages (used if called from destructor)
		*/
	void stopRequestReception(bool force = false);

	/**
		* \brief Enables (starts) the reception of data requests
		*/
	void startRequestReception();

	/**
		* \brief This function receives data request packets, adding new requests to the request list
		*/
	void receiveRequestsLoop();

	/// <summary>
	/// Determine if the RequestReceiver is receiving requests
	/// </summary>
	/// <returns>True if the request receiver is running</returns>
	bool isRunning() { return running_; }

	/// <summary>
	/// Sets the current run number
	/// </summary>
	/// <param name="run">The current run number</param>
	void SetRunNumber(uint32_t run) { run_number_ = run; }

private:
	RequestReceiver(RequestReceiver const&) = delete;
	RequestReceiver(RequestReceiver&&) = delete;
	RequestReceiver& operator=(RequestReceiver const&) = delete;
	RequestReceiver& operator=(RequestReceiver&&) = delete;

	bool running_{false};
	std::atomic<bool> request_stop_requested_;
	std::atomic<bool> request_received_;
	std::atomic<bool> should_stop_;

	int request_port_{3001};
	uint32_t run_number_{0};
	std::string request_addr_;
	std::string multicast_in_addr_;
	bool receive_requests_;

	//Socket parameters
	int request_socket_{-1};
	std::chrono::steady_clock::time_point request_stop_timeout_;
	size_t end_of_run_timeout_ms_{1000};
	mutable std::mutex state_mutex_;
	boost::thread requestThread_;

	std::shared_ptr<RequestBuffer> requests_;
};
}  // namespace artdaq

#endif  //ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH
