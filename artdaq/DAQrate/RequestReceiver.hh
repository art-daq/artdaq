#ifndef ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH
#define ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH

#include <boost/thread.hpp>
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQrate/detail/RequestMessage.hh"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/ConfigurationTable.h"

#include <condition_variable>
#include <mutex>

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
		/// "request_port" (Default: 3001) : Port on which data requests will be received
		fhicl::Atom<int> request_port{fhicl::Name{"request_port"}, fhicl::Comment{"Port to listen for request messages on"}, 3001};
		/// "request_address" (Default: "227.128.12.26") : Address which RequestReceiver will listen for requests on
		fhicl::Atom<std::string> request_addr{fhicl::Name{"request_address"}, fhicl::Comment{"Multicast address to listen for request messages on"}, "227.128.12.26"};
		/// "multicast_interface_ip" (Default: "0.0.0.0") : Use this hostname for multicast(to assign to the proper NIC)
		fhicl::Atom<std::string> output_address{fhicl::Name{"multicast_interface_ip"}, fhicl::Comment{"Use this hostname for multicast (to assign to the proper NIC)"}, "0.0.0.0"};
		/// "acknowledgement_port" (Default: 3002) : Port to which acknowledgements will be sent, if requested
		fhicl::Atom<int> acknowledgement_port{fhicl::Name{"acknowledgement_port"}, fhicl::Comment{"Port to which acknowledgements will be sent, if requested"}, 3002};
		/// "acknowledgement_address" (Default: "localhost") : Address to send acknowledgements to
		fhicl::Atom<std::string> acknowledgement_address{fhicl::Name{"acknowledgement_address"}, fhicl::Comment{"Address to send acknowledgements to"}, "localhost"};
		/// "end_of_run_quiet_timeout_ms" (Default: 1000) : Time, in milliseconds, that the entire system must be quiet for check_stop to return true in request mode. **DO NOT EDIT UNLESS YOU KNOW WHAT YOU ARE DOING!**
		fhicl::Atom<size_t> end_of_run_timeout_ms{fhicl::Name{"end_of_run_quiet_timeout_ms"}, fhicl::Comment{"Amount of time (in ms) to wait for no new requests when a Stop transition is pending"}, 1000};
		/// "request_increment" (Default: 1) : Expected increment of sequence ID between each request
		fhicl::Atom<artdaq::Fragment::sequence_id_t> request_increment{fhicl::Name{"request_increment"}, fhicl::Comment{"Expected increment of sequence ID between each request"}, 1};
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
		 */
	RequestReceiver(const fhicl::ParameterSet& ps);
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
	/// Send an acknowledgement for the given RequestMessage
	/// </summary>
	/// <param name="message">Message to acknowledge</param>
	void sendAcknowledgement(detail::RequestMessage message);

	/// <summary>
	/// Get the current requests
	/// </summary>
	/// <returns>Map relating sequence IDs to timestamps</returns>
	std::map<artdaq::Fragment::sequence_id_t, artdaq::detail::RequestPacket> GetRequests() const
	{
		std::unique_lock<std::mutex> lk(request_mutex_);
		std::map<artdaq::Fragment::sequence_id_t, artdaq::detail::RequestPacket> out;
		for (auto& in : requests_)
		{
			out[in.second.first.sequence_id] = in.second.first;
		}
		return out;
	}

	/// <summary>
	/// Remove the request with the given sequence ID from the request map
	/// </summary>
	/// <param name="reqID">Request ID to remove</param>
	void RemoveRequest(artdaq::Fragment::sequence_id_t reqID);

	/// <summary>
	/// Determine if the RequestReceiver is receiving requests
	/// </summary>
	/// <returns>True if the request receiver is running</returns>
	bool isRunning() { return running_; }

	/// <summary>
	/// Clear all requests from the map
	/// </summary>
	void ClearRequests()
	{
		std::unique_lock<std::mutex> lk(request_mutex_);
		requests_.clear();
	}

	/// <summary>
	/// Get the current requests, then clear the map
	/// </summary>
	/// <returns>Map relating sequence IDs to timestamps</returns>
	std::map<artdaq::Fragment::sequence_id_t, artdaq::detail::RequestPacket> GetAndClearRequests()
	{
		std::unique_lock<std::mutex> lk(request_mutex_);
		std::map<artdaq::Fragment::sequence_id_t, artdaq::detail::RequestPacket> out;
		for (auto& in : requests_)
		{
			out[in.second.first.sequence_id] = in.second.first;
		}
		if (requests_.size()) { highest_seen_request_ = requests_.rbegin()->first; }
		out_of_order_requests_.clear();
		requests_.clear();
		return out;
	}

	/// <summary>
	/// Get the number of requests currently stored in the RequestReceiver
	/// </summary>
	/// <returns>The number of requests stored in the RequestReceiver</returns>
	size_t size()
	{
		std::unique_lock<std::mutex> tlk(request_mutex_);
		return requests_.size();
	}

	/// <summary>
	/// Wait for a new request message, up to the timeout given
	/// </summary>
	/// <param name="timeout_ms">Milliseconds to wait for a new request to arrive</param>
	/// <returns>True if any requests are present in the request map</returns>
	bool WaitForRequests(int timeout_ms)
	{
		std::unique_lock<std::mutex> lk(request_mutex_);  // Lock needed by wait_for
		// See if we have to wait at all
		if (requests_.size() > 0) return true;
		// If we do have to wait, check requests_.size to make sure we're not being notified spuriously
		return request_cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [this]() { return requests_.size() > 0; });
	}

	/// <summary>
	/// Get the time a given request was received
	/// </summary>
	/// <param name="reqID">Request ID of the request</param>
	/// <returns>steady_clock::time_point corresponding to when the request was received</returns>
	std::chrono::steady_clock::time_point GetRequestTime(artdaq::Fragment::sequence_id_t reqID)
	{
		std::unique_lock<std::mutex> lk(request_mutex_);
		return requests_.count(reqID) ? requests_[reqID].second : std::chrono::steady_clock::now();
	}

	/// <summary>
	/// Sets the current run number
	/// </summary>
	/// <param name="run">The current run number</param>
	void SetRunNumber(uint32_t run) { run_number_ = run; }

private:
	// FHiCL-configurable variables. Note that the C++ variable names
	// are the FHiCL variable names with a "_" appended
	int request_port_;
	std::string request_addr_;
	std::string multicast_out_addr_;
	int ack_port_;
	std::string ack_address_;
	struct sockaddr_in ack_addr_;
	bool running_;
	uint32_t run_number_;

	//Socket parameters
	int request_socket_;
	int ack_socket_;
	std::map<artdaq::Fragment::sequence_id_t, std::pair<artdaq::detail::RequestPacket, std::chrono::steady_clock::time_point>> requests_;
	std::atomic<bool> request_stop_requested_;
	std::chrono::steady_clock::time_point request_stop_timeout_;
	std::atomic<bool> request_received_;
	size_t end_of_run_timeout_ms_;
	std::atomic<bool> should_stop_;
	mutable std::mutex request_mutex_;
	mutable std::mutex state_mutex_;
	std::condition_variable request_cv_;
	boost::thread requestThread_;

	std::atomic<artdaq::Fragment::sequence_id_t> highest_seen_request_;
	std::set<artdaq::Fragment::sequence_id_t> out_of_order_requests_;
	artdaq::Fragment::sequence_id_t request_increment_;
};
}  // namespace artdaq

#endif  //ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH
