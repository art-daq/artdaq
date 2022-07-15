#ifndef artdaq_DAQrate_RequestSender_hh
#define artdaq_DAQrate_RequestSender_hh

#include "artdaq/DAQrate/detail/RequestMessage.hh"

#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/Comment.h"
#include "fhiclcpp/types/ConfigurationTable.h"
#include "fhiclcpp/types/Name.h"
#include "fhiclcpp/types/Table.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>

namespace artdaq {

/**
	 * \brief The RequestSender contains methods used to send data requests and Routing tokens
	 */
class RequestSender
{
public:
	/// <summary>
	/// Configuration of the RequestSender. May be used for parameter validation
	/// </summary>
	struct Config
	{
		/// "send_requests" (Default: false): Whether to send DataRequests when new sequence IDs are seen
		fhicl::Atom<bool> send_requests{fhicl::Name{"send_requests"}, fhicl::Comment{"Enable sending Data Request messages"}, false};
		/// "request_port" (Default: 3001): Port to send DataRequests on
		fhicl::Atom<int> request_port{fhicl::Name{"request_port"}, fhicl::Comment{"Port to send DataRequests on"}, 3001};
		/// "request_delay_ms" (Default: 10): How long to wait before sending new DataRequests
		fhicl::Atom<size_t> request_delay_ms{fhicl::Name{"request_delay_ms"}, fhicl::Comment{"How long to wait before sending new DataRequests"}, 10};
		/// "request_shutdown_timeout_us" (Default: 100000 us): How long to wait for pending requests to be sent at shutdown
		fhicl::Atom<size_t> request_shutdown_timeout_us{fhicl::Name{"request_shutdown_timeout_us"}, fhicl::Comment{"How long to wait for pending requests to be sent at shutdown"}, 100000};
		/// "multicast_interface_ip" (Default: "0.0.0.0"): Use this hostname for multicast output (to assign to the proper NIC)
		fhicl::Atom<std::string> output_address{fhicl::Name{"multicast_interface_ip"}, fhicl::Comment{"Use this hostname for multicast output(to assign to the proper NIC)"}, "0.0.0.0"};
		/// "request_address" (Default: "227.128.12.26"): Multicast address to send DataRequests to
		fhicl::Atom<std::string> request_address{fhicl::Name{"request_address"}, fhicl::Comment{"Multicast address to send DataRequests to"}, "227.128.12.26"};
		/// "min_request_interval_ms" (Default: 500): Minimum time between automatic sends (ignored in EndOfRun RequetsMode)
		fhicl::Atom<size_t> min_request_interval_ms{fhicl::Name{"min_request_interval_ms"}, fhicl::Comment{"Minimum time between automatic sends (ignored in EndOfRun RequetsMode)"}, 100};
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
		 * \brief Default Constructor is deleted
		 */
	RequestSender() = delete;

	/**
		 * \brief Copy Constructor is deleted
		 */
	RequestSender(RequestSender const&) = delete;

	/**
		 * \brief Copy Assignment operator is deleted
		 * \return RequestSender copy
		 */
	RequestSender& operator=(RequestSender const&) = delete;

	RequestSender(RequestSender&&) = delete;             ///< Move Constructor is deleted
	RequestSender& operator=(RequestSender&&) = delete;  ///< Move-assignment operator is deleted

	/**
		 * \brief RequestSender Constructor
		 * \param pset ParameterSet used to configured RequestSender. See artdaq::RequestSender::Config
		 */
	explicit RequestSender(const fhicl::ParameterSet& pset);
	/**
		 * \brief RequestSender Destructor
		 */
	virtual ~RequestSender();

	/**
		 * \brief Set the mode for RequestMessages. Used to indicate when RequestSender should enter "EndOfRun" mode
		 * \param mode Mode to set
		 */
	void SetRequestMode(detail::RequestMessageMode mode);

	/**
		 * \brief Get the mode for RequestMessages.
		 * \return Current RequestMessageMode of the RequestSender
		 */
	detail::RequestMessageMode GetRequestMode() const { return request_mode_; }

	/**
		 * \brief Send a request message containing all current requests
		 * \param endOfRunOnly Whether the request should only be sent in EndOfRun RequestMessageMode (default: false)
		 */
	void SendRequest(bool endOfRunOnly = false);

	/**
		 * \brief Add a request to the request list
		 * \param seqID Sequence ID for request
		 * \param timestamp Timestamp to request
		 */
	void AddRequest(Fragment::sequence_id_t seqID, Fragment::timestamp_t timestamp);

	/**
		 * \brief Remove a request from the request list
		 * \param seqID Sequence ID of request
		 */
	void RemoveRequest(Fragment::sequence_id_t seqID);

	/**
		 * \brief Set the run number to be used in request messages
		 * \param run Run number
		 */
	void SetRunNumber(uint32_t run) { run_number_ = run; }

	/**
	 * \brief Determine if the RequestSender is currently sending any requests
	 * \return True if RequestSender has requests to send
	 *
	 * This function is used for testing
	 */
	bool RequestsInFlight() { return request_sending_.load() != 0; }

private:
private:
	// Request stuff
	bool send_requests_;
	std::atomic<bool> initialized_;
	mutable std::mutex request_mutex_;
	mutable std::mutex request_send_mutex_;
	std::map<Fragment::sequence_id_t, Fragment::timestamp_t> active_requests_;
	std::string request_address_;
	int request_port_;
	size_t request_delay_;
	size_t request_shutdown_timeout_us_;
	int request_socket_;
	struct sockaddr_in request_addr_;
	std::string multicast_out_addr_;
	detail::RequestMessageMode request_mode_;
	std::chrono::steady_clock::time_point last_request_send_time_;
	size_t min_request_interval_ms_;

	std::atomic<int> request_sending_;
	uint32_t run_number_;

private:
	void setup_requests_();

	void do_send_request_();
};
}  // namespace artdaq
#endif /* artdaq_DAQrate_RequestSender_hh */
