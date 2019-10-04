#ifndef artdaq_DAQrate_RequestSender_hh
#define artdaq_DAQrate_RequestSender_hh

#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/DAQdata/Globals.hh"  // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#include "artdaq/DAQrate/detail/RequestMessage.hh"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Table.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <chrono>
#include <future>
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
	/// Configuration for Routing token sending
	///
	/// This configuration should be the same for all processes sending routing tokens to a given RoutingMaster.
	/// </summary>
	struct RoutingTokenConfig
	{
		/// "use_routing_master" (Default: false) : Whether to send tokens to a RoutingMaster
		fhicl::Atom<bool> use_routing_master{fhicl::Name{"use_routing_master"}, fhicl::Comment{"True if using the Routing Master"}, false};
		/// "routing_token_port" (Default: 35555) : Port to send tokens on
		fhicl::Atom<int> routing_token_port{fhicl::Name{"routing_token_port"}, fhicl::Comment{"Port to send tokens on"}, 35555};
		/// "routing_master_hostname" (Default: "localhost") : Hostname or IP of RoutingMaster
		fhicl::Atom<std::string> routing_token_host{fhicl::Name{"routing_master_hostname"}, fhicl::Comment{"Hostname or IP of RoutingMaster"}, "localhost"};
	};

	/// <summary>
	/// Configuration of the RequestSender. May be used for parameter validation
	/// </summary>
	struct Config
	{
		/// "send_requests" (Default: false): Whether to send DataRequests when new sequence IDs are seen
		fhicl::Atom<bool> send_requests{fhicl::Name{"send_requests"}, fhicl::Comment{"Enable sending Data Request messages"}, false};
		/// "request_address" (Default: "227.128.12.26"): Multicast address to send DataRequests to
		fhicl::Atom<std::string> request_address{fhicl::Name{"request_address"}, fhicl::Comment{"Multicast address to send DataRequests to"}, "227.128.12.26"};
		/// "request_port" (Default: 3001): Port to send DataRequests on
		fhicl::Atom<int> request_port{fhicl::Name{"request_port"}, fhicl::Comment{"Port to send DataRequests on"}, 3001};
		/// "acknowledgement_port" (Default: 3002) : Port to listen for acknowledgements, if requested
		fhicl::Atom<int> acknowledgement_port{fhicl::Name{"acknowledgement_port"}, fhicl::Comment{"Port to listen for acknowledgements, if requested"}, 3002};
		/// "request_delay_ms" (Default: 10): How long to wait before sending new DataRequests
		fhicl::Atom<size_t> request_delay_ms{fhicl::Name{"request_delay_ms"}, fhicl::Comment{"How long to wait before sending new DataRequests"}, 10};
		/// "request_shutdown_timeout_us" (Default: 100000 us): How long to wait for pending requests to be sent at shutdown
		fhicl::Atom<size_t> request_shutdown_timeout_us{fhicl::Name{"request_shutdown_timeout_us"}, fhicl::Comment{"How long to wait for pending requests to be sent at shutdown"}, 100000};
		/// "multicast_interface_ip" (Default: "0.0.0.0"): Use this hostname for multicast output (to assign to the proper NIC)
		fhicl::Atom<std::string> output_address{fhicl::Name{"multicast_interface_ip"}, fhicl::Comment{"Use this hostname for multicast output(to assign to the proper NIC)"}, "0.0.0.0"};
		/// "request_timeout_s" (Default: -1): Amount of time, in seconds, to keep sending a request. Values <= 0 indicate no timeout.
		fhicl::Atom<double> request_timeout_s{fhicl::Name{"request_timeout_s"}, fhicl::Comment{"Amount of time, in seconds, to keep sending a request. Values <= 0 indicate no timeout."}, -1};
		/// "request_acknowledgements" (Default: false): Whether to request acknowledgements for requests sent
		fhicl::Atom<bool> request_acknowledgements{fhicl::Name{"request_acknowledgements"}, fhicl::Comment{"Whether to request acknowledgements for requests sent"}, false};
		/// "sender_ranks": List of ranks to expect acknowledgements from
		fhicl::Sequence<int> sender_ranks{fhicl::Name{"sender_ranks"}, fhicl::Comment{"List of ranks to expect acknowledgements from"}};
		fhicl::Table<RoutingTokenConfig> routing_token_config{fhicl::Name{"routing_token_config"}, fhicl::Comment{"FHiCL table containing RoutingToken configuration"}};  ///< Configuration for sending RoutingTokens. See artdaq::RequestSender::RoutingTokenConfig
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

	/**
		 * \brief RequestSender Constructor
		 * \param pset ParameterSet used to configured RequestSender. See artdaq::RequestSender::Config
		 */
	RequestSender(const fhicl::ParameterSet& pset);
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
		 * \param rank Destination rank for request. Default is -1, which will be replaced with my_rank
		 */
	void AddRequest(Fragment::sequence_id_t seqID, Fragment::timestamp_t timestamp, int rank = -1);

	/**
		 * \brief Remove a request from the request list
		 * \param seqID Sequence ID of request
		 */
	void RemoveRequest(Fragment::sequence_id_t seqID);

	/**
		 * \brief Send a RoutingToken message indicating that slots are available
		 * \param nSlots Number of slots available
		 * \param run_number Run number for token
		 */
	void SendRoutingToken(int nSlots, int run_number);

	/**
		 * \brief Get the count of number of tokens sent
		 * \return The number of tokens sent by RequestSender
		 */
	size_t GetSentTokenCount() const { return tokens_sent_.load(); }

	/**
		 * \brief Set the run number to be used in request messages
		 * \param run Run number
		 */
	void SetRunNumber(uint32_t run) { run_number_ = run; }

	/**
	 * \brief Start the Acknowledgement reception thread.
	 *
	 * If acknowledgements are being requested and the thread is not running when a request is sent, this method will be called automatically
	 */
	void StartAcknowledgementReception();
	/**
	 * \brief Stop the Acknowledgement reception thread.
	 *
	 * Automatically called in the RequestSender destructor
	 */
	void StopAcknowledgementReception();

	/**
	 * \brief Get the current active requests that RequestSender will send
	 * \return A map of requests, indexed by sequence ID
	 */
	std::map<Fragment::sequence_id_t, detail::RequestPacket> GetActiveRequests() const { return active_requests_; }

	/**
	 * \brief Clear completed requests from the active requests list
	 */
	void ClearCompletedRequests();

	/**
	 * \brief Get the number of acks received
	 * \return The number of acks received by this RequestSender
	 */
	size_t GetAckCount() const { return ack_messages_received_.load(); }

	/**
	 * \brief Get the number of requests sent
	 * \return The number of request messages sent by this RequestSender
	 */
	size_t GetRequestCount() const { return requests_sent_.load(); }

private:
	// Request stuff
	bool send_requests_;
	std::atomic<bool> initialized_;
	mutable std::mutex request_mutex_;
	mutable std::mutex request_send_mutex_;
	std::map<Fragment::sequence_id_t, detail::RequestPacket> active_requests_;
	std::string request_address_;
	int request_port_;
	int ack_port_;
	size_t request_delay_;
	size_t request_shutdown_timeout_us_;
	int request_socket_;
	int ack_socket_;
	struct sockaddr_in request_addr_;
	std::string multicast_out_addr_;
	detail::RequestMessageMode request_mode_;
	double request_timeout_s_;

	bool send_routing_tokens_;
	int token_port_;
	int token_socket_;

	std::string token_address_;
	std::atomic<int> request_sending_;
	std::atomic<size_t> requests_sent_;
	std::atomic<size_t> tokens_sent_;
	uint32_t run_number_;

	bool request_acknowledgements_;
	std::atomic<bool> receive_acknowledgements_enabled_;
	std::atomic<bool> ack_thread_running_;
	boost::thread ack_thread_;
	std::atomic<size_t> ack_messages_received_;
	std::set<int> sender_ranks_;

private:
	void setup_requests_();

	void do_send_request_();

	void setup_tokens_();

	void send_routing_token_(int nSlots, int run_number);

	void setup_acknowledgements_();

	void receive_acknowledgements_();
};
}  // namespace artdaq
#endif /* artdaq_DAQrate_RequestSender_hh */
