#ifndef artdaq_DAQrate_RequestSender_hh
#define artdaq_DAQrate_RequestSender_hh

#include "artdaq/DAQdata/Globals.hh" // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/DAQrate/detail/RequestMessage.hh"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Table.h"

#include <map>
#include <memory>
#include <chrono>
#include <future>
#include <stdint.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>

namespace artdaq
{

	/**
	 * \brief The RequestSender contains methods used to send data requests and Routing tokens
	 */
	class RequestSender
	{
	public:
		struct RoutingTokenConfig
		{
			fhicl::Atom<bool> use_routing_master{ fhicl::Name{ "use_routing_master" }, fhicl::Comment{ "True if using the Routing Master" }, false };
			fhicl::Atom<int> routing_token_port{ fhicl::Name{ "routing_token_port" },fhicl::Comment{ "Port to send tokens on" },35555 };
			fhicl::Atom<std::string> routing_token_host{ fhicl::Name{ "routing_master_hostname" }, fhicl::Comment{ "Hostname or IP of RoutingMaster" },"localhost" };
		};

		struct Config
		{
			fhicl::Atom<bool> send_requests{ fhicl::Name{ "send_requests" }, fhicl::Comment{ "Enable sending Data Request messages" }, false };
			fhicl::Atom<int> request_port{ fhicl::Name{"request_port"}, fhicl::Comment{"Port to send DataRequests on"},3001 };
			fhicl::Atom<size_t> request_delay_ms{ fhicl::Name{"request_delay_ms"}, fhicl::Comment{"How long to wait before sending new DataRequests"}, 10 };
			fhicl::Atom<size_t> request_shutdown_timeout_us{ fhicl::Name{ "request_shutdown_timeout_us"},fhicl::Comment{"How long to wait for pending requests to be sent at shutdown"}, 100000 };
			fhicl::Atom<std::string> output_address{ fhicl::Name{ "multicast_interface_ip"}, fhicl::Comment{"Use this hostname for multicast output(to assign to the proper NIC)" }, "0.0.0.0" };
			fhicl::Atom<std::string> request_address{ fhicl::Name{"request_address"}, fhicl::Comment{ "Multicast address to send DataRequests to" }, "227.128.12.26" };
			fhicl::Table<RoutingTokenConfig> routing_token_config{ fhicl::Name{"routing_token_config"}, fhicl::Comment{"FHiCL table containing RoutingToken configuration"} };
		};
#if MESSAGEFACILITY_HEX_VERSION >= 0x20103
		using Parameters = fhicl::WrappedTable<Config>;
#endif

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
		 * \param pset ParameterSet used to configured RequestSender
		 *
		 * \verbatim
		 * RequestSender accepts the following Parameters:
		 * "send_requests" (Default: false): Whether to send DataRequests when new sequence IDs are seen
		 * "request_port" (Default: 3001): Port to send DataRequests on
		 * "request_delay_ms" (Default: 10): How long to wait before sending new DataRequests
		 * "request_shutdown_timeout_us" (Default: 100000 us): How long to wait for pending requests to be sent at shutdown
		 * "multicast_interface_ip" (Default: "0.0.0.0"): Use this hostname for multicast output (to assign to the proper NIC)
		 * "request_address" (Default: "227.128.12.26"): Multicast address to send DataRequests to
		 * "routing_token_config" (Default: Empty table): FHiCL table containing RoutingToken configuration
		 *   "use_routing_master" (Default: false): Whether to send tokens to a RoutingMaster
		 *   "routing_token_port" (Default: 35555): Port to send tokens on
		 *   "routing_master_hostname" (Default: "localhost"): Hostname or IP of RoutingMaster
		 * \endverbatim
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
		 */
		void AddRequest(Fragment::sequence_id_t seqID, Fragment::timestamp_t timestamp);

		/**
		 * \brief Remove a request from the request list
		 * \param seqID Sequence ID of request
		 */
		void RemoveRequest(Fragment::sequence_id_t seqID);

		/**
		 * \brief Send a RoutingToken message indicating that slots are available
		 * \param nSlots Number of slots available
		 */
		void SendRoutingToken(int nSlots);
	private:

		// Request stuff
		bool send_requests_;
		mutable std::mutex request_mutex_;
		mutable std::mutex request_send_mutex_;
		std::map<Fragment::sequence_id_t, Fragment::timestamp_t> active_requests_;
		int request_port_;
		size_t request_delay_;
		size_t request_shutdown_timeout_us_;
		int request_socket_;
		struct sockaddr_in request_addr_;
		std::string multicast_out_addr_;
		detail::RequestMessageMode request_mode_;

		bool send_routing_tokens_;
		int token_port_;
		int token_socket_;
		std::string token_address_;
		std::atomic<bool> request_sending_;

	private:
		void setup_requests_(std::string trigger_addr);

		void do_send_request_();

		void setup_tokens_();

		void send_routing_token_(int nSlots);
	};
}
#endif /* artdaq_DAQrate_RequestSender_hh */
