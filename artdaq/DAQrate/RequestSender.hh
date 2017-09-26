#ifndef artdaq_DAQrate_RequestSender_hh
#define artdaq_DAQrate_RequestSender_hh

#include "artdaq/DAQdata/Globals.hh" // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/DAQrate/detail/RequestMessage.hh"
#include "fhiclcpp/ParameterSet.h"

#include <map>
#include <memory>
#include <chrono>
//#include <thread>
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
		 * "request_shutdown_timeout_us" (Default: 0.1s): How long to wait for pending requests to be sent at shutdown
		 * "output_address" (Default: "localhost"): Use this hostname for multicast output (to assign to the proper NIC)
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
		struct sockaddr_in token_addr_;
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
