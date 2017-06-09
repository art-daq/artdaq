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
	 * \brief The RequestSender class collects Fragment objects, until it receives a complete
	 * event, at which point the event is handed over to the art thread
	 * \todo Make the art thread a separate process
	 * 
	 * An RequestSender is given Fragments, which it collects until it
	 * finds it has a complete RawEvent. When a complete RawEvent is
	 * assembled, the RequestSender puts it onto the global RawEvent queue.
	 * There should be only one RequestSender per process; an MPI program
	 * can thus have multiple RequestSenders. By construction, each
	 * RequestSender will only deal with events (and fragments) from a
	 * single run.
	 *
	 * The RequestSender is also responsible for starting the thread that
	 * will be popping events off the global queue. This is so that the
	 * RequestSender is guaranteed to live long enough to allow the global
	 * queue to be drained. The current implementation uses only a free
	 * function as the 'thread function' for this thread.
	 *
	 * A future enhancement of RequestSender may make it be able to move
	 * from handling run X to handling run Y; such an enhancement will
	 * have to include how to deal with any incomplete events in storage
	 * at the time of the introduction of the new run.
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
		void SetRequestMode(detail::RequestMessageMode mode) { request_mode_ = mode; }

		detail::RequestMessageMode GetRequestMode() { return request_mode_; }

		void SendRequest();

		void AddRequest(Fragment::sequence_id_t seqID, Fragment::timestamp_t timestamp);

		void RemoveRequest(Fragment::sequence_id_t seqID);

		void SendRoutingToken(int nSlots);
	private:
		
		// Request stuff
		bool send_requests_;
		mutable std::mutex request_mutex_;
		std::map<Fragment::sequence_id_t, Fragment::timestamp_t> active_requests_;
		int request_port_;
		size_t request_delay_;
		int request_socket_;
		struct sockaddr_in request_addr_;
		std::string multicast_out_addr_;
		detail::RequestMessageMode request_mode_;
		
		bool send_routing_tokens_;
		int token_port_;
		int token_socket_;
		struct sockaddr_in token_addr_;
		std::string token_address_;
		
	private:
		void setup_requests_(std::string trigger_addr);
		
		void do_send_request_();

		void setup_tokens_();

		void send_routing_token_(int nSlots);
	};
}
#endif /* artdaq_DAQrate_RequestSender_hh */
