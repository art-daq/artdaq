#ifndef artdaq_DAQrate_TokenSender_hh
#define artdaq_DAQrate_TokenSender_hh

#include "artdaq/DAQdata/Globals.hh" // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
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
	 * \brief The TokenSender contains methods used to send Routing tokens
	 */
	class TokenSender
	{
	public:
		/// <summary>
		/// Configuration for Routing token sending
		/// 
		/// This configuration should be the same for all processes sending routing tokens to a given RoutingMaster.
		/// </summary>
		struct Config
		{
			/// "use_routing_master" (Default: false) : Whether to send tokens to a RoutingMaster
			fhicl::Atom<bool> use_routing_master{ fhicl::Name{ "use_routing_master" }, fhicl::Comment{ "True if using the Routing Master" }, false };
			/// "routing_token_port" (Default: 35555) : Port to send tokens on
			fhicl::Atom<int> routing_token_port{ fhicl::Name{ "routing_token_port" },fhicl::Comment{ "Port to send tokens on" },35555 };
			/// "routing_master_hostname" (Default: "localhost") : Hostname or IP of RoutingMaster
			fhicl::Atom<std::string> routing_token_host{ fhicl::Name{ "routing_master_hostname" }, fhicl::Comment{ "Hostname or IP of RoutingMaster" },"localhost" };
		};
		using Parameters = fhicl::WrappedTable<Config>;

		/**
		 * \brief Default Constructor is deleted
		 */
		TokenSender() = delete;

		/**
		 * \brief Copy Constructor is deleted
		 */
		TokenSender(TokenSender const&) = delete;

		/**
		 * \brief Copy Assignment operator is deleted
		 * \return TokenSender copy
		 */
		TokenSender& operator=(TokenSender const&) = delete;

		/**
		 * \brief TokenSender Constructor
		 * \param pset ParameterSet used to configured TokenSender. See artdaq::TokenSender::Config
		 */
		TokenSender(const fhicl::ParameterSet& pset);
		/**
		 * \brief TokenSender Destructor
		 */
		virtual ~TokenSender();

		/**
		 * \brief Send a RoutingToken message indicating that slots are available
		 * \param nSlots Number of slots available
		 */
		void SendRoutingToken(int nSlots);

	private:

		// Request stuff
		std::atomic<bool> initialized_;

		bool send_routing_tokens_;
		int token_port_;
		int token_socket_;
		std::string token_address_;
		std::atomic<int> request_sending_;

	private:
		void setup_tokens_();

		void send_routing_token_(int nSlots);
	};
}
#endif /* artdaq_DAQrate_TokenSender_hh */
