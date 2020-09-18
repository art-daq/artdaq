#ifndef artdaq_DAQrate_TokenSender_hh
#define artdaq_DAQrate_TokenSender_hh

#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/DAQdata/Globals.hh"  // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#include "artdaq/DAQrate/detail/RequestMessage.hh"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Table.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <chrono>
#include <cstdint>
#include <future>
#include <map>
#include <memory>

namespace artdaq {

/**
	 * \brief The TokenSender contains methods used to send data requests and Routing tokens
	 */
class TokenSender
{
public:
	/// <summary>
	/// Configuration for Routing token sending
	///
	/// This configuration should be the same for all processes sending routing tokens to a given RoutingManager.
	/// </summary>
	struct Config
	{
		/// "use_routing_manager" (Default: false) : Whether to send tokens to a RoutingManager
		fhicl::Atom<bool> use_routing_manager{fhicl::Name{"use_routing_manager"}, fhicl::Comment{"True if using the Routing Manager"}, false};
		/// "routing_token_port" (Default: 35555) : Port to send tokens on
		fhicl::Atom<int> routing_token_port{fhicl::Name{"routing_token_port"}, fhicl::Comment{"Port to send tokens on"}, 35555};
		/// "routing_manager_hostname" (Default: "localhost") : Hostname or IP of RoutingManager
		fhicl::Atom<std::string> routing_token_host{fhicl::Name{"routing_manager_hostname"}, fhicl::Comment{"Hostname or IP of RoutingManager"}, "localhost"};
	};
	/// Used for ParameterSet validation (if desired)
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

	TokenSender(TokenSender&&) = delete;             ///< Move Constructor is deleted
	TokenSender& operator=(TokenSender&&) = delete;  ///< Move-assignment operator is deleted

	/**
		 * \brief TokenSender Constructor
		 * \param pset ParameterSet used to configured TokenSender. See artdaq::TokenSender::Config
		 */
	explicit TokenSender(const fhicl::ParameterSet& pset);
	/**
		 * \brief TokenSender Destructor
		 */
	virtual ~TokenSender();

	/**
		 * \brief Send a RoutingToken message indicating that slots are available
		 * \param nSlots Number of slots available
		 * \param run_number Run number for token
		 * \param rank Rank of token
		 */
	void SendRoutingToken(int nSlots, int run_number, int rank = my_rank);

	/**
		 * \brief Get the count of number of tokens sent
		 * \return The number of tokens sent by TokenSender
		 */
	size_t GetSentTokenCount() const { return tokens_sent_.load(); }

	/**
		 * \brief Set the run number to be used in request messages
		 * \param run Run number
		 */
	void SetRunNumber(uint32_t run) { run_number_ = run; }

	/**
	 * \brief Determine if routing token sends are enabled
	 * \return If routing tokens will be sent by this TokenSender
	 */
	bool RoutingTokenSendsEnabled() { return send_routing_tokens_; }

private:
	std::atomic<bool> initialized_;

	bool send_routing_tokens_;
	int token_port_;
	int token_socket_;
	std::string token_address_;
	std::atomic<size_t> tokens_sent_;
	uint32_t run_number_;

private:

	void setup_tokens_();

	void send_routing_token_(int nSlots, int run_number, int rank);
};
}  // namespace artdaq
#endif /* artdaq_DAQrate_TokenSender_hh */
