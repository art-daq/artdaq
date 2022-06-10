#ifndef ARTDAQ_DAQRATE_TOKEN_RECEIVER_HH
#define ARTDAQ_DAQRATE_TOKEN_RECEIVER_HH

#include "artdaq/DAQrate/StatisticsHelper.hh"
#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"

#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/Comment.h"
#include "fhiclcpp/types/ConfigurationTable.h"
#include "fhiclcpp/types/Name.h"

#include <boost/thread.hpp>

#include <atomic>
#include <sys/epoll.h>
#include <map>
#include <unordered_map>
#include <string>
#include <vector>

namespace artdaq {
/**
	 * \brief Receives event builder "free buffer" tokens and adds them to a specified RoutingPolicy.
	 */
class TokenReceiver
{
public:
	/**
		 * \brief Configuration of the TokenReceiver. May be used for parameter validation.
		 */
	struct Config
	{
		/// "routing_token_port" (Default: 35555) : Port on which routing tokens will be received
		fhicl::Atom<int> routing_token_port{fhicl::Name{"routing_token_port"}, fhicl::Comment{"Port to listen for routing tokens on"}, 355555};
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
		 * \brief TokenReceiver Constructor 
		 * \param ps ParameterSet used to configure TokenReceiver. See artdaq::TokenReceiver::Config
		 * \param policy RoutingManagerPolicy that manages the received tokens
		 * \param update_interval_msec The amount of time to wait in epoll_wait for a new update to arrive
		 */
	explicit TokenReceiver(const fhicl::ParameterSet& ps, std::shared_ptr<RoutingManagerPolicy> policy,
	                       size_t update_interval_msec);

	/**
	 * \brief TokenReceiver Destructor
	 */
	virtual ~TokenReceiver();

	/**
	 * \brief Starts the reception of event builder tokens
	 */
	void startTokenReception();

	/**
	 * \brief Temporarily suspends the reception of event builder tokens
	 */
	void pauseTokenReception() { reception_is_paused_ = true; }

	/**
	 * \brief Resumes the reception of event builder tokens after a suspension
	 */
	void resumeTokenReception() { reception_is_paused_ = false; }

	/**
	 * \brief Stops the reception of event builder tokens
	 * \param force Whether to suppress any error messages (used if called from destructor)
	 */
	void stopTokenReception(bool force = false);

	/**
	 * \brief Specifies a StatisticsHelper instance to use when gathering statistics
	 * \param helper A shared pointer to the StatisticsHelper instance
	 * \param stat_key Name to use for gathering statistics on tokens received
	 */
	void setStatsHelper(std::shared_ptr<StatisticsHelper> const& helper, std::string const& stat_key)
	{
		statsHelperPtr_ = helper;
		tokens_received_stat_key_ = stat_key;
	}

	/**
	 * \brief Sets the current run number
	 * \param run The current run number
	 */
	void setRunNumber(uint32_t run) { run_number_ = run; }

	/**
	 * \brief Returns the number of tokens that have been received
	 * \return The number of tokens that have been received since the most recent start
	 */
	size_t getReceivedTokenCount() const { return received_token_count_; }

private:
	TokenReceiver(TokenReceiver const&) = delete;
	TokenReceiver(TokenReceiver&&) = delete;
	TokenReceiver& operator=(TokenReceiver const&) = delete;
	TokenReceiver& operator=(TokenReceiver&&) = delete;

	void receiveTokensLoop_();

	int token_port_;
	std::shared_ptr<RoutingManagerPolicy> policy_;
	size_t update_interval_msec_;

	int token_socket_{-1};
	std::vector<epoll_event> receive_token_events_;
	std::unordered_map<int, std::string> receive_token_addrs_;
	int token_epoll_fd_{-1};

	boost::thread token_thread_;
	std::atomic<bool> thread_is_running_;
	std::atomic<bool> reception_is_paused_;
	std::atomic<bool> shutdown_requested_;
	std::atomic<uint32_t> run_number_;

	std::atomic<size_t> received_token_count_;
	std::unordered_map<int, size_t> received_token_counter_;
	std::shared_ptr<StatisticsHelper> statsHelperPtr_;
	std::string tokens_received_stat_key_;
};
}  // namespace artdaq

#endif  //ARTDAQ_DAQRATE_TOKEN_RECEIVER_HH
