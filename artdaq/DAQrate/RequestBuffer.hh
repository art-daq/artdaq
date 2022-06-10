#ifndef _artdaq_DAQrate_RequestBuffer_hh_
#define _artdaq_DAQrate_RequestBuffer_hh_

#include <condition_variable>
#include <map>
#include <set>

#include "artdaq-core/Data/Fragment.hh"

namespace artdaq {
/**
 * @brief Holds requests from RequestReceiver while they are being processed
*/
class RequestBuffer
{
public:
	/**
	 * @brief RequestBuffer Constructor
	 * @param request_increment Expected increase in request sequence ID each request
	*/
	explicit RequestBuffer(Fragment::sequence_id_t request_increment = 1);

	/**
	 * @brief RequestBuffer Destructor
	*/
	virtual ~RequestBuffer();

	/**
	 * @brief Add a Request to the buffer
	 * @param seq Sequence ID of the request
	 * @param ts Timestamp for the request
	*/
	void push(artdaq::Fragment::sequence_id_t seq, artdaq::Fragment::timestamp_t ts);

	/**
	 * @brief Reset RequestBuffer, discarding all requests and tracking information
	*/
	void reset();

	/// <summary>
	/// Get the current requests
	/// </summary>
	/// <returns>Map relating sequence IDs to timestamps</returns>
	std::map<artdaq::Fragment::sequence_id_t, artdaq::Fragment::timestamp_t> GetRequests() const;

	/// <summary>
	/// Get the "next" request, i.e. the first unsatisfied request that has not already been returned by GetNextRequest
	/// </summary>
	/// <returns>Request data for "next" request. Will return (0,0) if there is no "next" request</returns>
	///
	/// This function uses last_next_request_ to ensure that it does not return the same request more than once
	std::pair<artdaq::Fragment::sequence_id_t, artdaq::Fragment::timestamp_t> GetNextRequest();

	/// <summary>
	/// Remove the request with the given sequence ID from the request map
	/// </summary>
	/// <param name="reqID">Request ID to remove</param>
	void RemoveRequest(artdaq::Fragment::sequence_id_t reqID);

	/// <summary>
	/// Clear all requests from the map
	/// </summary>
	void ClearRequests();
	/// <summary>
	/// Get the current requests, then clear the map
	/// </summary>
	/// <returns>Map relating sequence IDs to timestamps</returns>
	std::map<artdaq::Fragment::sequence_id_t, artdaq::Fragment::timestamp_t> GetAndClearRequests();

	/// <summary>
	/// Get the number of requests currently stored in the RequestReceiver
	/// </summary>
	/// <returns>The number of requests stored in the RequestReceiver</returns>
	size_t size();
	/// <summary>
	/// Wait for a new request message, up to the timeout given
	/// </summary>
	/// <param name="timeout_ms">Milliseconds to wait for a new request to arrive</param>
	/// <returns>True if any requests are present in the request map</returns>
	bool WaitForRequests(int timeout_ms);

	/// <summary>
	/// Get the time a given request was received
	/// </summary>
	/// <param name="reqID">Request ID of the request</param>
	/// <returns>steady_clock::time_point corresponding to when the request was received</returns>
	std::chrono::steady_clock::time_point GetRequestTime(artdaq::Fragment::sequence_id_t reqID);

	/**
	 * @brief Determine whether the RequestBuffer is active
	 * @return 
	*/
	bool isRunning() const { return receiver_running_; }
	/**
	 * @brief Set whether the RequestBuffer is active
	 * @param running Whether the RequestBuffer is active
	*/
	void setRunning(bool running) { receiver_running_ = running; }

private:
	std::map<artdaq::Fragment::sequence_id_t, artdaq::Fragment::timestamp_t> requests_;
	std::map<artdaq::Fragment::sequence_id_t, std::chrono::steady_clock::time_point> request_timing_;
	std::atomic<artdaq::Fragment::sequence_id_t> highest_seen_request_;
	std::atomic<artdaq::Fragment::sequence_id_t> last_next_request_;  // The last request returned by GetNextRequest
	std::set<artdaq::Fragment::sequence_id_t> out_of_order_requests_;
	artdaq::Fragment::sequence_id_t request_increment_;
	mutable std::mutex request_mutex_;
	std::condition_variable request_cv_;

	std::atomic<bool> receiver_running_;
};
}  // namespace artdaq

#endif  // _artdaq_DAQrate_RequestBuffer_hh_