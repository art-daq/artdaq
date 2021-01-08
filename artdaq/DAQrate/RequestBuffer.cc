#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_RequestBuffer").c_str()  // include these 2 first -

#include "artdaq/DAQrate/RequestBuffer.hh"

artdaq::RequestBuffer::RequestBuffer(artdaq::Fragment::sequence_id_t request_increment)

    : requests_()
    , request_timing_()
    , highest_seen_request_(0)
    , last_next_request_(0)
    , out_of_order_requests_()
    , request_increment_(request_increment)
    , receiver_running_(false)
{
}

artdaq::RequestBuffer::~RequestBuffer() {}

void artdaq::RequestBuffer::push(artdaq::Fragment::sequence_id_t seq, artdaq::Fragment::timestamp_t ts)
{
	std::lock_guard<std::mutex> tlk(request_mutex_);
	if (requests_.count(seq) && requests_[seq] != ts)
	{
		TLOG(TLVL_ERROR) << "Received conflicting request for SeqID "
		                 << seq << "!"
		                 << " Old ts=" << requests_[seq]
		                 << ", new ts=" << ts << ". Keeping OLD!";
	}
	else if (!requests_.count(seq))
	{
		int delta = seq - highest_seen_request_;
		TLOG(11) << "Received request for sequence ID " << seq
		         << " and timestamp " << ts << " (delta: " << delta << ")";
		if (delta <= 0 || out_of_order_requests_.count(seq))
		{
			TLOG(11) << "Already serviced this request ( sequence ID " << seq << ")! Ignoring...";
		}
		else
		{
			requests_[seq] = ts;
			request_timing_[seq] = std::chrono::steady_clock::now();
		}
	}
	request_cv_.notify_all();
}

void artdaq::RequestBuffer::reset()
{
	std::lock_guard<std::mutex> lk(request_mutex_);
	requests_.clear();
	request_timing_.clear();
	highest_seen_request_ = 0;
	last_next_request_ = 0;
	out_of_order_requests_.clear();
}

/// <summary>
/// Get the current requests
/// </summary>
/// <returns>Map relating sequence IDs to timestamps</returns>

std::map<artdaq::Fragment::sequence_id_t, artdaq::Fragment::timestamp_t> artdaq::RequestBuffer::GetRequests() const
{
	std::lock_guard<std::mutex> lk(request_mutex_);
	std::map<artdaq::Fragment::sequence_id_t, Fragment::timestamp_t> out;
	for (auto& in : requests_)
	{
		out[in.first] = in.second;
	}
	return out;
}

std::pair<artdaq::Fragment::sequence_id_t, artdaq::Fragment::timestamp_t> artdaq::RequestBuffer::GetNextRequest()
{
	std::lock_guard<std::mutex> lk(request_mutex_);

	auto it = requests_.begin();
	while (it != requests_.end() && it->first <= last_next_request_) { ++it; }

	if (it == requests_.end())
	{
		return std::make_pair<artdaq::Fragment::sequence_id_t, artdaq::Fragment::timestamp_t>(0, 0);
	}

	last_next_request_ = it->first;
	return *it;
}

void artdaq::RequestBuffer::RemoveRequest(artdaq::Fragment::sequence_id_t reqID)
{
	TLOG(10) << "RemoveRequest: Removing request for id " << reqID;
	std::lock_guard<std::mutex> lk(request_mutex_);
	requests_.erase(reqID);

	if (reqID > highest_seen_request_)
	{
		TLOG(10) << "RemoveRequest: out_of_order_requests_.size() == " << out_of_order_requests_.size() << ", reqID=" << reqID << ", expected=" << highest_seen_request_ + request_increment_;
		if (out_of_order_requests_.size() || reqID != highest_seen_request_ + request_increment_)
		{
			out_of_order_requests_.insert(reqID);

			auto it = out_of_order_requests_.begin();
			while (it != out_of_order_requests_.end())  // Stop accounting for requests after stop
			{
				if (*it == highest_seen_request_ + request_increment_)
				{
					highest_seen_request_ = *it;
					it = out_of_order_requests_.erase(it);
				}
				else
				{
					break;
				}
			}
		}
		else  // no out-of-order requests and this request is highest seen + request_increment_
		{
			highest_seen_request_ = reqID;
		}
		TLOG(10) << "RemoveRequest: reqID=" << reqID << " Setting highest_seen_request_ to " << highest_seen_request_;
	}
	if (metricMan && request_timing_.count(reqID))
	{
		metricMan->sendMetric("Request Response Time", TimeUtils::GetElapsedTime(request_timing_[reqID]), "seconds", 2, MetricMode::Average);
	}
	request_timing_.erase(reqID);
}

/// <summary>
/// Clear all requests from the map
/// </summary>

void artdaq::RequestBuffer::ClearRequests()
{
	std::lock_guard<std::mutex> lk(request_mutex_);
	requests_.clear();
}

/// <summary>
/// Get the current requests, then clear the map
/// </summary>
/// <returns>Map relating sequence IDs to timestamps</returns>

std::map<artdaq::Fragment::sequence_id_t, artdaq::Fragment::timestamp_t> artdaq::RequestBuffer::GetAndClearRequests()
{
	std::lock_guard<std::mutex> lk(request_mutex_);
	std::map<artdaq::Fragment::sequence_id_t, Fragment::timestamp_t> out;
	for (auto& in : requests_)
	{
		out[in.first] = in.second;
	}
	if (requests_.size()) { highest_seen_request_ = requests_.rbegin()->first; }
	out_of_order_requests_.clear();
	requests_.clear();
	request_timing_.clear();
	return out;
}

/// <summary>
/// Get the number of requests currently stored in the RequestReceiver
/// </summary>
/// <returns>The number of requests stored in the RequestReceiver</returns>

size_t artdaq::RequestBuffer::size()
{
	std::lock_guard<std::mutex> tlk(request_mutex_);
	return requests_.size();
}

/// <summary>
/// Wait for a new request message, up to the timeout given
/// </summary>
/// <param name="timeout_ms">Milliseconds to wait for a new request to arrive</param>
/// <returns>True if any requests are present in the request map</returns>

bool artdaq::RequestBuffer::WaitForRequests(int timeout_ms)
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

std::chrono::steady_clock::time_point artdaq::RequestBuffer::GetRequestTime(artdaq::Fragment::sequence_id_t reqID)
{
	std::lock_guard<std::mutex> lk(request_mutex_);
	return request_timing_.count(reqID) ? request_timing_[reqID] : std::chrono::steady_clock::now();
}
