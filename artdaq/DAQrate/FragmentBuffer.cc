#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_FragmentBuffer").c_str()  // include these 2 first -

#include "artdaq/DAQrate/FragmentBuffer.hh"

#include <boost/exception/all.hpp>
#include <boost/throw_exception.hpp>

#include <iterator>
#include <limits>

#include "canvas/Utilities/Exception.h"
#include "cetlib_except/exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq-core/Utilities/SimpleLookupPolicy.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"

#include <sys/poll.h>
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include "artdaq/DAQdata/TCPConnect.hh"

#define TLVL_ADDFRAGMENT 32
#define TLVL_CHECKSTOP 33
#define TLVL_WAITFORBUFFERREADY 34
#define TLVL_GETBUFFERSTATS 35
#define TLVL_CHECKDATABUFFER 36
#define TLVL_APPLYREQUESTS 37
#define TLVL_APPLYREQUESTS_VERBOSE 38
#define TLVL_SENDEMPTYFRAGMENTS 39
#define TLVL_CHECKWINDOWS 40
#define TLVL_EMPTYFRAGMENT 41

artdaq::FragmentBuffer::FragmentBuffer(const fhicl::ParameterSet& ps)
    : requestBuffer_()
    , bufferModeKeepLatest_(ps.get<bool>("buffer_mode_keep_latest", false))
    , windowOffset_(ps.get<Fragment::timestamp_t>("request_window_offset", 0))
    , windowWidth_(ps.get<Fragment::timestamp_t>("request_window_width", 0))
    , staleFragmentTimeout_(ps.get<Fragment::timestamp_t>("stale_fragment_timeout", 0))
    , expectedType_(ps.get<Fragment::type_t>("expected_fragment_type", Fragment::type_t(Fragment::EmptyFragmentType)))
    , uniqueWindows_(ps.get<bool>("request_windows_are_unique", true))
    , sendMissingFragments_(ps.get<bool>("send_missing_request_fragments", true))
    , missing_request_window_timeout_us_(ps.get<size_t>("missing_request_window_timeout_us", 5000000))
    , window_close_timeout_us_(ps.get<size_t>("window_close_timeout_us", 2000000))
    , error_on_empty_(ps.get<bool>("error_on_empty_fragment", false))
    , circularDataBufferMode_(ps.get<bool>("circular_buffer_mode", false))
    , maxDataBufferDepthFragments_(ps.get<int>("data_buffer_depth_fragments", 1000))
    , maxDataBufferDepthBytes_(ps.get<size_t>("data_buffer_depth_mb", 1000) * 1024 * 1024)
    , systemFragmentCount_(0)
    , should_stop_(false)
{
	auto fragment_ids = ps.get<std::vector<artdaq::Fragment::fragment_id_t>>("fragment_ids", std::vector<artdaq::Fragment::fragment_id_t>());

	TLOG(TLVL_DEBUG + 33) << "artdaq::FragmentBuffer::FragmentBuffer(ps)";
	int fragment_id = ps.get<int>("fragment_id", -99);

	if (fragment_id != -99)
	{
		if (fragment_ids.size() != 0)
		{
			auto report = "Error in FragmentBuffer: can't both define \"fragment_id\" and \"fragment_ids\" in FHiCL document";
			TLOG(TLVL_ERROR) << report;
			throw cet::exception("FragmentBufferConfig") << report;
		}
		else
		{
			fragment_ids.emplace_back(fragment_id);
		}
	}

	for (auto& id : fragment_ids)
	{
		dataBuffers_[id] = std::make_shared<DataBuffer>();
		dataBuffers_[id]->DataBufferDepthBytes = 0;
		dataBuffers_[id]->DataBufferDepthFragments = 0;
		dataBuffers_[id]->BufferFragmentKept = false;
	}

	std::string modeString = ps.get<std::string>("request_mode", "ignored");
	if (modeString == "single" || modeString == "Single")
	{
		mode_ = RequestMode::Single;
	}
	else if (modeString.find("buffer") != std::string::npos || modeString.find("Buffer") != std::string::npos)
	{
		mode_ = RequestMode::Buffer;
	}
	else if (modeString == "window" || modeString == "Window")
	{
		mode_ = RequestMode::Window;
	}
	else if (modeString.find("ignore") != std::string::npos || modeString.find("Ignore") != std::string::npos)
	{
		mode_ = RequestMode::Ignored;
	}
	else if (modeString.find("sequence") != std::string::npos || modeString.find("Sequence") != std::string::npos)
	{
		mode_ = RequestMode::SequenceID;
	}
	if (mode_ != RequestMode::Ignored && !ps.get<bool>("receive_requests", false))
	{
		TLOG(TLVL_WARNING) << "Request Mode was requested as " << modeString << ", but is being set to Ignored because \"receive_requests\" was not set to true";
		mode_ = RequestMode::Ignored;
	}
	TLOG(TLVL_DEBUG + 32) << "Request mode is " << printMode_();
}

artdaq::FragmentBuffer::~FragmentBuffer()
{
	TLOG(TLVL_INFO) << "Fragment Buffer Destructor; Clearing data buffers";
	Reset(true);
}

void artdaq::FragmentBuffer::Reset(bool stop)
{
	should_stop_ = stop;
	for (auto& id : dataBuffers_)
	{
		std::lock_guard<std::mutex> dlk(id.second->DataBufferMutex);
		id.second->DataBufferDepthBytes = 0;
		id.second->DataBufferDepthFragments = 0;
		id.second->BufferFragmentKept = false;
		id.second->DataBuffer.clear();
	}

	{
		std::lock_guard<std::mutex> lk(systemFragmentMutex_);
		systemFragments_.clear();
		systemFragmentCount_ = 0;
	}
}

void artdaq::FragmentBuffer::AddFragmentsToBuffer(FragmentPtrs frags)
{
	std::unordered_map<Fragment::fragment_id_t, FragmentPtrs> frags_by_id;
	while (!frags.empty())
	{
		auto dataIter = frags.begin();
		auto frag_id = (*dataIter)->fragmentID();

		if (Fragment::isBroadcastFragmentType((*dataIter)->type()))
		{
			std::lock_guard<std::mutex> lk(systemFragmentMutex_);
			systemFragments_.emplace_back(std::move(*dataIter));
			systemFragmentCount_++;
			frags.erase(dataIter);
			continue;
		}

		if (!dataBuffers_.count(frag_id))
		{
			TLOG(TLVL_ERROR) << "Received Fragment with Fragment ID " << frag_id << ", which is not in the declared Fragment IDs list!";
			throw cet::exception("FragmentIDs") << "Received Fragment with Fragment ID " << frag_id << ", which is not in the declared Fragment IDs list!";
		}

		frags_by_id[frag_id].emplace_back(std::move(*dataIter));
		frags.erase(dataIter);
	}

	auto type_it = frags_by_id.begin();
	while (type_it != frags_by_id.end())
	{
		auto frag_id = type_it->first;

		waitForDataBufferReady(frag_id);
		auto dataBuffer = dataBuffers_[frag_id];
		std::lock_guard<std::mutex> dlk(dataBuffer->DataBufferMutex);
		switch (mode_)
		{
			case RequestMode::Single: {
				auto dataIter = type_it->second.rbegin();
				TLOG(TLVL_ADDFRAGMENT) << "Adding Fragment with Fragment ID " << frag_id << ", Sequence ID " << (*dataIter)->sequenceID() << ", and Timestamp " << (*dataIter)->timestamp() << " to buffer";
				dataBuffer->DataBuffer.clear();
				dataBuffer->DataBufferDepthBytes = (*dataIter)->sizeBytes();
				dataBuffer->DataBuffer.emplace_back(std::move(*dataIter));
				dataBuffer->DataBufferDepthFragments = 1;
				type_it->second.clear();
			}
			break;
			case RequestMode::Buffer:
			case RequestMode::Ignored:
			case RequestMode::Window:
			case RequestMode::SequenceID:
			default:
				while (!type_it->second.empty())
				{
					auto dataIter = type_it->second.begin();
					TLOG(TLVL_ADDFRAGMENT) << "Adding Fragment with Fragment ID " << frag_id << ", Sequence ID " << (*dataIter)->sequenceID() << ", and Timestamp " << (*dataIter)->timestamp() << " to buffer";

					dataBuffer->DataBufferDepthBytes += (*dataIter)->sizeBytes();
					dataBuffer->DataBuffer.emplace_back(std::move(*dataIter));
					type_it->second.erase(dataIter);
				}
				dataBuffer->DataBufferDepthFragments = dataBuffer->DataBuffer.size();
				break;
		}
		getDataBufferStats(frag_id);
		++type_it;
	}
	dataCondition_.notify_all();
}

bool artdaq::FragmentBuffer::check_stop()
{
	TLOG(TLVL_CHECKSTOP) << "CFG::check_stop: should_stop=" << should_stop_.load();

	if (!should_stop_.load()) return false;
	if (mode_ == RequestMode::Ignored)
	{
		return true;
	}

	if (requestBuffer_ != nullptr)
	{
		// check_stop returns true if the CFG should stop. We should wait for the Request Buffer to report Request Receiver stopped before stopping.
		TLOG(TLVL_DEBUG + 32) << "should_stop is true, requestBuffer_->isRunning() is " << std::boolalpha << requestBuffer_->isRunning();
		if (!requestBuffer_->isRunning())
		{
			return true;
		}
	}
	return false;
}

std::string artdaq::FragmentBuffer::printMode_()
{
	switch (mode_)
	{
		case RequestMode::Single:
			return "Single";
		case RequestMode::Buffer:
			return "Buffer";
		case RequestMode::Window:
			return "Window";
		case RequestMode::Ignored:
			return "Ignored";
		case RequestMode::SequenceID:
			return "SequenceID";
	}

	return "ERROR";
}

size_t artdaq::FragmentBuffer::dataBufferFragmentCount_()
{
	size_t count = 0;
	for (auto& id : dataBuffers_) count += id.second->DataBufferDepthFragments;
	count += systemFragmentCount_.load();
	return count;
}

bool artdaq::FragmentBuffer::waitForDataBufferReady(Fragment::fragment_id_t id)
{
	if (!dataBuffers_.count(id))
	{
		TLOG(TLVL_ERROR) << "DataBufferError: "
		                 << "Error in FragmentBuffer: Cannot wait for data buffer for ID " << id << " because it does not exist!";
		throw cet::exception("DataBufferError") << "Error in FragmentBuffer: Cannot wait for data buffer for ID " << id << " because it does not exist!";
	}
	auto startwait = std::chrono::steady_clock::now();
	auto first = true;
	auto lastwaittime = 0ULL;
	auto dataBuffer = dataBuffers_[id];

	while (dataBufferIsTooLarge(id))
	{
		if (!circularDataBufferMode_)
		{
			if (should_stop_.load())
			{
				TLOG(TLVL_DEBUG + 32) << "Run ended while waiting for buffer to shrink!";
				getDataBufferStats(id);
				dataCondition_.notify_all();
				return false;
			}
			auto waittime = TimeUtils::GetElapsedTimeMilliseconds(startwait);

			if (first || (waittime != lastwaittime && waittime % 1000 == 0))
			{
				std::lock_guard<std::mutex> lk(dataBuffer->DataBufferMutex);
				if (dataBufferIsTooLarge(id))
				{
					TLOG(TLVL_WARNING) << "Bad Omen: Data Buffer has exceeded its size limits. "
					                   << "(frag_id=" << id
					                   << ", frags=" << dataBuffer->DataBufferDepthFragments << "/" << maxDataBufferDepthFragments_
					                   << ", szB=" << dataBuffer->DataBufferDepthBytes << "/" << maxDataBufferDepthBytes_ << ")"
					                   << ", timestamps=" << dataBuffer->DataBuffer.front()->timestamp() << "-" << dataBuffer->DataBuffer.back()->timestamp();
					TLOG(TLVL_DEBUG + 33) << "Bad Omen: Possible causes include requests not getting through or Ignored-mode BR issues";

					if (metricMan)
					{
						metricMan->sendMetric("Bad Omen wait time", waittime / 1000.0, "s", 1, MetricMode::LastPoint);
					}
				}
				first = false;
			}
			if (waittime % 5 && waittime != lastwaittime)
			{
				TLOG(TLVL_WAITFORBUFFERREADY) << "getDataLoop: Data Retreival paused for " << waittime << " ms waiting for data buffer to drain";
			}
			lastwaittime = waittime;
			usleep(1000);
		}
		else
		{
			std::lock_guard<std::mutex> lk(dataBuffer->DataBufferMutex);
			if (dataBufferIsTooLarge(id))
			{
				auto begin = dataBuffer->DataBuffer.begin();
				if (begin == dataBuffer->DataBuffer.end())
				{
					TLOG(TLVL_WARNING) << "Data buffer is reported as too large, but doesn't contain any Fragments! Possible corrupt memory!";
					continue;
				}
				if (*begin)
				{
					TLOG(TLVL_WAITFORBUFFERREADY) << "waitForDataBufferReady: Dropping Fragment with timestamp " << (*begin)->timestamp() << " from data buffer (Buffer over-size, circular data buffer mode)";

					dataBuffer->DataBufferDepthBytes -= (*begin)->sizeBytes();
					dataBuffer->DataBuffer.erase(begin);
					dataBuffer->DataBufferDepthFragments = dataBuffer->DataBuffer.size();
					dataBuffer->BufferFragmentKept = false;  // If any Fragments are removed from data buffer, then we know we don't have to ignore the first one anymore
				}
			}
		}
	}
	return true;
}

bool artdaq::FragmentBuffer::dataBufferIsTooLarge(Fragment::fragment_id_t id)
{
	if (!dataBuffers_.count(id))
	{
		TLOG(TLVL_ERROR) << "DataBufferError: "
		                 << "Error in FragmentBuffer: Cannot check size of data buffer for ID " << id << " because it does not exist!";
		throw cet::exception("DataBufferError") << "Error in FragmentBuffer: Cannot check size of data buffer for ID " << id << " because it does not exist!";
	}
	auto dataBuffer = dataBuffers_[id];
	return (maxDataBufferDepthFragments_ > 0 && dataBuffer->DataBufferDepthFragments.load() > maxDataBufferDepthFragments_) ||
	       (maxDataBufferDepthBytes_ > 0 && dataBuffer->DataBufferDepthBytes.load() > maxDataBufferDepthBytes_);
}

void artdaq::FragmentBuffer::getDataBufferStats(Fragment::fragment_id_t id)
{
	if (!dataBuffers_.count(id))
	{
		TLOG(TLVL_ERROR) << "DataBufferError: "
		                 << "Error in FragmentBuffer: Cannot get stats of data buffer for ID " << id << " because it does not exist!";
		throw cet::exception("DataBufferError") << "Error in FragmentBuffer: Cannot get stats of data buffer for ID " << id << " because it does not exist!";
	}
	auto dataBuffer = dataBuffers_[id];

	if (metricMan)
	{
		TLOG(TLVL_GETBUFFERSTATS) << "getDataBufferStats: Sending Metrics";
		metricMan->sendMetric("Buffer Depth Fragments", dataBuffer->DataBufferDepthFragments.load(), "fragments", 1, MetricMode::LastPoint);
		metricMan->sendMetric("Buffer Depth Bytes", dataBuffer->DataBufferDepthBytes.load(), "bytes", 1, MetricMode::LastPoint);

		auto bufferDepthFragmentsPercent = dataBuffer->DataBufferDepthFragments.load() * 100 / static_cast<double>(maxDataBufferDepthFragments_);
		auto bufferDepthBytesPercent = dataBuffer->DataBufferDepthBytes.load() * 100 / static_cast<double>(maxDataBufferDepthBytes_);
		metricMan->sendMetric("Fragment Buffer Full %Fragments", bufferDepthFragmentsPercent, "%", 3, MetricMode::LastPoint);
		metricMan->sendMetric("Fragment Buffer Full %Bytes", bufferDepthBytesPercent, "%", 3, MetricMode::LastPoint);
		metricMan->sendMetric("Fragment Buffer Full %", bufferDepthFragmentsPercent > bufferDepthBytesPercent ? bufferDepthFragmentsPercent : bufferDepthBytesPercent, "%", 1, MetricMode::LastPoint);
	}
	TLOG(TLVL_GETBUFFERSTATS) << "getDataBufferStats: frags=" << dataBuffer->DataBufferDepthFragments.load() << "/" << maxDataBufferDepthFragments_
	                          << ", sz=" << dataBuffer->DataBufferDepthBytes.load() << "/" << maxDataBufferDepthBytes_;
}

std::string artdaq::FragmentBuffer::getStatReport()
{
	std::ostringstream oss;

	for (auto& it : dataBuffers_)
	{
		Fragment::fragment_id_t id = it.first;
		if (!dataBuffers_.count(id))
		{
			TLOG(TLVL_ERROR) << "DataBufferError: "
			                 << "Error in FragmentBuffer: Cannot get stats of data buffer for ID "
			                 << id << " because it does not exist!";
			throw cet::exception("DataBufferError") << "Error in FragmentBuffer: Cannot get stats of data buffer for ID "
			                                        << id << " because it does not exist!";
		}

		auto dataBuffer = dataBuffers_[id];

		TLOG(TLVL_GETBUFFERSTATS) << "getDataBufferStats: Sending Metrics";

		int nf = dataBuffer->DataBufferDepthFragments.load();
		int nb = dataBuffer->DataBufferDepthBytes.load();
		oss << std::endl
		    << "fragment_id:" << id << " nfragments:" << nf << " nbytes:" << nb
		    << " max_nf:" << maxDataBufferDepthFragments_ << " max_nb:" << maxDataBufferDepthBytes_;

		TLOG(TLVL_GETBUFFERSTATS) << "getDataBufferStats: frags=" << dataBuffer->DataBufferDepthFragments.load() << "/" << maxDataBufferDepthFragments_
		                          << ", sz=" << dataBuffer->DataBufferDepthBytes.load() << "/" << maxDataBufferDepthBytes_;
	}

	return oss.str();
}

void artdaq::FragmentBuffer::checkDataBuffer(Fragment::fragment_id_t id)
{
	if (!dataBuffers_.count(id))
	{
		TLOG(TLVL_ERROR) << "DataBufferError: "
		                 << "Error in FragmentBuffer: Cannot check data buffer for ID " << id << " because it does not exist!";
		throw cet::exception("DataBufferError") << "Error in FragmentBuffer: Cannot check data buffer for ID " << id << " because it does not exist!";
	}

	if (dataBuffers_[id]->DataBufferDepthFragments > 0 && mode_ != RequestMode::Single && mode_ != RequestMode::Ignored)
	{
		auto dataBuffer = dataBuffers_[id];
		std::lock_guard<std::mutex> lk(dataBuffer->DataBufferMutex);

		// Eliminate extra fragments
		while (dataBufferIsTooLarge(id))
		{
			auto begin = dataBuffer->DataBuffer.begin();
			TLOG(TLVL_CHECKDATABUFFER) << "checkDataBuffer: Dropping Fragment with timestamp " << (*begin)->timestamp() << " from data buffer (Buffer over-size)";
			dataBuffer->DataBufferDepthBytes -= (*begin)->sizeBytes();
			dataBuffer->DataBuffer.erase(begin);
			dataBuffer->DataBufferDepthFragments = dataBuffer->DataBuffer.size();
			dataBuffer->BufferFragmentKept = false;  // If any Fragments are removed from data buffer, then we know we don't have to ignore the first one anymore
		}

		TLOG(TLVL_CHECKDATABUFFER) << "DataBufferDepthFragments is " << dataBuffer->DataBufferDepthFragments << ", DataBuffer.size is " << dataBuffer->DataBuffer.size();
		if (dataBuffer->DataBufferDepthFragments > 0 && staleFragmentTimeout_ > 0)
		{
			TLOG(TLVL_CHECKDATABUFFER) << "Determining if Fragments can be dropped from data buffer";
			Fragment::timestamp_t last = dataBuffer->DataBuffer.back()->timestamp();
			Fragment::timestamp_t min = last > staleFragmentTimeout_ ? last - staleFragmentTimeout_ : 0;
			for (auto it = dataBuffer->DataBuffer.begin(); it != dataBuffer->DataBuffer.end();)
			{
				if ((*it)->timestamp() < min)
				{
					TLOG(TLVL_CHECKDATABUFFER) << "checkDataBuffer: Dropping Fragment with timestamp " << (*it)->timestamp() << " from data buffer (timeout=" << staleFragmentTimeout_ << ", min=" << min << ")";
					dataBuffer->DataBufferDepthBytes -= (*it)->sizeBytes();
					dataBuffer->BufferFragmentKept = false;  // If any Fragments are removed from data buffer, then we know we don't have to ignore the first one anymore
					it = dataBuffer->DataBuffer.erase(it);
					dataBuffer->DataBufferDepthFragments = dataBuffer->DataBuffer.size();
				}
				else
				{
					break;
				}
			}
		}
	}
}

void artdaq::FragmentBuffer::applyRequestsIgnoredMode(artdaq::FragmentPtrs& frags)
{
	// dataBuffersMutex_ is held by calling function
	// We just copy everything that's here into the output.
	TLOG(TLVL_APPLYREQUESTS) << "Mode is Ignored; Copying data to output";
	for (auto& id : dataBuffers_)
	{
		std::lock_guard<std::mutex> lk(id.second->DataBufferMutex);
		std::move(id.second->DataBuffer.begin(), id.second->DataBuffer.end(), std::inserter(frags, frags.end()));
		id.second->DataBufferDepthBytes = 0;
		id.second->DataBufferDepthFragments = 0;
		id.second->BufferFragmentKept = false;
		id.second->DataBuffer.clear();
	}
}

void artdaq::FragmentBuffer::applyRequestsSingleMode(artdaq::FragmentPtrs& frags)
{
	// We only care about the latest request received. Send empties for all others.
	auto requests = requestBuffer_->GetAndClearRequests();
	if (requests.size() == 0) return;

	auto last_request = requests.rbegin();

	for (auto& id : dataBuffers_)
	{
		for (auto& request : requests)
		{
			if (request.first != last_request->first)
			{
				sendEmptyFragment(frags, request.first, id.first, "Single Mode stale request");
			}
		}
		std::lock_guard<std::mutex> lk(id.second->DataBufferMutex);
		if (id.second->DataBufferDepthFragments > 0)
		{
			assert(id.second->DataBufferDepthFragments == 1);
			TLOG(TLVL_APPLYREQUESTS) << "Mode is Single; Sending copy of most recent Fragment (timestamp=" << (*id.second->DataBuffer.begin())->timestamp() << ")";
			for (auto& fragptr : id.second->DataBuffer)
			{
				// Return the latest data point
				auto frag = fragptr.get();
				auto newfrag = std::unique_ptr<artdaq::Fragment>(new Fragment(last_request->first, frag->fragmentID(), frag->type()));
				newfrag->resize(frag->size() - detail::RawFragmentHeader::num_words());
				memcpy(newfrag->headerAddress(), frag->headerAddress(), frag->sizeBytes());
				newfrag->setTimestamp(last_request->second);
				newfrag->setSequenceID(last_request->first);
				frags.push_back(std::move(newfrag));
			}
		}
		else
		{
			sendEmptyFragment(frags, last_request->first, id.first, "No data for");
		}
	}
}

void artdaq::FragmentBuffer::applyRequestsBufferMode(artdaq::FragmentPtrs& frags)
{
	// We only care about the latest request received. Send empties for all others.
	auto requests = requestBuffer_->GetAndClearRequests();
	if (requests.size() == 0) return;

	auto last_request = requests.rbegin();

	for (auto& id : dataBuffers_)
	{
		for (auto& request : requests)
		{
			if (request.first != last_request->first)
			{
				sendEmptyFragment(frags, request.first, id.first, "Buffer Mode stale request");
			}
		}

		TLOG(TLVL_APPLYREQUESTS) << "applyRequestsBufferMode: Creating ContainerFragment for Buffered Fragments (SeqID " << last_request->first << ")";
		frags.emplace_back(new artdaq::Fragment(last_request->first, id.first, artdaq::Fragment::ContainerFragmentType));
		frags.back()->setTimestamp(last_request->second);
		ContainerFragmentLoader cfl(*frags.back());
		cfl.set_missing_data(false);  // Buffer mode is never missing data, even if there IS no data.

		// If we kept a Fragment from the previous iteration, but more data has arrived, discard it
		std::lock_guard<std::mutex> lk(id.second->DataBufferMutex);
		if (id.second->BufferFragmentKept && id.second->DataBufferDepthFragments > 1)
		{
			id.second->DataBufferDepthBytes -= id.second->DataBuffer.front()->sizeBytes();
			id.second->DataBuffer.erase(id.second->DataBuffer.begin());
			id.second->DataBufferDepthFragments = id.second->DataBuffer.size();
		}

		// Buffer mode TFGs should simply copy out the whole dataBuffer_ into a ContainerFragment
		FragmentPtrs fragsToAdd;
		std::move(id.second->DataBuffer.begin(), --id.second->DataBuffer.end(), std::back_inserter(fragsToAdd));
		id.second->DataBuffer.erase(id.second->DataBuffer.begin(), --id.second->DataBuffer.end());

		if (fragsToAdd.size() > 0)
		{
			TLOG(TLVL_APPLYREQUESTS) << "applyRequestsBufferMode: Adding " << fragsToAdd.size() << " Fragments to Container (SeqID " << last_request->first << ")";
			cfl.addFragments(fragsToAdd);
		}
		else
		{
			TLOG(TLVL_APPLYREQUESTS) << "applyRequestsBufferMode: No Fragments to add (SeqID " << last_request->first << ")";
		}

		if (id.second->DataBuffer.size() == 1)
		{
			TLOG(TLVL_APPLYREQUESTS) << "applyRequestsBufferMode: Adding Fragment with timestamp " << id.second->DataBuffer.front()->timestamp() << " to Container with sequence ID " << last_request->first;
			cfl.addFragment(id.second->DataBuffer.front());
			if (bufferModeKeepLatest_)
			{
				id.second->BufferFragmentKept = true;
				id.second->DataBufferDepthBytes = id.second->DataBuffer.front()->sizeBytes();
				id.second->DataBufferDepthFragments = id.second->DataBuffer.size();  // 1
			}
			else
			{
				id.second->DataBuffer.clear();
				id.second->BufferFragmentKept = false;
				id.second->DataBufferDepthBytes = 0;
				id.second->DataBufferDepthFragments = 0;
			}
		}
	}
}

void artdaq::FragmentBuffer::applyRequestsWindowMode_CheckAndFillDataBuffer(artdaq::FragmentPtrs& frags, artdaq::Fragment::fragment_id_t id, artdaq::Fragment::sequence_id_t seq, artdaq::Fragment::timestamp_t ts)
{
	auto dataBuffer = dataBuffers_[id];

	TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode_CheckAndFillDataBuffer: Checking that data exists for request window " << seq;
	Fragment::timestamp_t min = ts > windowOffset_ ? ts - windowOffset_ : 0;
	Fragment::timestamp_t max = ts + windowWidth_ > windowOffset_ ? ts + windowWidth_ - windowOffset_ : 1;

	TLOG(TLVL_APPLYREQUESTS) << "ApplyRequestsWindowsMode_CheckAndFillDataBuffer: min is " << min << ", max is " << max
	                         << " and first/last points in buffer are " << (dataBuffer->DataBufferDepthFragments > 0 ? dataBuffer->DataBuffer.front()->timestamp() : 0)
	                         << "/" << (dataBuffer->DataBufferDepthFragments > 0 ? dataBuffer->DataBuffer.back()->timestamp() : 0)
	                         << " (sz=" << dataBuffer->DataBufferDepthFragments << " [" << dataBuffer->DataBufferDepthBytes.load()
	                         << "/" << maxDataBufferDepthBytes_ << "])";
	bool windowClosed = dataBuffer->DataBufferDepthFragments > 0 && dataBuffer->DataBuffer.back()->timestamp() >= max;
	bool windowTimeout = !windowClosed && TimeUtils::GetElapsedTimeMicroseconds(requestBuffer_->GetRequestTime(seq)) > window_close_timeout_us_;
	if (windowTimeout)
	{
		TLOG(TLVL_WARNING) << "applyRequestsWindowMode_CheckAndFillDataBuffer: A timeout occurred waiting for data to close the request window ({" << min << "-" << max
		                   << "}, buffer={" << (dataBuffer->DataBufferDepthFragments > 0 ? dataBuffer->DataBuffer.front()->timestamp() : 0) << "-"
		                   << (dataBuffer->DataBufferDepthFragments > 0 ? dataBuffer->DataBuffer.back()->timestamp() : 0)
		                   << "} ). Time waiting: "
		                   << TimeUtils::GetElapsedTimeMicroseconds(requestBuffer_->GetRequestTime(seq)) << " us "
		                   << "(> " << window_close_timeout_us_ << " us).";
	}
	if (windowClosed || windowTimeout)
	{
		TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode_CheckAndFillDataBuffer: Creating ContainerFragment for Window-requested Fragments (SeqID " << seq << ")";
		frags.emplace_back(new artdaq::Fragment(seq, id, artdaq::Fragment::ContainerFragmentType));
		frags.back()->setTimestamp(ts);
		ContainerFragmentLoader cfl(*frags.back());

		// In the spirit of NOvA's MegaPool: (RS = Request start (min), RE = Request End (max))
		//  --- | Buffer Start | --- | Buffer End | ---
		// 1. RS RE |           |     |            |
		// 2. RS |              |  RE |            |
		// 3. RS |              |     |            | RE
		// 4.    |              | RS RE |          |
		// 5.    |              | RS  |            | RE
		// 6.    |              |     |            | RS RE
		//
		// If RE (or RS) is after the end of the buffer, we wait for window_close_timeout_us_. If we're here, then that means that windowClosed is false, and the missing_data flag should be set.
		// If RS (or RE) is before the start of the buffer, then missing_data should be set to true, as data is assumed to arrive in the buffer in timestamp order
		// If the dataBuffer has size 0, then windowClosed will be false
		if (!windowClosed || (dataBuffer->DataBufferDepthFragments > 0 && dataBuffer->DataBuffer.front()->timestamp() > min))
		{
			TLOG(TLVL_DEBUG + 32) << "applyRequestsWindowMode_CheckAndFillDataBuffer: Request window starts before and/or ends after the current data buffer, setting ContainerFragment's missing_data flag!"
			                      << " (requestWindowRange=[" << min << "," << max << "], "
			                      << "buffer={" << (dataBuffer->DataBufferDepthFragments > 0 ? dataBuffer->DataBuffer.front()->timestamp() : 0) << "-"
			                      << (dataBuffer->DataBufferDepthFragments > 0 ? dataBuffer->DataBuffer.back()->timestamp() : 0) << "} (SeqID " << seq << ")";
			cfl.set_missing_data(true);
		}

		auto it = dataBuffer->DataBuffer.begin();
		// Likely that it will be closer to the end...
		if (windowTimeout)
		{
			it = dataBuffer->DataBuffer.end();
			--it;
			while (it != dataBuffer->DataBuffer.begin())
			{
				if ((*it)->timestamp() < min)
				{
					break;
				}
				--it;
			}
		}

		FragmentPtrs fragsToAdd;
		// Do a little bit more work to decide which fragments to send for a given request
		for (; it != dataBuffer->DataBuffer.end();)
		{
			Fragment::timestamp_t fragT = (*it)->timestamp();
			if (fragT < min)
			{
				++it;
				continue;
			}
			if (fragT > max || (fragT == max && windowWidth_ > 0))
			{
				break;
			}

			TLOG(TLVL_APPLYREQUESTS_VERBOSE) << "applyRequestsWindowMode_CheckAndFillDataBuffer: Adding Fragment with timestamp " << (*it)->timestamp() << " to Container (SeqID " << seq << ")";
			if (uniqueWindows_)
			{
				dataBuffer->DataBufferDepthBytes -= (*it)->sizeBytes();
				fragsToAdd.emplace_back(std::move(*it));
				it = dataBuffer->DataBuffer.erase(it);
			}
			else
			{
				fragsToAdd.emplace_back(it->get());
				++it;
			}
		}

		if (fragsToAdd.size() > 0)
		{
			TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode_CheckAndFillDataBuffer: Adding " << fragsToAdd.size() << " Fragments to Container (SeqID " << seq << ")";
			cfl.addFragments(fragsToAdd);

			// Don't delete Fragments which are still in the Fragment buffer
			if (!uniqueWindows_)
			{
				for (auto& frag : fragsToAdd)
				{
					frag.release();
				}
			}
			fragsToAdd.clear();
		}
		else
		{
			TLOG(error_on_empty_ ? TLVL_ERROR : TLVL_APPLYREQUESTS) << "applyRequestsWindowMode_CheckAndFillDataBuffer: No Fragments match request (SeqID " << seq << ", window " << min << " - " << max << ")";
		}

		dataBuffer->DataBufferDepthFragments = dataBuffer->DataBuffer.size();
		dataBuffer->WindowsSent.insert(seq);
	}
}

void artdaq::FragmentBuffer::applyRequestsWindowMode(artdaq::FragmentPtrs& frags)
{
	TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode BEGIN";

	auto requests = requestBuffer_->GetRequests();

	TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode: Starting request processing for " << requests.size() << " requests";
	for (auto req = requests.begin(); req != requests.end();)
	{
		TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode: processing request with sequence ID " << req->first << ", timestamp " << req->second;

		auto ts = req->second;
		if (ts == Fragment::InvalidTimestamp)
		{
			TLOG(TLVL_ERROR) << "applyRequestsWindowMode: Received InvalidTimestamp in request " << req->first << ", cannot apply! Check that push-mode BRs are filling appropriate timestamps in their Fragments!";
			req = requests.erase(req);
			continue;
		}

		for (auto& id : dataBuffers_)
		{
			std::lock_guard<std::mutex> lk(id.second->DataBufferMutex);
			if (!id.second->WindowsSent.count(req->first))
			{
				applyRequestsWindowMode_CheckAndFillDataBuffer(frags, id.first, req->first, req->second);
			}
		}
		checkSentWindows(req->first);
		++req;
	}

	// Check sent windows for requests that can be removed
	std::set<artdaq::Fragment::sequence_id_t> seqs;
	for (auto& id : dataBuffers_)
	{
		std::lock_guard<std::mutex> lk(id.second->DataBufferMutex);
		for (auto& seq : id.second->WindowsSent)
		{
			seqs.insert(seq);
		}
	}
	for (auto& seq : seqs)
	{
		checkSentWindows(seq);
	}
}

void artdaq::FragmentBuffer::applyRequestsSequenceIDMode(artdaq::FragmentPtrs& frags)
{
	TLOG(TLVL_APPLYREQUESTS) << "applyRequestsSequenceIDMode BEGIN";

	auto requests = requestBuffer_->GetRequests();

	TLOG(TLVL_APPLYREQUESTS) << "applyRequestsSequenceIDMode: Starting request processing";
	for (auto req = requests.begin(); req != requests.end();)
	{
		TLOG(TLVL_APPLYREQUESTS) << "applyRequestsSequenceIDMode: Checking that data exists for request SequenceID " << req->first;

		for (auto& id : dataBuffers_)
		{
			std::lock_guard<std::mutex> lk(id.second->DataBufferMutex);
			if (!id.second->WindowsSent.count(req->first))
			{
				TLOG(TLVL_APPLYREQUESTS_VERBOSE) << "Searching id " << id.first << " for Fragments with Sequence ID " << req->first;
				for (auto it = id.second->DataBuffer.begin(); it != id.second->DataBuffer.end();)
				{
					auto seq = (*it)->sequenceID();
					TLOG(TLVL_APPLYREQUESTS_VERBOSE) << "applyRequestsSequenceIDMode: Fragment SeqID " << seq << ", request ID " << req->first;
					if (seq == req->first)
					{
						TLOG(TLVL_APPLYREQUESTS_VERBOSE) << "applyRequestsSequenceIDMode: Adding Fragment to output";
						id.second->WindowsSent.insert(req->first);
						id.second->DataBufferDepthBytes -= (*it)->sizeBytes();
						frags.push_back(std::move(*it));
						it = id.second->DataBuffer.erase(it);
						id.second->DataBufferDepthFragments = id.second->DataBuffer.size();
					}
					else
					{
						++it;
					}
				}
			}
		}
		checkSentWindows(req->first);
		++req;
	}

	// Check sent windows for requests that can be removed
	std::set<artdaq::Fragment::sequence_id_t> seqs;
	for (auto& id : dataBuffers_)
	{
		std::lock_guard<std::mutex> lk(id.second->DataBufferMutex);
		for (auto& seq : id.second->WindowsSent)
		{
			seqs.insert(seq);
		}
	}
	for (auto& seq : seqs)
	{
		checkSentWindows(seq);
	}
}

bool artdaq::FragmentBuffer::applyRequests(artdaq::FragmentPtrs& frags)
{
	if (check_stop())
	{
		return false;
	}

	// Wait for data, if in ignored mode, or a request otherwise
	if (mode_ == RequestMode::Ignored)
	{
		auto start_time = std::chrono::steady_clock::now();
		while (dataBufferFragmentCount_() == 0 && TimeUtils::GetElapsedTime(start_time) < 1.0)
		{
			if (check_stop()) return false;
			std::unique_lock<std::mutex> lock(dataConditionMutex_);
			dataCondition_.wait_for(lock, std::chrono::milliseconds(10), [this]() { return dataBufferFragmentCount_() > 0; });
		}
	}
	else if (requestBuffer_ == nullptr)
	{
		TLOG(TLVL_ERROR) << "Request Buffer must be set (via SetRequestBuffer) before applyRequests/getData can be called!";
		return false;
	}
	else
	{
		if ((check_stop() && requestBuffer_->size() == 0)) return false;

		std::unique_lock<std::mutex> lock(dataConditionMutex_);
		dataCondition_.wait_for(lock, std::chrono::milliseconds(10));

		checkDataBuffers();

		// Wait up to 1000 ms for a request...
		auto counter = 0;

		while (requestBuffer_->size() == 0 && counter < 100)
		{
			if (check_stop()) return false;

			checkDataBuffers();

			requestBuffer_->WaitForRequests(10);  // milliseconds
			counter++;
		}
	}

	if (systemFragmentCount_.load() > 0)
	{
		std::lock_guard<std::mutex> lk(systemFragmentMutex_);
		TLOG(TLVL_INFO) << "Copying " << systemFragmentCount_.load() << " System Fragments into output";

		std::move(systemFragments_.begin(), systemFragments_.end(), std::inserter(frags, frags.end()));
		systemFragments_.clear();
		systemFragmentCount_ = 0;
	}

	switch (mode_)
	{
		case RequestMode::Single:
			applyRequestsSingleMode(frags);
			break;
		case RequestMode::Window:
			applyRequestsWindowMode(frags);
			break;
		case RequestMode::Buffer:
			applyRequestsBufferMode(frags);
			break;
		case RequestMode::SequenceID:
			applyRequestsSequenceIDMode(frags);
			break;
		case RequestMode::Ignored:
		default:
			applyRequestsIgnoredMode(frags);
			break;
	}

	getDataBuffersStats();

	auto stale_requests = requestBuffer_->GetRequests(std::chrono::steady_clock::now() - std::chrono::microseconds(missing_request_window_timeout_us_));
	for (auto& req : stale_requests)
	{
		TLOG(TLVL_CHECKWINDOWS) << "checkSentWindows: Request " << req.first << " has been in the request buffer for more than " << missing_request_window_timeout_us_ << " us, removing from requestBuffer_.";
		requestBuffer_->RemoveRequest(req.first);
		for (auto& id : dataBuffers_)
		{
			std::lock_guard<std::mutex> lk(id.second->DataBufferMutex);
			if (id.second->WindowsSent.count(req.first))
			{
				id.second->WindowsSent.erase(req.first);
			}
			else
			{
				sendEmptyFragment(frags, req.first, id.first, "Request timeout");
			}
		}
	}

	if (frags.size() > 0)
		TLOG(TLVL_APPLYREQUESTS) << "Finished Processing requests, returning " << frags.size() << " fragments";
	return true;
}

bool artdaq::FragmentBuffer::sendEmptyFragment(artdaq::FragmentPtrs& frags, size_t seqId, Fragment::fragment_id_t fragmentId, std::string desc)
{
	TLOG(TLVL_EMPTYFRAGMENT) << desc << " sequence ID " << seqId << ", sending empty fragment";
	auto frag = new Fragment();
	frag->setSequenceID(seqId);
	frag->setFragmentID(fragmentId);
	frag->setSystemType(Fragment::EmptyFragmentType);
	frags.emplace_back(FragmentPtr(frag));
	return true;
}

void artdaq::FragmentBuffer::checkSentWindows(artdaq::Fragment::sequence_id_t seq)
{
	TLOG(TLVL_CHECKWINDOWS) << "checkSentWindows: Checking if request " << seq << " can be removed from request list";
	bool seqComplete = true;
	for (auto& id : dataBuffers_)
	{
		std::lock_guard<std::mutex> lk(id.second->DataBufferMutex);
		if (!id.second->WindowsSent.count(seq))
		{
			seqComplete = false;
		}
	}
	if (seqComplete)
	{
		TLOG(TLVL_CHECKWINDOWS) << "checkSentWindows: Request " << seq << " is complete, removing from requestBuffer_.";
		requestBuffer_->RemoveRequest(seq);

		for (auto& id : dataBuffers_)
		{
			std::lock_guard<std::mutex> lk(id.second->DataBufferMutex);
			id.second->WindowsSent.erase(seq);
		}
	}
}
