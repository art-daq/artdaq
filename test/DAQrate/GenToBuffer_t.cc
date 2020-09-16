#define TRACE_NAME "GenToBuffer"

#include <boost/program_options.hpp>
#include <boost/thread.hpp>
#include <thread>
#include "fhiclcpp/make_ParameterSet.h"
namespace bpo = boost::program_options;

#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/DAQrate/FragmentBuffer.hh"
#include "artdaq/DAQrate/RequestBuffer.hh"
#include "artdaq/Generators/CommandableFragmentGenerator.hh"
#include "artdaq/Generators/makeCommandableFragmentGenerator.hh"

namespace artdaq {
class GenToBufferTest
{
public:
	explicit GenToBufferTest(fhicl::ParameterSet ps)
	    : generator_ptr_(nullptr)
	    , fragment_buffer_ptr_(new FragmentBuffer(ps))
	    , request_buffer_ptr_(new RequestBuffer(1))
	    , fragment_count_(0)
	    , running_(false)
	{
		generator_ptr_ = makeCommandableFragmentGenerator(ps.get<std::string>("generator"), ps);
		generator_ptr_->SetRequestBuffer(request_buffer_ptr_);
		fragment_buffer_ptr_->SetRequestBuffer(request_buffer_ptr_);
	}

	void start(int run_number)
	{
		metricMan->do_start();
		request_buffer_ptr_->setRunning(true);
		generator_ptr_->StartCmd(run_number, 0, 0);

		running_ = true;
		boost::thread::attributes attrs;
		attrs.set_stack_size(4096 * 2000);  // 8 MB
		receive_thread_.reset(new boost::thread(attrs, boost::bind(&GenToBufferTest::receive_fragments, this)));
		send_thread_.reset(new boost::thread(attrs, boost::bind(&GenToBufferTest::send_fragments, this)));
	}
	void stop()
	{
		generator_ptr_->StopCmd(0, 0);
		fragment_buffer_ptr_->Stop();
		request_buffer_ptr_->setRunning(false);

		running_ = false;
		if (receive_thread_ && receive_thread_->joinable()) receive_thread_->join();
		if (send_thread_ && send_thread_->joinable()) send_thread_->join();
		metricMan->do_stop();
	}

	std::shared_ptr<RequestBuffer> GetRequestBuffer() { return request_buffer_ptr_; }

private:
	void receive_fragments()
	{
		TLOG(TLVL_DEBUG) << "Waiting for first fragment.";
		artdaq::FragmentPtrs frags;

		bool active = true;

		while (active && running_)
		{
			auto loop_start = std::chrono::steady_clock::now();
			TLOG(18) << "receive_fragments getNext start";
			active = generator_ptr_->getNext(frags);
			TLOG(18) << "receive_fragments getNext done (active=" << active << ")";
			auto after_getnext = std::chrono::steady_clock::now();
			// 08-May-2015, KAB & JCF: if the generator getNext() method returns false
			// (which indicates that the data flow has stopped) *and* the reason that
			// it has stopped is because there was an exception that wasn't handled by
			// the experiment-specific FragmentGenerator class, we move to the
			// InRunError state so that external observers (e.g. RunControl or
			// DAQInterface) can see that there was a problem.
			if (!active && generator_ptr_ && generator_ptr_->exception())
			{
				TLOG(TLVL_ERROR) << "Generator has an exception, aborting!";
				break;
			}

			if (!active) { break; }

			if (frags.size() > 0)
			{
				metricMan->sendMetric("Fragments Generated", frags.size(), "fragments", 3, artdaq::MetricMode::Accumulate | artdaq::MetricMode::Rate | artdaq::MetricMode::Average);

				TLOG(18) << "receive_fragments AddFragmentsToBuffer start";
				fragment_buffer_ptr_->AddFragmentsToBuffer(std::move(frags));
				TLOG(18) << "receive_fragments AddFragmentsToBuffer done";
				auto after_addFragsToBuffer = std::chrono::steady_clock::now();
				metricMan->sendMetric("FragmentBufferAddTime", artdaq::TimeUtils::GetElapsedTime(after_getnext, after_addFragsToBuffer), "s", 3, artdaq::MetricMode::Accumulate | artdaq::MetricMode::Average | artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum);
			}
			metricMan->sendMetric("GetNextTime", artdaq::TimeUtils::GetElapsedTime(loop_start, after_getnext), "s", 3, artdaq::MetricMode::Accumulate | artdaq::MetricMode::Average | artdaq::MetricMode::Minimum | artdaq::MetricMode::Maximum);

			frags.clear();
		}
		TLOG(TLVL_DEBUG) << "receive_fragments loop end";
	}

	void send_fragments()
	{
		TLOG(TLVL_DEBUG) << "Waiting for first fragment.";
		artdaq::FragmentPtrs frags;

		bool active = true;

		while (active && running_)
		{
			auto loop_start = std::chrono::steady_clock::now();

			TLOG(18) << "send_fragments applyRequests start";
			active = fragment_buffer_ptr_->applyRequests(frags);
			TLOG(18) << "send_fragments applyRequests done (active=" << active << ")";

			auto after_requests = std::chrono::steady_clock::now();
			if (!active) { break; }

			for (auto& fragPtr : frags)
			{
				if (!fragPtr.get())
				{
					TLOG(TLVL_WARNING) << "Encountered a bad fragment pointer in fragment " << fragment_count_ << ". "
					                   << "This is most likely caused by a problem with the Fragment Generator!";
					continue;
				}
				if (fragment_count_ == 0)
				{
					TLOG(TLVL_DEBUG) << "Received first Fragment from Fragment Generator, sequence ID " << fragPtr->sequenceID() << ", size = " << fragPtr->sizeBytes() << " bytes.";
				}
				artdaq::Fragment::sequence_id_t sequence_id = fragPtr->sequenceID();
				SetMFIteration("Sequence ID " + std::to_string(sequence_id));

				TLOG(17) << "send_fragments seq=" << sequence_id << " sendFragment start";
				++fragment_count_;

				// Turn on lvls (mem and/or slow) 3,13,14 to log every send.
				TLOG(((fragment_count_ == 1) ? TLVL_DEBUG
				                             : (((fragment_count_ % 250) == 0) ? 13 : 14)))
				    << ((fragment_count_ == 1)
				            ? "Sent first Fragment"
				            : "Sending fragment " + std::to_string(fragment_count_))
				    << " with SeqID " << sequence_id << ".";
			}

			metricMan->sendMetric("Fragments Discarded", frags.size(), "fragments", 3, artdaq::MetricMode::Accumulate | artdaq::MetricMode::Rate | artdaq::MetricMode::Average);
			frags.clear();
			auto after_frag_check = std::chrono::steady_clock::now();
			metricMan->sendMetric("ApplyRequestsTime", artdaq::TimeUtils::GetElapsedTime(loop_start, after_requests), "s", 3, artdaq::MetricMode::Average | artdaq::MetricMode::Accumulate | artdaq::MetricMode::Maximum | artdaq::MetricMode::Minimum);
			metricMan->sendMetric("FragmentDiscardTime", artdaq::TimeUtils::GetElapsedTime(after_requests, after_frag_check), "s", 3, artdaq::MetricMode::Average | artdaq::MetricMode::Accumulate);

			std::this_thread::yield();
		}

		// 11-May-2015, KAB: call MetricManager::do_stop whenever we exit the
		// processing fragments loop so that metrics correctly go to zero when
		// there is no data flowing
		metricMan->do_stop();

		TLOG(TLVL_DEBUG) << "send_fragments loop end";
	}

private:
	std::unique_ptr<CommandableFragmentGenerator> generator_ptr_;
	std::unique_ptr<FragmentBuffer> fragment_buffer_ptr_;
	std::shared_ptr<RequestBuffer> request_buffer_ptr_;
	std::atomic<size_t> fragment_count_;
	std::atomic<bool> running_;
	std::unique_ptr<boost::thread> receive_thread_;
	std::unique_ptr<boost::thread> send_thread_;
};
}  // namespace artdaq

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("RequestSender");

	struct FragmentReceiverConfig
	{
		fhicl::TableFragment<artdaq::CommandableFragmentGenerator::Config> generatorConfig;
		fhicl::TableFragment<artdaq::FragmentBuffer::Config> fragmentBufferConfig;
	};

	struct DAQConfig
	{
		fhicl::Table<FragmentReceiverConfig> frConfig{fhicl::Name{"fragment_receiver"}};
	};

	struct Config
	{
		fhicl::Table<DAQConfig> daq{fhicl::Name{"daq"}};
		fhicl::Table<artdaq::MetricManager::Config> metrics{fhicl::Name{"metrics"}};
		fhicl::Atom<double> test_duration_s{fhicl::Name{"test_duration_s"}, fhicl::Comment{"Duration, in seconds, for the test"}, 60.0};
		fhicl::Atom<size_t> time_between_requests_us{fhicl::Name{"time_between_requests_us"}, fhicl::Comment{"Amount of time to wait between generated requests, in us"}, 1000};
		fhicl::Atom<artdaq::Fragment::timestamp_t> timestamp_increment{fhicl::Name{"timestamp_increment"}, fhicl::Comment{"Amount to increment the timestamp for each request"}, 1};
		fhicl::Atom<int> run_number{fhicl::Name{"run_number"}, fhicl::Comment{"Run Number to use for the test"}, 101};
	};

	auto pset = LoadParameterSet<Config>(argc, argv, "GenToBuffer", "This test application evaluates the rate of Fragment Generation and Request Application.");
	auto fr_pset = pset;

	metricMan->initialize(pset.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet()), "GenToBuffer");

	if (pset.has_key("daq"))
	{
		fr_pset = pset.get<fhicl::ParameterSet>("daq");
	}

	if (fr_pset.has_key("fragment_receiver"))
	{
		fr_pset = fr_pset.get<fhicl::ParameterSet>("fragment_receiver");
	}
	artdaq::GenToBufferTest gtbt(fr_pset);

	auto buf = gtbt.GetRequestBuffer();

	auto start_time = std::chrono::steady_clock::now();
	auto duration = pset.get<double>("test_duration_s", 60);
	artdaq::Fragment::sequence_id_t seq = 0;
	artdaq::Fragment::timestamp_t timestamp = 1;
	auto time_between_requests_us = pset.get<size_t>("time_between_requests_us", 1000);
	auto timestamp_scale = pset.get<artdaq::Fragment::timestamp_t>("timestamp_increment", 1);

	gtbt.start(pset.get<int>("run_number", 101));

	while (artdaq::TimeUtils::GetElapsedTime(start_time) < duration)
	{
		buf->push(++seq, timestamp);
		timestamp += timestamp_scale;

		auto us_since_start = artdaq::TimeUtils::GetElapsedTimeMicroseconds(start_time);
		int64_t time_diff = seq * time_between_requests_us - us_since_start;
		TLOG(40) << "Time Diff: " << time_diff << ", Time since start: " << us_since_start << ", current epoch: " << seq * time_between_requests_us;
		if (time_diff > 10)
		{
			usleep(time_diff);
		}
	}

	gtbt.stop();
}