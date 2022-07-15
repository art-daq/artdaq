#define TRACE_NAME "FragmentBuffer_t"

#define BOOST_TEST_MODULE FragmentBuffer_t
#include <boost/test/unit_test.hpp>

#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/DAQrate/FragmentBuffer.hh"
#include "artdaq/DAQrate/detail/RequestSender.hh"

#include <thread>

#define MESSAGEFACILITY_DEBUG true

#define RATE_TEST_COUNT 100000
#define TRACE_REQUIRE_EQUAL(l, r)                                                                                                \
	do                                                                                                                           \
	{                                                                                                                            \
		if (l == r)                                                                                                              \
		{                                                                                                                        \
			TLOG(TLVL_DEBUG) << __LINE__ << ": Checking if " << #l << " (" << l << ") equals " << #r << " (" << r << ")...YES!"; \
		}                                                                                                                        \
		else                                                                                                                     \
		{                                                                                                                        \
			TLOG(TLVL_ERROR) << __LINE__ << ": Checking if " << #l << " (" << l << ") equals " << #r << " (" << r << ")...NO!";  \
		}                                                                                                                        \
		BOOST_REQUIRE_EQUAL(l, r);                                                                                               \
	} while (0)

namespace artdaqtest {
class FragmentBufferTestGenerator;
}

/**
 * \brief CommandableFragmentGenerator derived class for testing
 */
class artdaqtest::FragmentBufferTestGenerator
{
public:
	/**
	 * \brief FragmentBufferTestGenerator Constructor
	 */
	explicit FragmentBufferTestGenerator(const fhicl::ParameterSet& ps);

	/**
	 * @brief Generate Fragments
	 * @param n Number of Fragments to generate
	 * @param fragmentIds List of Fragment IDs to generate Fragments for (if different than configured fragment IDs)
	 * @return artdaq::FragmentPtrs containing generated Fragments
	*/
	artdaq::FragmentPtrs Generate(size_t n, std::vector<artdaq::Fragment::fragment_id_t> fragmentIds = std::vector<artdaq::Fragment::fragment_id_t>());

public:
	/**
	 * \brief Set the timestamp to be used for the next Fragment
	 * \param ts Timestamp to be used for the next Fragment
	 */
	void setTimestamp(artdaq::Fragment::timestamp_t ts) { ts_ = ts; }

	/**
	 * \brief Get the timestamp that will be used for the next Fragment
	 * \return The timestamp that will be used for the next Fragment
	 */
	artdaq::Fragment::timestamp_t getTimestamp() { return ts_; }

private:
	artdaq::Fragment::timestamp_t ts_;
	artdaq::Fragment::sequence_id_t seq_;
	std::set<artdaq::Fragment::fragment_id_t> fragmentIDs_;
};

artdaqtest::FragmentBufferTestGenerator::FragmentBufferTestGenerator(const fhicl::ParameterSet& ps)
    : ts_(0), seq_(1)
{
	metricMan->initialize(ps.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet()));
	metricMan->do_start();

	auto idlist = ps.get<std::vector<artdaq::Fragment::fragment_id_t>>("fragment_ids", {ps.get<artdaq::Fragment::fragment_id_t>("fragment_id", 1)});
	for (auto& id : idlist)
	{
		fragmentIDs_.insert(id);
	}
}

artdaq::FragmentPtrs artdaqtest::FragmentBufferTestGenerator::Generate(size_t n, std::vector<artdaq::Fragment::fragment_id_t> fragmentIds)
{
	if (fragmentIds.size() == 0) std::copy(fragmentIDs_.begin(), fragmentIDs_.end(), std::back_inserter(fragmentIds));

	artdaq::FragmentPtrs frags;
	while (n > 0)
	{
		++ts_;
		for (auto& id : fragmentIds)
		{
			TLOG(TLVL_DEBUG) << "Adding Fragment with ID " << id << ", SeqID " << seq_ << ", and timestamp " << ts_;
			frags.emplace_back(new artdaq::Fragment(seq_, id, artdaq::Fragment::FirstUserFragmentType, ts_));
		}
		++seq_;
		n--;
	}

	return frags;
}

BOOST_AUTO_TEST_SUITE(FragmentBuffer_t)

BOOST_AUTO_TEST_CASE(ImproperConfiguration)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "ImproperConfiguration test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<std::string>("request_mode", "window");

	artdaq::FragmentBuffer fp(ps);
	BOOST_REQUIRE_EQUAL(static_cast<int>(fp.request_mode()), static_cast<int>(artdaq::RequestMode::Ignored));

	ps.put<bool>("receive_requests", true);
	artdaq::FragmentBuffer fpp(ps);
	BOOST_REQUIRE_EQUAL(static_cast<int>(fpp.request_mode()), static_cast<int>(artdaq::RequestMode::Window));

	ps.put<std::vector<int>>("fragment_ids", {2, 3, 4});
	BOOST_REQUIRE_EXCEPTION(artdaq::FragmentBuffer ffp(ps), cet::exception, [](cet::exception const& e) { return e.category() == "FragmentBufferConfig"; });

	TLOG(TLVL_INFO) << "ImproperConfiguration test case END";
}

BOOST_AUTO_TEST_CASE(IgnoreRequests)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "IgnoreRequests test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<std::string>("request_mode", "ignored");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);

	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(53, 35);

	artdaqtest::FragmentBufferTestGenerator gen(ps);

	fp.AddFragmentsToBuffer(gen.Generate(1));

	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);

	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);

	TLOG(TLVL_INFO) << "IgnoreRequests test case END";
}

BOOST_AUTO_TEST_CASE(SingleMode)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "SingleMode test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "single");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);
	auto type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	buffer->push(2, 5);
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 5);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	fp.AddFragmentsToBuffer(gen.Generate(2));
	buffer->push(4, 7);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 5);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 2);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	auto ts = artdaq::Fragment::InvalidTimestamp;
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), ts);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
	auto emptyType = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), emptyType);
	fps.pop_front();
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 7);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	fps.clear();

	TLOG(TLVL_INFO) << "SingleMode test case END";
}

BOOST_AUTO_TEST_CASE(BufferMode)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "BufferMode test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "buffer");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	auto type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	BOOST_REQUIRE_GE(fps.front()->sizeBytes(), 2 * sizeof(artdaq::detail::RawFragmentHeader) + sizeof(artdaq::ContainerFragment::Metadata));
	auto cf = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	buffer->push(2, 5);
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 5);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf2 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf2.block_count(), 0);
	TRACE_REQUIRE_EQUAL(cf2.missing_data(), false);
	type = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(cf2.fragment_type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	fp.AddFragmentsToBuffer(gen.Generate(2));
	buffer->push(4, 7);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 2);

	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	auto ts = artdaq::Fragment::InvalidTimestamp;
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), ts);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
	auto emptyType = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), emptyType);
	TRACE_REQUIRE_EQUAL(fps.front()->size(), artdaq::detail::RawFragmentHeader::num_words());
	fps.pop_front();
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 7);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf3 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf3.block_count(), 2);
	TRACE_REQUIRE_EQUAL(cf3.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf3.fragment_type(), type);
	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 5);

	TLOG(TLVL_INFO) << "BufferMode test case END";
}

BOOST_AUTO_TEST_CASE(BufferMode_KeepLatest)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "BufferMode_KeepLatest test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "buffer");
	ps.put("buffer_mode_keep_latest", true);

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	auto type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	BOOST_REQUIRE_GE(fps.front()->sizeBytes(), 2 * sizeof(artdaq::detail::RawFragmentHeader) + sizeof(artdaq::ContainerFragment::Metadata));
	auto cf = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	buffer->push(2, 5);
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 5);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf2 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf2.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf2.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf2.fragment_type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	fp.AddFragmentsToBuffer(gen.Generate(2));
	buffer->push(4, 7);
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 2);

	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	auto ts = artdaq::Fragment::InvalidTimestamp;
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), ts);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
	auto emptyType = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), emptyType);
	TRACE_REQUIRE_EQUAL(fps.front()->size(), artdaq::detail::RawFragmentHeader::num_words());
	fps.pop_front();
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 7);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf3 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf3.block_count(), 2);
	TRACE_REQUIRE_EQUAL(cf3.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf3.fragment_type(), type);
	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 5);

	TLOG(TLVL_INFO) << "BufferMode_KeepLatest test case END";
}
BOOST_AUTO_TEST_CASE(CircularBufferMode)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "CircularBufferMode test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<bool>("receive_requests", true);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<int>("data_buffer_depth_fragments", 3);
	ps.put<std::string>("request_mode", "buffer");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	auto type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	BOOST_REQUIRE_GE(fps.front()->sizeBytes(), 2 * sizeof(artdaq::detail::RawFragmentHeader) + sizeof(artdaq::ContainerFragment::Metadata));
	auto cf = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	buffer->push(2, 5);
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 5);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf2 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf2.block_count(), 0);
	TRACE_REQUIRE_EQUAL(cf2.missing_data(), false);
	type = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(cf2.fragment_type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	fp.AddFragmentsToBuffer(gen.Generate(3));

	buffer->push(4, 7);
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 2);

	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	auto ts = artdaq::Fragment::InvalidTimestamp;
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), ts);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
	auto emptyType = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), emptyType);
	TRACE_REQUIRE_EQUAL(fps.front()->size(), artdaq::detail::RawFragmentHeader::num_words());
	fps.pop_front();
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 7);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf3 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf3.block_count(), 3);
	TRACE_REQUIRE_EQUAL(cf3.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf3.fragment_type(), type);
	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 5);

	fp.AddFragmentsToBuffer(gen.Generate(5));

	buffer->push(5, 8);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);

	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 8);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 5);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf4 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf4.block_count(), 3);
	TRACE_REQUIRE_EQUAL(cf4.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf4.fragment_type(), type);
	TRACE_REQUIRE_EQUAL(cf4.at(0)->timestamp(), 7);
	TRACE_REQUIRE_EQUAL(cf4.at(1)->timestamp(), 8);
	TRACE_REQUIRE_EQUAL(cf4.at(2)->timestamp(), 9);
	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 6);

	TLOG(TLVL_INFO) << "CircularBufferMode test case END";
}

BOOST_AUTO_TEST_CASE(WindowMode_Function)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WindowMode_Function test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);

	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<size_t>("data_buffer_depth_fragments", 5);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<std::string>("request_mode", "window");
	ps.put<bool>("receive_requests", true);
	ps.put<size_t>("missing_request_window_timeout_us", 500000);
	ps.put<size_t>("window_close_timeout_us", 500000);

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	auto type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	BOOST_REQUIRE_GE(fps.front()->sizeBytes(), 2 * sizeof(artdaq::detail::RawFragmentHeader) + sizeof(artdaq::ContainerFragment::Metadata));
	auto cf = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
	fps.clear();

	// No data for request
	buffer->push(2, 2);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);

	fp.AddFragmentsToBuffer(gen.Generate(1));

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 2);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf2 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf2.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf2.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf2.fragment_type(), type);
	fps.clear();

	// Request Timeout
	buffer->push(4, 3);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);

	usleep(1500000);
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);

	// Also, missing request timeout
	auto list = fp.GetSentWindowList(1);
	TRACE_REQUIRE_EQUAL(list.size(), 1);
	TRACE_REQUIRE_EQUAL(list.begin()->first, 4);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);

	usleep(1500000);

	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 3);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf3 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf3.block_count(), 0);
	TRACE_REQUIRE_EQUAL(cf3.missing_data(), true);
	type = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(cf3.fragment_type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	// Data-taking has passed request
	fp.AddFragmentsToBuffer(gen.Generate(12));

	buffer->push(5, 4);

	list = fp.GetSentWindowList(1);  // Out-of-order list is only updated in getNext calls
	TRACE_REQUIRE_EQUAL(list.size(), 1);
	sts = fp.applyRequests(fps);
	list = fp.GetSentWindowList(1);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 6);
	TRACE_REQUIRE_EQUAL(list.size(), 0);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 4);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 5);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf4 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf4.block_count(), 0);
	TRACE_REQUIRE_EQUAL(cf4.missing_data(), true);
	type = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(cf4.fragment_type(), type);
	fps.clear();

	// Out-of-order windows
	buffer->push(7, 13);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 13);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 7);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf5 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf5.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf5.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf5.fragment_type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 6);
	fps.clear();

	list = fp.GetSentWindowList(1);
	TRACE_REQUIRE_EQUAL(list.size(), 1);
	TRACE_REQUIRE_EQUAL(list.begin()->first, 7);

	buffer->push(6, 12);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 12);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 6);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf6 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf6.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf6.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf6.fragment_type(), type);
	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 8);

	list = fp.GetSentWindowList(1);
	TRACE_REQUIRE_EQUAL(list.size(), 0);

	usleep(1500000);

	TLOG(TLVL_INFO) << "WindowMode_Function test case END";
}

// 1. Both start and end before any data in buffer  "RequestBeforeBuffer"
// 2. Start before buffer, end in buffer            "RequestStartsBeforeBuffer"
// 3. Start befoer buffer, end after buffer         "RequestOutsideBuffer"
// 4. Start and end in buffer                       "RequestInBuffer"
// 5. Start in buffer, end after buffer             "RequestEndsAfterBuffer"
// 6. Start and end after buffer                    "RequestAfterBuffer"
BOOST_AUTO_TEST_CASE(WindowMode_RequestBeforeBuffer)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WindowMode_RequestBeforeBuffer test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);

	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 3);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<bool>("receive_requests", true);
	ps.put<size_t>("data_buffer_depth_fragments", 5);
	ps.put<std::string>("request_mode", "window");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	artdaq::FragmentPtrs fps;
	int sts;
	artdaq::Fragment::type_t type;

	// 1. Both start and end before any data in buffer
	//  -- Should return ContainerFragment with MissingData bit set and zero Fragments
	fp.AddFragmentsToBuffer(gen.Generate(10));  // Buffer start is at ts 6, end at 10

	buffer->push(1, 1);  // Requesting data from ts 1 to 3

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf4 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf4.block_count(), 0);
	TRACE_REQUIRE_EQUAL(cf4.missing_data(), true);
	type = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(cf4.fragment_type(), type);

	TLOG(TLVL_INFO) << "WindowMode_RequestBeforeBuffer test case END";
}
BOOST_AUTO_TEST_CASE(WindowMode_RequestStartsBeforeBuffer)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WindowMode_RequestStartsBeforeBuffer test case BEGIN";

	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 3);
	ps.put<size_t>("data_buffer_depth_fragments", 5);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "window");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	artdaq::FragmentPtrs fps;
	int sts;
	artdaq::Fragment::type_t type;

	fp.AddFragmentsToBuffer(gen.Generate(10));  // Buffer contains 6 to 10

	// 2. Start before buffer, end in buffer
	//  -- Should return ContainerFragment with MissingData bit set and one or more Fragments
	buffer->push(1, 4);  // Requesting data from ts 4 to 6

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 4);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf4 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf4.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf4.missing_data(), true);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf4.fragment_type(), type);

	TLOG(TLVL_INFO) << "WindowMode_RequestStartsBeforeBuffer test case END";
}
BOOST_AUTO_TEST_CASE(WindowMode_RequestOutsideBuffer)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WindowMode_RequestOutsideBuffer test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 4);
	ps.put<size_t>("window_close_timeout_us", 500000);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<size_t>("data_buffer_depth_fragments", 5);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "window");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	artdaq::FragmentPtrs fps;
	int sts;
	artdaq::Fragment::type_t type;

	fp.AddFragmentsToBuffer(gen.Generate(10));  // Buffer contains 6 to 10

	// 3. Start before buffer, end after buffer
	//  -- Should not return until buffer passes end or timeout (check both cases), MissingData bit set

	buffer->push(1, 6);  // Requesting data from ts 6 to 9, buffer will contain 10

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 6);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf.block_count(), 4);
	TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
	fps.clear();

	buffer->push(2, 9);  // Requesting data from ts 9 to 12

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);

	fp.AddFragmentsToBuffer(gen.Generate(3));  // Buffer start is at ts 10, end at 13

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 9);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf2 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf2.block_count(), 3);
	TRACE_REQUIRE_EQUAL(cf2.missing_data(), true);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf2.fragment_type(), type);
	fps.clear();

	buffer->push(3, 12);  // Requesting data from ts 11 to 14

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);

	usleep(550000);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 12);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf4 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf4.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf4.missing_data(), true);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf4.fragment_type(), type);

	TLOG(TLVL_INFO) << "WindowMode_RequestOutsideBuffer test case END";
}
BOOST_AUTO_TEST_CASE(WindowMode_RequestInBuffer)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WindowMode_RequestInBuffer test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 3);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<size_t>("data_buffer_depth_fragments", 5);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "window");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	artdaq::FragmentPtrs fps;
	int sts;
	artdaq::Fragment::type_t type;

	// 4. Start and end in buffer
	//  -- Should return ContainerFragment with one or more Fragments
	fp.AddFragmentsToBuffer(gen.Generate(6));  // Buffer start is at ts 2, end at 6

	buffer->push(1, 3);  // Requesting data from ts 3 to 5

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 3);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf4 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf4.block_count(), 3);
	TRACE_REQUIRE_EQUAL(cf4.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf4.fragment_type(), type);

	TLOG(TLVL_INFO) << "WindowMode_RequestInBuffer test case END";
}
BOOST_AUTO_TEST_CASE(WindowMode_RequestEndsAfterBuffer)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WindowMode_RequestEndsAfterBuffer test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 3);
	ps.put<size_t>("window_close_timeout_us", 500000);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<bool>("receive_requests", true);
	ps.put<size_t>("data_buffer_depth_fragments", 5);
	ps.put<std::string>("request_mode", "window");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	artdaq::FragmentPtrs fps;
	int sts;
	artdaq::Fragment::type_t type;

	fp.AddFragmentsToBuffer(gen.Generate(6));  // Buffer contains 2 to 6

	// 5. Start in buffer, end after buffer
	//  -- Should not return until buffer passes end or timeout (check both cases). MissingData bit set if timeout
	buffer->push(1, 5);  // Requesting data from ts 5 to 7

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);

	fp.AddFragmentsToBuffer(gen.Generate(2));  // Buffer contains 4 to 8

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 5);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf.block_count(), 3);
	TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
	fps.clear();

	buffer->push(2, 8);  // Requesting data from ts 8 to 10

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);

	usleep(550000);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 8);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf4 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf4.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf4.missing_data(), true);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf4.fragment_type(), type);

	TLOG(TLVL_INFO) << "WindowMode_RequestEndsAfterBuffer test case END";
}
BOOST_AUTO_TEST_CASE(WindowMode_RequestAfterBuffer)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WindowMode_RequestAfterBuffer test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 3);
	ps.put<size_t>("window_close_timeout_us", 500000);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<bool>("receive_requests", true);
	ps.put<size_t>("data_buffer_depth_fragments", 5);
	ps.put<std::string>("request_mode", "window");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	artdaq::FragmentPtrs fps;
	int sts;
	artdaq::Fragment::type_t type;

	// 6. Start and end after buffer
	//  -- Should not return until buffer passes end or timeout (check both cases). MissingData bit set if timeout
	fp.AddFragmentsToBuffer(gen.Generate(10));  // Buffer start is 6, end at 10

	buffer->push(1, 11);  // Requesting data from ts 11 to 13

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);
	fp.AddFragmentsToBuffer(gen.Generate(1));  // Buffer start is 7, end at 11

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);

	fp.AddFragmentsToBuffer(gen.Generate(3));  // Buffer start is 10, end at 14

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 11);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf.block_count(), 3);
	TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
	fps.clear();

	buffer->push(2, 16);  // Requesting data from ts 15 to 17

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);

	usleep(550000);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 16);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf4 = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf4.block_count(), 0);
	TRACE_REQUIRE_EQUAL(cf4.missing_data(), true);
	type = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(cf4.fragment_type(), type);

	TLOG(TLVL_INFO) << "WindowMode_RequestAfterBuffer test case END";
}

BOOST_AUTO_TEST_CASE(SequenceIDMode)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "SequenceIDMode test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "SequenceID");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	// Test that Fragment with matching Sequence ID and timestamp is returned
	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);
	auto type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	// Test that no Fragment is returned when one does not exist in the buffer
	buffer->push(2, 5);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0u);

	// Test that Fragment with matching Sequence ID and non-matching timestamp is returned
	fp.AddFragmentsToBuffer(gen.Generate(1));

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 2);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	// Test out-of-order requests, with non-matching timestamps
	fp.AddFragmentsToBuffer(gen.Generate(2));

	buffer->push(4, 7);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 4);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	fps.clear();

	buffer->push(3, 6);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 5);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 3);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);

	TLOG(TLVL_INFO) << "SequenceIDMode test case END";
}

BOOST_AUTO_TEST_CASE(IgnoreRequests_MultipleIDs)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "IgnoreRequests_MultipleIDs test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<std::vector<int>>("fragment_ids", {1, 2, 3});
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<std::string>("request_mode", "ignored");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(53, 35);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	artdaq::FragmentPtrs fps;
	std::map<artdaq::Fragment::fragment_id_t, size_t> ids;
	auto sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
		fps.pop_front();
	}

	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	fps.clear();

	TLOG(TLVL_INFO) << "IgnoreRequests_MultipleIDs test case END";
}

BOOST_AUTO_TEST_CASE(SingleMode_MultipleIDs)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "SingleMode_MultipleIDs test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<std::vector<int>>("fragment_ids", {1, 2, 3});
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<bool>("receive_requests", true);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<std::string>("request_mode", "single");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	artdaq::FragmentPtrs fps;
	std::map<artdaq::Fragment::fragment_id_t, size_t> ids;
	auto sts = fp.applyRequests(fps);
	auto type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	buffer->push(2, 5);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 5);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	fp.AddFragmentsToBuffer(gen.Generate(2));

	buffer->push(4, 7);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 5);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 6);
	auto ts = artdaq::Fragment::InvalidTimestamp;
	auto emptyType = artdaq::Fragment::EmptyFragmentType;
	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), ts);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), emptyType);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 7);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	fps.clear();

	// Single mode should generate 3 Fragments, 2 new ones and one old one
	fp.AddFragmentsToBuffer(gen.Generate(1));

	buffer->push(5, 9);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 6);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3);
	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 9);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 5);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	fps.clear();

	TLOG(TLVL_INFO) << "SingleMode_MultipleIDs test case END";
}

BOOST_AUTO_TEST_CASE(BufferMode_MultipleIDs)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "BufferMode_MultipleIDs test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<std::vector<int>>("fragment_ids", {1, 2, 3});
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "buffer");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	artdaq::FragmentPtrs fps;
	std::map<artdaq::Fragment::fragment_id_t, size_t> ids;
	auto sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	auto type = artdaq::Fragment::ContainerFragmentType;
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		BOOST_REQUIRE_GE(fps.front()->sizeBytes(), 2 * sizeof(artdaq::detail::RawFragmentHeader) + sizeof(artdaq::ContainerFragment::Metadata));
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	buffer->push(2, 5);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 5);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 0);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::EmptyFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	fp.AddFragmentsToBuffer(gen.Generate(2));

	buffer->push(4, 7);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 6);

	auto ts = artdaq::Fragment::InvalidTimestamp;
	auto emptyType = artdaq::Fragment::EmptyFragmentType;
	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), ts);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), emptyType);
		TRACE_REQUIRE_EQUAL(fps.front()->size(), artdaq::detail::RawFragmentHeader::num_words());
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 7);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf3 = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf3.block_count(), 2);
		TRACE_REQUIRE_EQUAL(cf3.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf3.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 5);

	TLOG(TLVL_INFO) << "BufferMode_MultipleIDs test case END";
}

BOOST_AUTO_TEST_CASE(CircularBufferMode_MultipleIDs)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "CircularBufferMode_MultipleIDs test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<std::vector<int>>("fragment_ids", {1, 2, 3});
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<bool>("receive_requests", true);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<int>("data_buffer_depth_fragments", 3);
	ps.put<std::string>("request_mode", "buffer");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	artdaq::FragmentPtrs fps;
	std::map<artdaq::Fragment::fragment_id_t, size_t> ids;
	auto sts = fp.applyRequests(fps);
	auto type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		BOOST_REQUIRE_GE(fps.front()->sizeBytes(), 2 * sizeof(artdaq::detail::RawFragmentHeader) + sizeof(artdaq::ContainerFragment::Metadata));
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	buffer->push(2, 5);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 5);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 0);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::EmptyFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	fp.AddFragmentsToBuffer(gen.Generate(3));

	buffer->push(4, 7);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 6);

	auto ts = artdaq::Fragment::InvalidTimestamp;
	auto emptyType = artdaq::Fragment::EmptyFragmentType;
	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), ts);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), emptyType);
		TRACE_REQUIRE_EQUAL(fps.front()->size(), artdaq::detail::RawFragmentHeader::num_words());
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 7);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf3 = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf3.block_count(), 3);
		TRACE_REQUIRE_EQUAL(cf3.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf3.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 5);

	fp.AddFragmentsToBuffer(gen.Generate(5));

	buffer->push(5, 8);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 8);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 5);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 3);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		TRACE_REQUIRE_EQUAL(cf.at(0)->timestamp(), 7);
		TRACE_REQUIRE_EQUAL(cf.at(1)->timestamp(), 8);
		TRACE_REQUIRE_EQUAL(cf.at(2)->timestamp(), 9);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 6);

	TLOG(TLVL_INFO) << "CircularBufferMode_MultipleIDs test case END";
}

BOOST_AUTO_TEST_CASE(WindowMode_Function_MultipleIDs)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WindowMode_Function_MultipleIDs test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<std::vector<int>>("fragment_ids", {1, 2, 3});
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<size_t>("data_buffer_depth_fragments", 5);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "window");
	ps.put<size_t>("missing_request_window_timeout_us", 500000);
	ps.put<size_t>("window_close_timeout_us", 500000);

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	artdaq::FragmentPtrs fps;
	std::map<artdaq::Fragment::fragment_id_t, size_t> ids;
	auto sts = fp.applyRequests(fps);
	auto type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		BOOST_REQUIRE_GE(fps.front()->sizeBytes(), 2 * sizeof(artdaq::detail::RawFragmentHeader) + sizeof(artdaq::ContainerFragment::Metadata));
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	fps.clear();

	// No data for request
	buffer->push(2, 2);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);

	fp.AddFragmentsToBuffer(gen.Generate(1));

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 2);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	fps.clear();

	// Request Timeout
	buffer->push(4, 3);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);

	usleep(1500000);
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3);

	// Also, missing request timeout
	auto list = fp.GetSentWindowList(1);
	TRACE_REQUIRE_EQUAL(list.size(), 1);
	TRACE_REQUIRE_EQUAL(list.begin()->first, 4);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);

	usleep(1500000);

	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 3);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 0);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), true);
		type = artdaq::Fragment::EmptyFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	// Data-taking has passed request
	fp.AddFragmentsToBuffer(gen.Generate(12));

	buffer->push(5, 4);

	list = fp.GetSentWindowList(1);  // Out-of-order list is only updated in getNext calls
	TRACE_REQUIRE_EQUAL(list.size(), 1);
	sts = fp.applyRequests(fps);
	list = fp.GetSentWindowList(1);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 6);
	TRACE_REQUIRE_EQUAL(list.size(), 0);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 4);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 5);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 0);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), true);
		type = artdaq::Fragment::EmptyFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	fps.clear();

	// Out-of-order windows
	buffer->push(7, 13);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 13);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 7);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 6);
	fps.clear();

	list = fp.GetSentWindowList(1);
	TRACE_REQUIRE_EQUAL(list.size(), 1);
	TRACE_REQUIRE_EQUAL(list.begin()->first, 7);

	buffer->push(6, 12);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 12);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 6);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 8);

	list = fp.GetSentWindowList(1);
	TRACE_REQUIRE_EQUAL(list.size(), 0);

	fp.AddFragmentsToBuffer(gen.Generate(1, {1, 2}));

	buffer->push(8, 15);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 2);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 15);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 8);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 0);
	ids.clear();
	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 8);

	gen.setTimestamp(14);  // Reset timestamp
	fp.AddFragmentsToBuffer(gen.Generate(1, {3}));

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1);
	while (fps.size() > 0)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 15);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 8);
		type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		auto cf = artdaq::ContainerFragment(*fps.front());
		TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
		TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
		type = artdaq::Fragment::FirstUserFragmentType;
		TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 0);
	TRACE_REQUIRE_EQUAL(ids[2], 0);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	fps.clear();
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 9);

	TLOG(TLVL_INFO) << "WindowMode_Function_MultipleIDs test case END";
}

BOOST_AUTO_TEST_CASE(SequenceIDMode_MultipleIDs)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "SequenceIDMode_MultipleIDs test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("board_id", 1);
	ps.put<std::vector<int>>("fragment_ids", {1, 2, 3});
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<bool>("separate_data_thread", true);
	ps.put<bool>("separate_monitoring_thread", false);
	ps.put<bool>("receive_requests", true);
	ps.put<int64_t>("hardware_poll_interval_us", 0);
	ps.put<std::string>("request_mode", "SequenceID");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	// Test that Fragment with matching Sequence ID and timestamp is returned
	artdaq::FragmentPtrs fps;
	std::map<artdaq::Fragment::fragment_id_t, size_t> ids;
	auto sts = fp.applyRequests(fps);
	auto type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);

	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	// Test that no Fragment is returned when one does not exist in the buffer
	buffer->push(2, 5);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0u);

	// Test that Fragment with matching Sequence ID and non-matching timestamp is returned
	fp.AddFragmentsToBuffer(gen.Generate(1));

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 2);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	// Test out-of-order requests, with non-matching timestamps
	fp.AddFragmentsToBuffer(gen.Generate(2));

	buffer->push(4, 7);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 4);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 4);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();
	fps.clear();

	buffer->push(3, 6);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 5);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 3u);
	for (auto ii = 0; ii < 3; ++ii)
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 3);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 1);
	ids.clear();

	TLOG(TLVL_INFO) << "SequenceIDMode_MultipleIDs test case END";
}

BOOST_AUTO_TEST_CASE(IgnoreRequests_StateMachine)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "IgnoreRequests_StateMachine test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<std::string>("request_mode", "ignored");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);

	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(53, 35);

	artdaqtest::FragmentBufferTestGenerator gen(ps);

	fp.AddFragmentsToBuffer(gen.Generate(1));

	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);

	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	fps.clear();

	fp.Reset(false);
	sts = fp.applyRequests(fps);

	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0u);

	fp.AddFragmentsToBuffer(gen.Generate(1));

	sts = fp.applyRequests(fps);

	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 2);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	fps.clear();

	fp.Stop();
	fp.AddFragmentsToBuffer(gen.Generate(1));

	sts = fp.applyRequests(fps);

	TRACE_REQUIRE_EQUAL(sts, false);
	TRACE_REQUIRE_EQUAL(fps.size(), 0u);

	TLOG(TLVL_INFO) << "IgnoreRequests_StateMachine test case END";
}

BOOST_AUTO_TEST_CASE(SingleMode_StateMachine)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "SingleMode_StateMachine test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "single");

	auto type = artdaq::Fragment::FirstUserFragmentType;
	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	fp.Reset(false);
	buffer->reset();
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 0u);

	buffer->push(1, 1);
	fp.AddFragmentsToBuffer(gen.Generate(1));
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 2);
	fps.clear();

	fp.Stop();
	buffer->push(2, 5);
	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 5);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 3);
	fps.clear();

	buffer->setRunning(false);
	fp.AddFragmentsToBuffer(gen.Generate(2));
	buffer->push(4, 7);

	sts = fp.applyRequests(fps);
	TRACE_REQUIRE_EQUAL(sts, false);
	TRACE_REQUIRE_EQUAL(fps.size(), 0);

	TLOG(TLVL_INFO) << "SingleMode_StateMachine test case END";
}

BOOST_AUTO_TEST_CASE(WindowMode_RateTests)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WindowMode_RateTests test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);

	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<size_t>("data_buffer_depth_fragments", 2 * RATE_TEST_COUNT);
	ps.put<bool>("circular_buffer_mode", false);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "window");
	ps.put<size_t>("missing_request_window_timeout_us", 500000);
	ps.put<size_t>("window_close_timeout_us", 500000);

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	auto beginop = std::chrono::steady_clock::now();

	TLOG(TLVL_INFO) << "Generating " << RATE_TEST_COUNT << " requests BEGIN";
	size_t req_seq = 1;
	for (; req_seq < RATE_TEST_COUNT + 1; ++req_seq)
	{
		buffer->push(req_seq, req_seq);
	}
	TLOG(TLVL_INFO) << "Generating " << RATE_TEST_COUNT << " requests END.Time elapsed = " << artdaq::TimeUtils::GetElapsedTime(beginop)
	                << "(" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " reqs/s) ";

	beginop = std::chrono::steady_clock::now();
	TLOG(TLVL_INFO) << "Generating/adding " << RATE_TEST_COUNT << " Fragments BEGIN";
	fp.AddFragmentsToBuffer(gen.Generate(RATE_TEST_COUNT));
	TLOG(TLVL_INFO) << "Generating/adding " << RATE_TEST_COUNT << " Fragments END. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(beginop)
	                << " (" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " frags/s)";

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	beginop = std::chrono::steady_clock::now();
	TLOG(TLVL_INFO) << "Applying requests BEGIN";
	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);
	TLOG(TLVL_INFO) << "Applying requests END. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(beginop)
	                << " (" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " reqs/s)";
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), RATE_TEST_COUNT + 1);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), RATE_TEST_COUNT);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	auto type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	BOOST_REQUIRE_GE(fps.front()->sizeBytes(), 2 * sizeof(artdaq::detail::RawFragmentHeader) + sizeof(artdaq::ContainerFragment::Metadata));
	auto cf = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf.block_count(), 1);
	TRACE_REQUIRE_EQUAL(cf.missing_data(), false);
	type = artdaq::Fragment::FirstUserFragmentType;
	TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
	fps.clear();

	TLOG(TLVL_INFO) << "Generating " << RATE_TEST_COUNT << " requests BEGIN";
	for (; req_seq < (2 * RATE_TEST_COUNT) + 1; ++req_seq)
	{
		buffer->push(req_seq, req_seq);
	}
	TLOG(TLVL_INFO) << "Generating " << RATE_TEST_COUNT << " requests END. Time elapsed = " << artdaq::TimeUtils::GetElapsedTime(beginop)
	                << "(" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " reqs/s) ";

	beginop = std::chrono::steady_clock::now();
	TLOG(TLVL_INFO) << "Generating/adding " << RATE_TEST_COUNT << " Fragments Individually BEGIN";
	for (int ii = 0; ii < RATE_TEST_COUNT; ++ii)
		fp.AddFragmentsToBuffer(gen.Generate(1));
	TLOG(TLVL_INFO) << "Generating/adding " << RATE_TEST_COUNT << " Fragments Individually END. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(beginop)
	                << " (" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " frags/s)";

	TLOG(TLVL_INFO) << "WindowMode_RateTests test case END";
}

BOOST_AUTO_TEST_CASE(CircularBufferMode_RateTests)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "CircularBufferMode_RateTests test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<size_t>("data_buffer_depth_fragments", RATE_TEST_COUNT / 2);
	ps.put<bool>("circular_buffer_mode", true);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "window");
	ps.put<size_t>("missing_request_window_timeout_us", 500000);
	ps.put<size_t>("window_close_timeout_us", 500000);

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	auto beginop = std::chrono::steady_clock::now();

	TLOG(TLVL_INFO) << "Generating " << RATE_TEST_COUNT << " requests BEGIN";
	for (size_t ii = 1; ii < RATE_TEST_COUNT + 1; ++ii)
	{
		buffer->push(ii, ii);
	}
	TLOG(TLVL_INFO) << "Generating " << RATE_TEST_COUNT << " requests END. Time elapsed = " << artdaq::TimeUtils::GetElapsedTime(beginop)
	                << "(" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " reqs / s) ";

	beginop = std::chrono::steady_clock::now();
	TLOG(TLVL_INFO) << "Generating/adding " << RATE_TEST_COUNT << " Fragments BEGIN";
	fp.AddFragmentsToBuffer(gen.Generate(RATE_TEST_COUNT));
	TLOG(TLVL_INFO) << "Generating/adding " << RATE_TEST_COUNT << " Fragments END. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(beginop)
	                << " (" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " frags/s)";

	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), 1);

	beginop = std::chrono::steady_clock::now();
	TLOG(TLVL_INFO) << "Applying requests BEGIN";
	artdaq::FragmentPtrs fps;
	auto sts = fp.applyRequests(fps);
	TLOG(TLVL_INFO) << "Applying requests END. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(beginop)
	                << " (" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " reqs/s)";
	TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), RATE_TEST_COUNT + 1);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), RATE_TEST_COUNT);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	auto type = artdaq::Fragment::ContainerFragmentType;
	TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
	auto cf = artdaq::ContainerFragment(*fps.front());
	TRACE_REQUIRE_EQUAL(cf.block_count(), 0);
	TRACE_REQUIRE_EQUAL(cf.missing_data(), true);
	type = artdaq::Fragment::EmptyFragmentType;
	TRACE_REQUIRE_EQUAL(cf.fragment_type(), type);
	fps.clear();

	TLOG(TLVL_INFO) << "CircularBufferMode_RateTests test case END";
}

BOOST_AUTO_TEST_CASE(WindowMode_RateTests_threaded)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WindowMode_RateTests_threaded test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<size_t>("data_buffer_depth_fragments", RATE_TEST_COUNT);
	ps.put<bool>("circular_buffer_mode", false);
	ps.put<std::string>("request_mode", "window");
	ps.put<bool>("receive_requests", true);
	ps.put<size_t>("missing_request_window_timeout_us", 500000);
	ps.put<size_t>("window_close_timeout_us", 500000);

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	std::atomic<int> thread_sync = 0;
	auto gen_requests = [&]() {
		++thread_sync;
		while (thread_sync < 3) {}
		auto beginop = std::chrono::steady_clock::now();
		TLOG(TLVL_INFO) << "Generating " << RATE_TEST_COUNT << " requests BEGIN";
		for (size_t ii = 1; ii < RATE_TEST_COUNT + 1; ++ii)
		{
			buffer->push(ii, ii);
		}
		TLOG(TLVL_INFO) << "Generating " << RATE_TEST_COUNT << " requests END. Time elapsed = " << artdaq::TimeUtils::GetElapsedTime(beginop)
		                << "(" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " reqs / s) ";
	};

	auto gen_frags = [&]() {
		++thread_sync;
		while (thread_sync < 3) {}
		auto beginop = std::chrono::steady_clock::now();
		TLOG(TLVL_INFO) << "Generating/adding " << RATE_TEST_COUNT << " Fragments BEGIN";
		for (int ii = 0; ii < RATE_TEST_COUNT; ++ii)
			fp.AddFragmentsToBuffer(gen.Generate(1));
		TLOG(TLVL_INFO) << "Generating/adding " << RATE_TEST_COUNT << " Fragments END. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(beginop)
		                << " (" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " frags/s)";
	};

	auto apply_requests = [&]() {
		++thread_sync;
		while (thread_sync < 3) {}
		artdaq::FragmentPtrs fps;
		auto begin_test = std::chrono::steady_clock::now();
		while (fps.size() < RATE_TEST_COUNT && artdaq::TimeUtils::GetElapsedTime(begin_test) < RATE_TEST_COUNT / 1000)  // 1 ms per request
		{
			auto beginop = std::chrono::steady_clock::now();
			TLOG(TLVL_INFO) << "Applying requests BEGIN";
			auto sts = fp.applyRequests(fps);
			TRACE_REQUIRE_EQUAL(sts, true);
			TLOG(TLVL_INFO) << "Applying requests END. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(beginop);
		}
		if (fps.size() < RATE_TEST_COUNT)
		{
			TLOG(TLVL_WARNING) << "Some requests did not return data. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(begin_test) << ", " << fps.size() << " / " << RATE_TEST_COUNT << " Fragments received";
		}
		else
		{
			TLOG(TLVL_INFO) << "All request replies received. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(begin_test)
			                << " (" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(begin_test) << " reqs/s)";
			TRACE_REQUIRE_EQUAL(fp.GetNextSequenceID(), RATE_TEST_COUNT + 1);

			TRACE_REQUIRE_EQUAL(fps.size(), RATE_TEST_COUNT);
		}
		TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
		auto type = artdaq::Fragment::ContainerFragmentType;
		TRACE_REQUIRE_EQUAL(fps.front()->type(), type);
		fps.clear();
	};

	std::thread t1(gen_requests);
	std::thread t2(gen_frags);
	std::thread t3(apply_requests);

	t1.join();
	t2.join();
	t3.join();

	TLOG(TLVL_INFO) << "WindowMode_RateTests_threaded test case END";
}

BOOST_AUTO_TEST_CASE(WaitForDataBufferReady_RaceCondition)
{
	artdaq::configureMessageFacility("FragmentBuffer_t", true, MESSAGEFACILITY_DEBUG);
	TLOG(TLVL_INFO) << "WaitForDataBufferReady_RaceCondition test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("fragment_id", 1);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<size_t>("data_buffer_depth_fragments", 1);
	ps.put<bool>("circular_buffer_mode", false);
	ps.put<bool>("receive_requests", true);
	ps.put<std::string>("request_mode", "window");

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::FragmentBufferTestGenerator gen(ps);
	artdaq::FragmentBuffer fp(ps);
	fp.SetRequestBuffer(buffer);

	std::atomic<int> thread_sync = 0;

	auto gen_frags = [&]() {
		++thread_sync;
		while (thread_sync < 2) {}
		auto beginop = std::chrono::steady_clock::now();
		TLOG(TLVL_INFO) << "Generating/adding " << RATE_TEST_COUNT << " Fragments BEGIN";
		for (int ii = 0; ii < RATE_TEST_COUNT; ++ii)
			fp.AddFragmentsToBuffer(gen.Generate(1));
		TLOG(TLVL_INFO) << "Generating/adding " << RATE_TEST_COUNT << " Fragments END. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(beginop)
		                << " (" << RATE_TEST_COUNT / artdaq::TimeUtils::GetElapsedTime(beginop) << " frags/s)";
		thread_sync = 0;
	};

	auto reset_buffer = [&]() {
		++thread_sync;
		while (thread_sync < 2) {}
		auto beginop = std::chrono::steady_clock::now();
		TLOG(TLVL_INFO) << "Resetting loop BEGIN";
		size_t counter = 0;
		while (thread_sync > 0)
		{
			fp.Reset(false);
			counter++;
		}
		TLOG(TLVL_INFO) << "Reset " << RATE_TEST_COUNT << " times during loop. Time elapsed=" << artdaq::TimeUtils::GetElapsedTime(beginop);
	};

	std::thread t1(gen_frags);
	std::thread t2(reset_buffer);

	t1.join();
	t2.join();

	TLOG(TLVL_INFO) << "WaitForDataBufferReady_RaceCondition test case END";
}

BOOST_AUTO_TEST_SUITE_END()
