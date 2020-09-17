#define TRACE_NAME "CommandableFragmentGenerator_t"

#define BOOST_TEST_MODULE CommandableFragmentGenerator_t
#include <boost/test/unit_test.hpp>

#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/Generators/CommandableFragmentGenerator.hh"

#define TRACE_REQUIRE_EQUAL(l, r)                                                                                                    \
	do                                                                                                                               \
	{                                                                                                                                \
		if ((l) == (r))                                                                                                              \
		{                                                                                                                            \
			TLOG(TLVL_DEBUG) << __LINE__ << ": Checking if " << #l << " (" << (l) << ") equals " << #r << " (" << (r) << ")...YES!"; \
		}                                                                                                                            \
		else                                                                                                                         \
		{                                                                                                                            \
			TLOG(TLVL_ERROR) << __LINE__ << ": Checking if " << #l << " (" << (l) << ") equals " << #r << " (" << (r) << ")...NO!";  \
		}                                                                                                                            \
		BOOST_REQUIRE_EQUAL((l), (r));                                                                                               \
	} while (false)

#define TRACE_REQUIRE(b)                                                                                 \
	do                                                                                                   \
	{                                                                                                    \
		if (b)                                                                                           \
		{                                                                                                \
			TLOG(TLVL_DEBUG) << __LINE__ << ": Checking if " << #b << " (" << (b) << ") is true...YES!"; \
		}                                                                                                \
		else                                                                                             \
		{                                                                                                \
			TLOG(TLVL_ERROR) << __LINE__ << ": Checking if " << #b << " (" << (b) << ") is true...NO!";  \
		}                                                                                                \
		BOOST_REQUIRE((b));                                                                              \
	} while (false)

namespace artdaqtest {
class CommandableFragmentGeneratorTest;
}

/**
 * \brief CommandableFragmentGenerator derived class for testing
 */
class artdaqtest::CommandableFragmentGeneratorTest : public artdaq::CommandableFragmentGenerator
{
public:
	/**
	 * \brief CommandableFragmentGeneratorTest Constructor
	 */
	explicit CommandableFragmentGeneratorTest(const fhicl::ParameterSet& ps);

	~CommandableFragmentGeneratorTest() override { joinThreads(); };

private:
	CommandableFragmentGeneratorTest(CommandableFragmentGeneratorTest const&) = delete;
	CommandableFragmentGeneratorTest(CommandableFragmentGeneratorTest&&) = delete;
	CommandableFragmentGeneratorTest& operator=(CommandableFragmentGeneratorTest const&) = delete;
	CommandableFragmentGeneratorTest& operator=(CommandableFragmentGeneratorTest&&) = delete;

protected:
	/**
	 * \brief Generate data and return it to CommandableFragmentGenerator
	 * \param frags FragmentPtrs list that new Fragments should be added to
	 * \return True if data was generated
	 *
	 * CommandableFragmentGeneratorTest merely default-constructs Fragments, emplacing them on the frags list.
	 */
	bool getNext_(artdaq::FragmentPtrs& frags) override;

	/**
	 * \brief Returns whether the hwFail flag has not been set
	 * \return If hwFail has been set, false, otherwise true
	 */
	bool checkHWStatus_() override { return !hwFail_.load(); }

	/**
	 * \brief Perform start actions. No-Op
	 */
	void start() override;

	/**
	 * \brief Perform immediate stop actions. No-Op
	 */
	void stopNoMutex() override;

	/**
	* \brief Perform stop actions. No-Op
	*/
	void stop() override;

	/**
	 * \brief Perform pause actions. No-Op
	 */
	void pause() override;

	/**
	 * \brief Perform resume actions. No-Op
	 */
	void resume() override;

public:
	/**
	 * \brief Have getNext_ generate count fragments
	 * \param count Number of fragments to generate
	 */
	void setFireCount(size_t count) { fireCount_ = count; }

	/**
	 * \brief Set the hwFail flag
	 */
	void setHwFail() { hwFail_ = true; }

	/**
	 * \brief Set the enabled IDs mask for the Fragment Generator
	 * \param bitmask Bitmask of enabled IDs for the Fragment Generator
	 *
	 * For testing, this bitmask allows a configured Fragment ID to not be generated by a given call to setFireCount.
	 * This is used to create asymmetric response from the Fragment generator in the _MultipleIDs test cases
	 */
	void setEnabledIds(uint64_t bitmask) { enabled_ids_ = bitmask; }

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
	std::atomic<size_t> fireCount_;
	std::atomic<bool> hwFail_;
	artdaq::Fragment::timestamp_t ts_;
	std::atomic<bool> hw_stop_;
	std::atomic<uint64_t> enabled_ids_;
};

artdaqtest::CommandableFragmentGeneratorTest::CommandableFragmentGeneratorTest(const fhicl::ParameterSet& ps)
    : CommandableFragmentGenerator(ps)
    , fireCount_(1)
    , hwFail_(false)
    , ts_(0)
    , hw_stop_(false)
    , enabled_ids_(-1)
{
	metricMan->initialize(ps.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet()));
	metricMan->do_start();
}

bool artdaqtest::CommandableFragmentGeneratorTest::getNext_(artdaq::FragmentPtrs& frags)
{
	while (fireCount_ > 0)
	{
		++ts_;
		for (auto& id : fragmentIDs())
		{
			if (id < 64 && ((enabled_ids_ & (0x1 << id)) != 0))
			{
				TLOG(TLVL_DEBUG) << "Adding Fragment with ID " << id << ", SeqID " << ev_counter() << ", and timestamp " << ts_;
				frags.emplace_back(new artdaq::Fragment(ev_counter(), id, artdaq::Fragment::FirstUserFragmentType, ts_));
			}
		}
		fireCount_--;
		ev_counter_inc();
	}

	return !hw_stop_;
}

void artdaqtest::CommandableFragmentGeneratorTest::start() { hw_stop_ = false; }

void artdaqtest::CommandableFragmentGeneratorTest::stopNoMutex() {}

void artdaqtest::CommandableFragmentGeneratorTest::stop() { hw_stop_ = true; }

void artdaqtest::CommandableFragmentGeneratorTest::pause() {}

void artdaqtest::CommandableFragmentGeneratorTest::resume() {}

BOOST_AUTO_TEST_SUITE(CommandableFragmentGenerator_t)

BOOST_AUTO_TEST_CASE(Simple)
{
	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
	TLOG(TLVL_INFO) << "Simple test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("board_id", 1);
	ps.put<int>("fragment_id", 1);
	artdaqtest::CommandableFragmentGeneratorTest testGen(ps);
	artdaq::FragmentPtrs fps;
	auto sts = testGen.getNext(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	TLOG(TLVL_INFO) << "Simple test case END";
}

BOOST_AUTO_TEST_CASE(MultipleIDs)
{
	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
	TLOG(TLVL_INFO) << "MultipleIDs test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("board_id", 1);
	ps.put<std::vector<int>>("fragment_ids", {1, 2, 3});

	artdaqtest::CommandableFragmentGeneratorTest testGen(ps);
	artdaq::FragmentPtrs fps;
	auto sts = testGen.getNext(fps);

	std::map<artdaq::Fragment::fragment_id_t, size_t> ids;
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

	testGen.setEnabledIds(0x6);  // 0110b, ID 3 disabled
	testGen.setFireCount(1);

	sts = testGen.getNext(fps);

	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 2u);
	while (!fps.empty())
	{
		ids[fps.front()->fragmentID()]++;
		TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 2);
		TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
		fps.pop_front();
	}
	TRACE_REQUIRE_EQUAL(ids[1], 1);
	TRACE_REQUIRE_EQUAL(ids[2], 1);
	TRACE_REQUIRE_EQUAL(ids[3], 0);

	TLOG(TLVL_INFO) << "MultipleIDs test case END";
}

BOOST_AUTO_TEST_CASE(HardwareFailure_NonThreaded)
{
	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
	TLOG(TLVL_INFO) << "HardwareFailure_NonThreaded test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("board_id", 1);
	ps.put<int>("fragment_id", 1);
	ps.put<bool>("separate_data_thread", false);
	ps.put<bool>("separate_monitoring_thread", false);
	ps.put<int64_t>("hardware_poll_interval_us", 10);

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::CommandableFragmentGeneratorTest gen(ps);
	gen.SetRequestBuffer(buffer);
	gen.StartCmd(1, 0xFFFFFFFF, 1);

	artdaq::FragmentPtrs fps;
	auto sts = gen.getNext(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	fps.clear();

	gen.setFireCount(1);
	gen.setHwFail();
	usleep(10000);
	sts = gen.getNext(fps);
	TRACE_REQUIRE_EQUAL(sts, false);
	TRACE_REQUIRE(fps.empty());

	gen.StopCmd(0xFFFFFFFF, 1);
	gen.joinThreads();
	TLOG(TLVL_INFO) << "HardwareFailure_NonThreaded test case END";
}

BOOST_AUTO_TEST_CASE(HardwareFailure_Threaded)
{
	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
	TLOG(TLVL_INFO) << "HardwareFailure_Threaded test case BEGIN";
	fhicl::ParameterSet ps;
	ps.put<int>("board_id", 1);
	ps.put<int>("fragment_id", 1);
	ps.put<bool>("separate_monitoring_thread", true);
	ps.put<int64_t>("hardware_poll_interval_us", 750000);

	auto buffer = std::make_shared<artdaq::RequestBuffer>();
	buffer->setRunning(true);
	artdaqtest::CommandableFragmentGeneratorTest gen(ps);
	gen.SetRequestBuffer(buffer);
	gen.StartCmd(1, 0xFFFFFFFF, 1);

	artdaq::FragmentPtrs fps;
	auto sts = gen.getNext(fps);
	TRACE_REQUIRE_EQUAL(sts, true);
	TRACE_REQUIRE_EQUAL(fps.size(), 1u);
	TRACE_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	TRACE_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	fps.clear();

	TLOG(TLVL_INFO) << "Setting failure bit";
	gen.setHwFail();

	sleep(1);

	TLOG(TLVL_INFO) << "Checking that failure is reported by getNext";
	gen.setFireCount(1);
	sts = gen.getNext(fps);
	TRACE_REQUIRE_EQUAL(sts, false);
	TRACE_REQUIRE(fps.empty());

	gen.StopCmd(0xFFFFFFFF, 1);
	gen.joinThreads();
	TLOG(TLVL_INFO) << "HardwareFailure_Threaded test case END";
}

BOOST_AUTO_TEST_SUITE_END()
