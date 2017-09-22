#define BOOST_TEST_MODULE CommandableFragmentGenerator_t
#include <boost/test/auto_unit_test.hpp>

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/Application/CommandableFragmentGenerator.hh"
#include "artdaq/DAQrate/RequestSender.hh"

namespace artdaqtest
{
	class CommandableFragmentGeneratorTest;
}

/**
 * \brief CommandableFragmentGenerator derived class for testing
 */
class artdaqtest::CommandableFragmentGeneratorTest :
	public artdaq::CommandableFragmentGenerator
{
public:
	/**
	 * \brief CommandableFragmentGeneratorTest Constructor
	 */
	explicit CommandableFragmentGeneratorTest(const fhicl::ParameterSet& ps);

	virtual ~CommandableFragmentGeneratorTest() = default;

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

	/**
	 * \brief Have getNext_ generate count fragments
	 * \param count Number of fragments to generate
	 */
	void setFireCount(size_t count) { fireCount_ = count; }

	/**
	 * \brief Set the hwFail flag
	 */
	void setHwFail() { hwFail_ = true; }
private:
	std::atomic<size_t> fireCount_;
	std::atomic<bool> hwFail_;
	artdaq::Fragment::timestamp_t ts_;
	bool hw_stop_;
};

artdaqtest::CommandableFragmentGeneratorTest::CommandableFragmentGeneratorTest(const fhicl::ParameterSet& ps)
	: CommandableFragmentGenerator(ps)
	, fireCount_(1)
	, hwFail_(false)
	, ts_(0)
	, hw_stop_(false)
{}

bool
artdaqtest::CommandableFragmentGeneratorTest::getNext_(artdaq::FragmentPtrs& frags)
{
	while (fireCount_ > 0)
	{
		frags.emplace_back(new artdaq::Fragment(ev_counter(), fragment_id(), artdaq::Fragment::FirstUserFragmentType, ++ts_));
		fireCount_--;
	}

	return !hw_stop_;
}

void
artdaqtest::CommandableFragmentGeneratorTest::start() { hw_stop_ = false; }

void
artdaqtest::CommandableFragmentGeneratorTest::stopNoMutex() {}

void
artdaqtest::CommandableFragmentGeneratorTest::stop() { hw_stop_ = true; }

void
artdaqtest::CommandableFragmentGeneratorTest::pause() {}

void
artdaqtest::CommandableFragmentGeneratorTest::resume() {}

BOOST_AUTO_TEST_SUITE(CommandableFragmentGenerator_t)

BOOST_AUTO_TEST_CASE(Simple)
{
	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
	TLOG_INFO("CommandableFragmentGenerator_t") << "Simple test case BEGIN" << TLOG_ENDL;
	fhicl::ParameterSet ps;
	ps.put<int>("board_id", 1);
	ps.put<int>("fragment_id", 1);
	artdaqtest::CommandableFragmentGeneratorTest testGen(ps);
	artdaq::FragmentPtrs fps;
	auto sts = testGen.getNext(fps);
	BOOST_REQUIRE_EQUAL(sts, true);
	BOOST_REQUIRE_EQUAL(fps.size(), 1u);
	BOOST_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	BOOST_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	BOOST_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	TLOG_INFO("CommandableFragmentGenerator_t") << "Simple test case END" << TLOG_ENDL;
}

BOOST_AUTO_TEST_CASE(IgnoreRequests)
{
	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
	TLOG_INFO("CommandableFragmentGenerator_t") << "IgnoreRequests test case BEGIN" << TLOG_ENDL;
	const int REQUEST_PORT = (seedAndRandom() % (32768 - 1024)) + 1024;
	const int DELAY_TIME = 1;
	fhicl::ParameterSet ps;
	ps.put<int>("board_id", 1);
	ps.put<int>("fragment_id", 1);
	ps.put<int>("request_port", REQUEST_PORT);
	ps.put<std::string>("request_address", "227.18.12.29");
	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
	ps.put<bool>("separate_data_thread", true);
	ps.put<bool>("separate_monitoring_thread", false);
	ps.put<int64_t>("hardware_poll_interval_us", 0);
	ps.put<std::string>("request_mode", "ignored");
	ps.put("request_delay_ms", DELAY_TIME);
	ps.put("send_requests", true);

	artdaq::RequestSender t(ps);
	artdaqtest::CommandableFragmentGeneratorTest gen(ps);
	gen.StartCmd(1, 0xFFFFFFFF, 1);
	t.AddRequest(53, 35);

	artdaq::FragmentPtrs fps;
	auto sts = gen.getNext(fps);
	BOOST_REQUIRE_EQUAL(sts, true);
	BOOST_REQUIRE_EQUAL(fps.size(), 1u);
	BOOST_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	BOOST_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	BOOST_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	gen.StopCmd(0xFFFFFFFF, 1);
	TLOG_INFO("CommandableFragmentGenerator_t") << "IgnoreRequests test case END" << TLOG_ENDL;
}

//BOOST_AUTO_TEST_CASE(SingleMode)
//{
//	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
//	TLOG_INFO("CommandableFragmentGenerator_t") << "SingleMode test case BEGIN" << TLOG_ENDL;
//	const int REQUEST_PORT = (seedAndRandom() % (32768 - 1024)) + 1024;
//	const int DELAY_TIME = 100;
//	fhicl::ParameterSet ps;
//	ps.put<int>("board_id", 1);
//	ps.put<int>("fragment_id", 1);
//	ps.put<int>("request_port", REQUEST_PORT);
//	ps.put<std::string>("request_address", "227.18.12.30");
//	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
//	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
//	ps.put<bool>("separate_data_thread", true);
//	ps.put<bool>("separate_monitoring_thread", false);
//	ps.put<int64_t>("hardware_poll_interval_us", 0); 
//	ps.put<std::string>("request_mode", "single");
//	ps.put("request_delay_ms", DELAY_TIME);
//	ps.put("send_requests", true);
//
//	artdaq::RequestSender t(ps);
//	t.AddRequest(1, 1);
//
//	artdaqtest::CommandableFragmentGeneratorTest gen(ps);
//	gen.StartCmd(1, 0xFFFFFFFF, 1);
//
//	artdaq::FragmentPtrs fps;
//	auto sts = gen.getNext(fps);
//	BOOST_REQUIRE_EQUAL(sts, true);
//	BOOST_REQUIRE_EQUAL(fps.size(), 1u);
//	BOOST_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
//	BOOST_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
//	BOOST_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
//	fps.clear();
//
//	gen.setFireCount(2);
//	t.AddRequest(3, 4);
//	sts = gen.getNext(fps);
//	BOOST_REQUIRE_EQUAL(sts, true);
//	BOOST_REQUIRE_EQUAL(fps.size(), 2u);
//	BOOST_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
//	BOOST_REQUIRE_EQUAL(fps.front()->timestamp(), 0);
//	BOOST_REQUIRE_EQUAL(fps.front()->sequenceID(), 2);
//	auto type = artdaq::Fragment::EmptyFragmentType;
//	BOOST_REQUIRE_EQUAL(fps.front()->type(), type);
//	fps.pop_front();
//	BOOST_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
//	BOOST_REQUIRE_EQUAL(fps.front()->timestamp(), 4);
//	BOOST_REQUIRE_EQUAL(fps.front()->sequenceID(), 3);
//	type = artdaq::Fragment::FirstUserFragmentType;
//	BOOST_REQUIRE_EQUAL(fps.front()->type(), type);
//	fps.clear();
//
//	gen.StopCmd(0xFFFFFFFF, 1);
//	TLOG_INFO("CommandableFragmentGenerator_t") << "SingleMode test case END" << TLOG_ENDL;
//}
//
//BOOST_AUTO_TEST_CASE(BufferMode)
//{
//	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
//	TLOG_INFO("CommandableFragmentGenerator_t") << "BufferMode test case BEGIN" << TLOG_ENDL;
//	const int REQUEST_PORT = (seedAndRandom() % (32768 - 1024)) + 1024;
//	const int DELAY_TIME = 100;
//	fhicl::ParameterSet ps;
//	ps.put<int>("board_id", 1);
//	ps.put<int>("fragment_id", 1);
//	ps.put<int>("request_port", REQUEST_PORT);
//	ps.put<std::string>("request_address", "227.18.12.31");
//	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
//	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
//	ps.put<bool>("separate_data_thread", true);
//	ps.put<bool>("separate_monitoring_thread", false);
//	ps.put<int64_t>("hardware_poll_interval_us", 0);
//	ps.put<std::string>("request_mode", "buffer");
//	ps.put("request_delay_ms", DELAY_TIME);
//	ps.put("send_requests", true);
//
//	artdaq::RequestSender t(ps);
//	artdaqtest::CommandableFragmentGeneratorTest gen(ps);
//	gen.StartCmd(1, 0xFFFFFFFF, 1);
//
//	artdaq::FragmentPtrs fps;
//	auto sts = gen.getNext(fps);
//	BOOST_REQUIRE_EQUAL(sts, true);
//	BOOST_REQUIRE_EQUAL(fps.size(), 1u);
//	BOOST_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
//	BOOST_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
//	BOOST_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
//	gen.StopCmd(0xFFFFFFFF, 1);
//
//
//	TLOG_INFO("CommandableFragmentGenerator_t") << "BufferMode test case END" << TLOG_ENDL;
//}

//BOOST_AUTO_TEST_CASE(WindowMode)
//{
//	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
//	TLOG_INFO("CommandableFragmentGenerator_t") << "WindowMode test case BEGIN" << TLOG_ENDL;
//	const int REQUEST_PORT = (seedAndRandom() % (32768 - 1024)) + 1024;
//	const int DELAY_TIME = 100;
//	fhicl::ParameterSet ps;
//	ps.put<int>("board_id", 1);
//	ps.put<int>("fragment_id", 1);
//	ps.put<int>("request_port", REQUEST_PORT);
//	ps.put<std::string>("request_address", "227.18.12.32");
//	ps.put<artdaq::Fragment::timestamp_t>("request_window_offset", 0);
//	ps.put<artdaq::Fragment::timestamp_t>("request_window_width", 0);
//	ps.put<bool>("separate_data_thread", true);
//	ps.put<bool>("separate_monitoring_thread", false);
//	ps.put<int64_t>("hardware_poll_interval_us", 0);
//	ps.put<std::string>("request_mode", "window");
//	ps.put("request_delay_ms", DELAY_TIME);
//	ps.put("send_requests", true);
//
//	artdaq::RequestSender t(ps);
//	artdaqtest::CommandableFragmentGeneratorTest gen(ps);
//	gen.StartCmd(1, 0xFFFFFFFF, 1);
//
//	artdaq::FragmentPtrs fps;
//	auto sts = gen.getNext(fps);
//	BOOST_REQUIRE_EQUAL(sts, true);
//	BOOST_REQUIRE_EQUAL(fps.size(), 1u);
//	BOOST_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
//	BOOST_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
//	BOOST_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
//	gen.StopCmd(0xFFFFFFFF, 1);
//	TLOG_INFO("CommandableFragmentGenerator_t") << "WindowMode test case END" << TLOG_ENDL;
//
//}

BOOST_AUTO_TEST_CASE(HardwareFailure_NonThreaded)
{
	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
	TLOG_INFO("CommandableFragmentGenerator_t") << "HardwareFailure_NonThreaded test case BEGIN" << TLOG_ENDL;
	fhicl::ParameterSet ps;
	ps.put<int>("board_id", 1);
	ps.put<int>("fragment_id", 1);
	ps.put<bool>("separate_data_thread", false);
	ps.put<bool>("separate_monitoring_thread", false);
	ps.put<int64_t>("hardware_poll_interval_us", 10);

	artdaqtest::CommandableFragmentGeneratorTest gen(ps);
	gen.StartCmd(1, 0xFFFFFFFF, 1);

	artdaq::FragmentPtrs fps;
	auto sts = gen.getNext(fps);
	BOOST_REQUIRE_EQUAL(sts, true);
	BOOST_REQUIRE_EQUAL(fps.size(), 1u);
	BOOST_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	BOOST_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	BOOST_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	fps.clear();

	gen.setFireCount(1);
	gen.setHwFail();
	usleep(10000);
	sts = gen.getNext(fps);
	BOOST_REQUIRE_EQUAL(sts, false);
	BOOST_REQUIRE_EQUAL(fps.size(), 0);

	gen.StopCmd(0xFFFFFFFF, 1);
	TLOG_INFO("CommandableFragmentGenerator_t") << "HardwareFailure_NonThreaded test case END" << TLOG_ENDL;
}

BOOST_AUTO_TEST_CASE(HardwareFailure_Threaded)
{
	artdaq::configureMessageFacility("CommandableFragmentGenerator_t");
	TLOG_INFO("CommandableFragmentGenerator_t") << "HardwareFailure_Threaded test case BEGIN" << TLOG_ENDL;
	fhicl::ParameterSet ps;
	ps.put<int>("board_id", 1);
	ps.put<int>("fragment_id", 1);
	ps.put<bool>("separate_data_thread", true);
	ps.put<bool>("separate_monitoring_thread", true);
	ps.put<int64_t>("hardware_poll_interval_us", 500000);

	artdaqtest::CommandableFragmentGeneratorTest gen(ps);
	gen.StartCmd(1, 0xFFFFFFFF, 1);



	artdaq::FragmentPtrs fps;
	auto sts = gen.getNext(fps);
	BOOST_REQUIRE_EQUAL(sts, true);
	BOOST_REQUIRE_EQUAL(fps.size(), 1u);
	BOOST_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	BOOST_REQUIRE_EQUAL(fps.front()->timestamp(), 1);
	BOOST_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);
	fps.clear();

	gen.setFireCount(1);
	gen.setHwFail();
	sts = gen.getNext(fps);
	BOOST_REQUIRE_EQUAL(sts, true);
	BOOST_REQUIRE_EQUAL(fps.size(), 1);
	BOOST_REQUIRE_EQUAL(fps.front()->fragmentID(), 1);
	BOOST_REQUIRE_EQUAL(fps.front()->timestamp(), 2);
	BOOST_REQUIRE_EQUAL(fps.front()->sequenceID(), 1);

	fps.clear();
	sleep(1);
	gen.setFireCount(1);
	sts = gen.getNext(fps);
	BOOST_REQUIRE_EQUAL(sts, false);
	BOOST_REQUIRE_EQUAL(fps.size(), 0);

	gen.StopCmd(0xFFFFFFFF, 1);
	TLOG_INFO("CommandableFragmentGenerator_t") << "HardwareFailure_Threaded test case END" << TLOG_ENDL;
}

BOOST_AUTO_TEST_SUITE_END()
