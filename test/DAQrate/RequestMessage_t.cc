#define TRACE_NAME "RequestMessage_t"
#include "artdaq/DAQdata/Globals.hh"

#define BOOST_TEST_MODULE RequestMessage_t
#include "artdaq/DAQrate/detail/RequestMessage.hh"
#include "cetlib/quiet_unit_test.hpp"
#include "cetlib_except/exception.h"

BOOST_AUTO_TEST_SUITE(RequestMessage_test)

BOOST_AUTO_TEST_CASE(Construct)
{
	artdaq::detail::RequestMessage message;
	message.setRunNumber(101);
	message.addRequest(1, 1001, 5);
	message.addRequest(2, 2002, 5);
	message.addRequest(3, 3003, 5);

	//artdaq::detail::RequestMessage message(&buffer[0], sts);

	BOOST_REQUIRE_EQUAL(message.isValid(), true);
	BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(message.getMode()),
	                    static_cast<uint8_t>(artdaq::detail::RequestMessageMode::Normal));
	BOOST_REQUIRE_EQUAL(message.size(), 3);

	std::vector<uint8_t> buf = message.GetMessage();
	size_t serialized_message_size = buf.size();

	artdaq::detail::RequestMessage message_copy1(&buf[0], serialized_message_size);
	BOOST_REQUIRE_EQUAL(message_copy1.isValid(), true);
	BOOST_REQUIRE_EQUAL(message_copy1.size(), 3);

	artdaq::detail::RequestMessage message_copy2(&buf[0], sizeof(artdaq::detail::RequestHeader)-1);
	BOOST_REQUIRE_EQUAL(message_copy2.isValid(), false);

	artdaq::detail::RequestMessage message_copy3(&buf[0], serialized_message_size-1);
	BOOST_REQUIRE_EQUAL(message_copy3.isValid(), true);
	BOOST_REQUIRE_EQUAL(message_copy3.size(), 2);

	artdaq::detail::RequestMessage message_copy4(&buf[0], sizeof(artdaq::detail::RequestHeader)+1);
	BOOST_REQUIRE_EQUAL(message_copy4.isValid(), true);
	BOOST_REQUIRE_EQUAL(message_copy4.size(), 0);

	for (size_t idx=sizeof(artdaq::detail::RequestHeader); idx<serialized_message_size; ++idx)
	{
		buf[idx] = 0;
	}
	artdaq::detail::RequestMessage message_copy5(&buf[0], serialized_message_size);
	BOOST_REQUIRE_EQUAL(message_copy5.isValid(), true);
	BOOST_REQUIRE_EQUAL(message_copy5.size(), 0);
}

BOOST_AUTO_TEST_SUITE_END()
