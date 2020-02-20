#define TRACE_NAME "RequestMessage_t"
#include "artdaq/DAQdata/Globals.hh"

#define BOOST_TEST_MODULE RequestMessage_t
#include "artdaq/DAQrate/detail/RequestMessage.hh"
#include "cetlib/quiet_unit_test.hpp"
#include "cetlib_except/exception.h"

BOOST_AUTO_TEST_SUITE(RequestMessage_test)

BOOST_AUTO_TEST_CASE(RequestMessage)
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
	BOOST_REQUIRE_EQUAL(message.getRunNumber(), 101);
	BOOST_REQUIRE_EQUAL(message.getAcknowledge(), false);
	BOOST_REQUIRE_EQUAL(message.size(), 3);

	std::vector<uint8_t> buf = message.GetMessage();
	size_t serialized_message_size = buf.size();

	artdaq::detail::RequestMessage message_copy1(&buf[0], serialized_message_size);
	BOOST_REQUIRE_EQUAL(message_copy1.isValid(), true);
	BOOST_REQUIRE_EQUAL(message_copy1.size(), 3);

	artdaq::detail::RequestMessage message_copy2(&buf[0], sizeof(artdaq::detail::RequestHeader) - 1);
	BOOST_REQUIRE_EQUAL(message_copy2.isValid(), false);

	artdaq::detail::RequestMessage message_copy3(&buf[0], serialized_message_size - 1);
	BOOST_REQUIRE_EQUAL(message_copy3.isValid(), true);
	BOOST_REQUIRE_EQUAL(message_copy3.size(), 2);

	artdaq::detail::RequestMessage message_copy4(&buf[0], sizeof(artdaq::detail::RequestHeader) + 1);
	BOOST_REQUIRE_EQUAL(message_copy4.isValid(), true);
	BOOST_REQUIRE_EQUAL(message_copy4.size(), 0);

	for (size_t idx = sizeof(artdaq::detail::RequestHeader); idx < serialized_message_size; ++idx)
	{
		buf[idx] = 0;
	}
	artdaq::detail::RequestMessage message_copy5(&buf[0], serialized_message_size);
	BOOST_REQUIRE_EQUAL(message_copy5.isValid(), true);
	BOOST_REQUIRE_EQUAL(message_copy5.size(), 0);

	message_copy5.setAcknowledge(true);
	BOOST_REQUIRE_EQUAL(message_copy5.getAcknowledge(), true);
	message_copy5.setMode(artdaq::detail::RequestMessageMode::EndOfRun);
	BOOST_REQUIRE_EQUAL(message_copy5.getMode(), artdaq::detail::RequestMessageMode::EndOfRun);

	auto reqs = message_copy1.getRequests();
	message_copy5.addRequest(reqs[0]);
	BOOST_REQUIRE_EQUAL(message_copy5.isValid(), true);
	BOOST_REQUIRE_EQUAL(message_copy5.size(), 1);
}

BOOST_AUTO_TEST_CASE(RequestPacket)
{
	artdaq::detail::RequestPacket p;
	BOOST_REQUIRE_EQUAL(p.isValid(), false);

	artdaq::detail::RequestPacket p2(1, 2);
	BOOST_REQUIRE_EQUAL(p2.isValid(), true);
	BOOST_REQUIRE_EQUAL(p2.isActive(), false);
	BOOST_REQUIRE_EQUAL(p2.sequence_id, 1);
	BOOST_REQUIRE_EQUAL(p2.rank, -1);
	BOOST_REQUIRE_EQUAL(p2.timestamp, 2);
	BOOST_REQUIRE_EQUAL(p2.activeRanks.size(), 0);

	BOOST_REQUIRE_EQUAL(p2.hasRank(1), false);
	p2.setActiveRanks({1, 2, 3});
	BOOST_REQUIRE_EQUAL(p2.isActive(), true);
	BOOST_REQUIRE_EQUAL(p2.hasRank(1), true);
	p2.clearRank(1);
	BOOST_REQUIRE_EQUAL(p2.hasRank(1), false);

	auto vec = p2.ToByteVector();
	BOOST_REQUIRE_GT(vec.size(), 0);

	artdaq::detail::RequestPacket p3(&vec[0]);
	BOOST_REQUIRE_EQUAL(p3.isValid(), true);
	BOOST_REQUIRE_EQUAL(p3.isActive(), true);
	BOOST_REQUIRE_EQUAL(p3.sequence_id, 1);
	BOOST_REQUIRE_EQUAL(p3.rank, -1);
	BOOST_REQUIRE_EQUAL(p3.timestamp, 2);
	BOOST_REQUIRE_EQUAL(p3.hasRank(3), true);
}

BOOST_AUTO_TEST_CASE(RequestAcknowledgement)
{
	my_rank = 4;
	artdaq::detail::RequestAcknowledgement ra(1, 2);
	BOOST_REQUIRE_EQUAL(ra.header, 0x4042424B);
	BOOST_REQUIRE_EQUAL(ra.packet_count, 1);
	BOOST_REQUIRE_EQUAL(ra.run_number, 2);
	BOOST_REQUIRE_EQUAL(ra.rank, 4);
}

BOOST_AUTO_TEST_CASE(VectorBitset)
{
	artdaq::detail::VectorBitset vb;
	BOOST_REQUIRE_EQUAL(vb.size(), 0);

	// querying should not resize
	BOOST_REQUIRE_EQUAL(vb.at(0), false);
	BOOST_REQUIRE_EQUAL(vb.at(-1), false);
	BOOST_REQUIRE_EQUAL(vb.at(1), false);

	BOOST_REQUIRE_EQUAL(vb.size(), 0);

	BOOST_REQUIRE_EQUAL(vb.any(), false);

	BOOST_REQUIRE_EQUAL(vb.getSetBits().size(), 0);

	std::vector<uint8_t> testData = {0x1,
	                                 0x5,
	                                 0x7,
	                                 0xF};  // 0000 1111 0000 0111 0000 0101 0000 0001b

	artdaq::detail::VectorBitset vb2(testData);
	BOOST_REQUIRE_EQUAL(vb2.size(), 4);

	// querying should not resize
	BOOST_REQUIRE_EQUAL(vb2.at(0), true);
	BOOST_REQUIRE_EQUAL(vb2.at(-1), false);
	BOOST_REQUIRE_EQUAL(vb2.at(1), false);

	BOOST_REQUIRE_EQUAL(vb2.size(), 4);

	BOOST_REQUIRE_EQUAL(vb2.any(), true);

	auto setBits = vb2.getSetBits();
	BOOST_REQUIRE_EQUAL(setBits.size(), 10);
	BOOST_REQUIRE_EQUAL(setBits[0], 0);
	BOOST_REQUIRE_EQUAL(setBits[1], 8);
	BOOST_REQUIRE_EQUAL(setBits[2], 10);
	BOOST_REQUIRE_EQUAL(setBits[3], 16);
	BOOST_REQUIRE_EQUAL(setBits[4], 17);
	BOOST_REQUIRE_EQUAL(setBits[5], 18);
	BOOST_REQUIRE_EQUAL(setBits[6], 24);
	BOOST_REQUIRE_EQUAL(setBits[7], 25);
	BOOST_REQUIRE_EQUAL(setBits[8], 26);
	BOOST_REQUIRE_EQUAL(setBits[9], 27);

	vb2.clear(0);
	BOOST_REQUIRE_EQUAL(vb2.at(0), false);
	vb2.set(0);
	BOOST_REQUIRE_EQUAL(vb2.at(0), true);

	vb2.set(32);
	BOOST_REQUIRE_EQUAL(vb2.size(), 5);
}

BOOST_AUTO_TEST_SUITE_END()
