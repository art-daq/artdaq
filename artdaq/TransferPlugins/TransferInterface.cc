#define TRACE_NAME "TransferInterface"
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "cetlib_except/exception.h"

artdaq::TransferInterface::TransferInterface(const fhicl::ParameterSet& ps, Role role)
	: role_(role)
	, source_rank_(ps.get<int>("source_rank", my_rank))
	, destination_rank_(ps.get<int>("destination_rank", my_rank))
	, unique_label_(ps.get<std::string>("unique_label", "transfer_between_" + std::to_string(source_rank_) + "_and_" + std::to_string(destination_rank_)))
	, buffer_count_(ps.get<size_t>("buffer_count", 10))
	, max_fragment_size_words_(ps.get<size_t>("max_fragment_size_words", 1024))
{
	TLOG_DEBUG("TransferInterface") << uniqueLabel() << " role:"<<(int)role<<" TransferInterface constructor has "
									<< ps.to_string() << TLOG_ENDL;
}

int artdaq::TransferInterface::receiveFragment(artdaq::Fragment& frag, size_t receive_timeout)
{
	auto ret = RECV_TIMEOUT;

	TLOG_TRACE("TransferInterface") << "Receiving Fragment Header from rank " << source_rank() << TLOG_ENDL;
	ret = receiveFragmentHeader(*reinterpret_cast<detail::RawFragmentHeader*>(frag.headerAddress()), receive_timeout);
	
	TLOG_TRACE("TransferInterface") << "Done receiving Header, ret is " << ret << ", should be " << source_rank() << TLOG_ENDL;
	if (ret == RECV_TIMEOUT) return ret;

	frag.autoResize();
	
	TLOG_TRACE("TransferInterface") << "Receiving Fragment Body from rank " << source_rank() << TLOG_ENDL;
	auto bodyret = receiveFragmentData(frag.headerAddress() + detail::RawFragmentHeader::num_words(), frag.sizeBytes() - detail::RawFragmentHeader::num_words() * sizeof(RawDataType));
	TLOG_TRACE("TransferInterface") << "Done receiving Body, ret is " << bodyret << ", should be " << source_rank() << TLOG_ENDL;

	if (bodyret != ret) throw cet::exception("TransferInterface") << "Got different return codes from receiveFragmentHeader and receiveFragmentData!";

	return ret;
}
