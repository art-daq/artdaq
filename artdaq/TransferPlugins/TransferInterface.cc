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
	, partition_number_(ps.get<short>("partition_number", GetPartitionNumber()))
{
	TLOG(TLVL_DEBUG) << uniqueLabel() << " role:"<<(int)role<<" TransferInterface constructor has "
									<< ps.to_string() ;
}

int artdaq::TransferInterface::receiveFragment(artdaq::Fragment& frag, size_t receive_timeout)
{
	auto ret = RECV_TIMEOUT;

	TLOG(TLVL_TRACE) << "Receiving Fragment Header from rank " << source_rank() ;
	ret = receiveFragmentHeader(*reinterpret_cast<detail::RawFragmentHeader*>(frag.headerAddress()), receive_timeout);
	
	TLOG(TLVL_TRACE) << "Done receiving Header, ret is " << ret << ", should be " << source_rank() ;
	if (ret < RECV_SUCCESS) return ret;

	frag.autoResize();
	
	TLOG(TLVL_TRACE) << "Receiving Fragment Body from rank " << source_rank() ;
	auto bodyret = receiveFragmentData(frag.headerAddress() + detail::RawFragmentHeader::num_words(), frag.sizeBytes() - detail::RawFragmentHeader::num_words() * sizeof(RawDataType));
	TLOG(TLVL_TRACE) << "Done receiving Body, ret is " << bodyret << ", should be " << source_rank() ;

	if (bodyret != ret) throw cet::exception("TransferInterface") << "Got different return codes from receiveFragmentHeader and receiveFragmentData!";

	return ret;
}

int artdaq::TransferInterface::GetPartitionNumber() const
{
	auto part = getenv("ARTDAQ_PARTITION_NUMBER"); // 0-127
	uint32_t part_u = 0;
	if (part != nullptr)
	{
		try {
			auto part_s = std::string(part);
			part_u = static_cast<uint32_t>(std::stoll(part_s, 0, 0));
		}
		catch (std::invalid_argument) {}
		catch (std::out_of_range) {}
	}

	return (part_u & 0x7F);
}