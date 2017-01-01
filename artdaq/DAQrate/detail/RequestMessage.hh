#ifndef artdaq_DAQrate_detail_RequestMessage_hh
#define artdaq_DAQrate_detail_RequestMessage_hh

#include "artdaq-core/Data/Fragment.hh"

namespace artdaq {
  namespace detail {
	struct RequestPacket;
	struct RequestHeader;
	class RequestMessage;
  }
}

struct artdaq::detail::RequestPacket
{
public:
  uint32_t header; //TRIG, or 0x54524947
  Fragment::sequence_id_t sequence_id;
  Fragment::timestamp_t timestamp;

  RequestPacket() 
	: header(0)
	, sequence_id(Fragment::InvalidSequenceID)
	, timestamp(Fragment::InvalidTimestamp)
  {}

  RequestPacket(const Fragment::sequence_id_t& seq, const Fragment::timestamp_t& ts)
	: header(0x54524947)
	, sequence_id(seq)
	, timestamp(ts) 
  {}

  bool isValid() const { return header == 0x54524947; }
};

struct artdaq::detail::RequestHeader
{
  uint32_t header; //HEDR, or 0x48454452
  uint32_t packet_count;

  RequestHeader() : header(0x48454452), packet_count(0) {}

  bool isValid() const { return header == 0x48454452; }
};

class artdaq::detail::RequestMessage
{
public:
  RequestMessage() : header_(), packets_() { }
 
  RequestHeader* header() { 
	header_.packet_count = packets_.size();
	return &header_; 
  }

  RequestPacket* buffer() { return &packets_[0]; }
  size_t size() const { return packets_.size(); }

  void addRequest(const Fragment::sequence_id_t& seq, const Fragment::timestamp_t& time) {
	packets_.emplace_back(RequestPacket(seq,time));
  }

private:
  RequestHeader header_;
  std::vector<RequestPacket> packets_;
};

#endif // artdaq_DAQrate_detail_RequestMessage
