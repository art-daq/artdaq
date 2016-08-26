#ifndef artdaq_DAQrate_detail_TriggerMessage_hh
#define artdaq_DAQrate_detail_TriggerMessage_hh

#include "artdaq-core/Data/Fragment.hh"

namespace artdaq {
  namespace detail {
    struct TriggerPacket;
	class TriggerMessage;
  }
}

struct artdaq::detail::TriggerPacket
{
public:
  uint32_t header; //TRIG, or 0x54524947
  Fragment::sequence_id_t sequence_id;
  Fragment::timestamp_t timestamp;
};

class artdaq::detail::TriggerMessage
{
public:
  TriggerMessage()
  {
    packet_.header = 0;
	packet_.sequence_id = Fragment::InvalidSequenceID;
	packet_.timestamp = Fragment::InvalidTimestamp;
  }
  TriggerMessage(Fragment::sequence_id_t seq, Fragment::timestamp_t time)
  {
	packet_.header = 0x54524947;
	packet_.sequence_id = seq;
	packet_.timestamp = time;
  }
  TriggerMessage(TriggerPacket& input)
  {
	packet_.header = input.header;
	packet_.sequence_id = input.sequence_id;
	packet_.timestamp = input.timestamp;
  }

  TriggerPacket* buffer() { return &packet_; }
  Fragment::sequence_id_t sequence_id() const { return packet_.sequence_id; }
  Fragment::timestamp_t timestamp() const { return packet_.timestamp; }

  void setSequenceID(Fragment::sequence_id_t seq) { packet_.sequence_id = seq; }
  void setTimestamp(Fragment::timestamp_t time) { packet_.timestamp = time; }

  bool isValid() { return packet_.header == 0x54524947; }

private:
  TriggerPacket packet_;
};

#endif // artdaq_DAQrate_detail_TriggerMessage
