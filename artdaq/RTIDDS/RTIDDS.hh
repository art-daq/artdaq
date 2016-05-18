#ifndef artdaq_RTIDDS_RTIDDS_hh
#define artdaq_RTIDDS_RTIDDS_hh

#include "artdaq-core/Data/Fragment.hh"

#include <ndds/ndds_cpp.h>

#include <string>
#include <queue>
#include <mutex>


namespace artdaq {
  class RTIDDS;
}

class artdaq::RTIDDS {

public:

  RTIDDS(std::string name, std::string max_size = "1000000");
  ~RTIDDS() = default;

  // JCF, Apr-7-2016
  // Are copy constructor, assignment operators, etc., logical absurdities?

  void copyFragmentToDDS_(bool& fragmentHasBeenCopied,
			  bool& esrHasBeenCopied,
			  bool& eodHasBeenCopied,
			  artdaq::Fragment& fragment);

  class OctetsListener: public DDSDataReaderListener {
  public:

    void onDataAvailable(DDSDataReader *reader);

    void receiveFragmentFromDDS(artdaq::Fragment& fragment,
				size_t receiveTimeout);

  private:

    DDS_Octets dds_octets_;
    std::queue<DDS_Octets> dds_octets_queue_;

    std::mutex queue_mutex_;

  };

  OctetsListener octets_listener_;

private:

  std::string name_;
  std::string max_size_;

  std::unique_ptr<DDSDomainParticipant, std::function<void(DDSDomainParticipant*)> >  participant_;

  DDSTopic* topic_octets_;
  DDSOctetsDataWriter* octets_writer_;
  DDSDataReader* octets_reader_;

  static void participantDeleter(DDSDomainParticipant* participant);

};

#endif
