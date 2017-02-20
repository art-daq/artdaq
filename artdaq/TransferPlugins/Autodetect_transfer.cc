#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/TransferPlugins/TCPSocketTransfer.hh"
#include "artdaq/TransferPlugins/ShmemTransfer.hh"

namespace artdaq {

  class AutodetectTransfer : public TransferInterface {
  public:
    AutodetectTransfer(const fhicl::ParameterSet&, Role);
    ~AutodetectTransfer() = default;

    virtual int receiveFragment(artdaq::Fragment& fragment,
				size_t receiveTimeout) 
    { return theTransfer_->receiveFragment(fragment, receiveTimeout); }

    virtual CopyStatus copyFragment(artdaq::Fragment& fragment,
				    size_t send_timeout_usec = std::numeric_limits<size_t>::max())
    { return theTransfer_->copyFragment(fragment, send_timeout_usec); }
    virtual CopyStatus moveFragment(artdaq::Fragment&& fragment,
				    size_t send_timeout_usec = std::numeric_limits<size_t>::max())
    { return theTransfer_->moveFragment(std::move(fragment), send_timeout_usec); }
  private:
    std::unique_ptr<TransferInterface> theTransfer_;
  };

}

artdaq::AutodetectTransfer::AutodetectTransfer(const fhicl::ParameterSet& pset, Role role)
  : TransferInterface(pset, role)
{
  mf::LogDebug(uniqueLabel()) << "Begin AutodetectTransfer constructor";
  std::string srcHost, destHost;
 	auto hosts = pset.get<std::vector<fhicl::ParameterSet>>("host_map");
	for (auto& ps : hosts) {
		auto rank = ps.get<int>("rank", -1);
		if(rank == source_rank()) {
		  srcHost = ps.get<std::string>("host", "localhost");
		}
		if(rank == destination_rank()) {
		  destHost = ps.get<std::string>("host", "localhost");
		}
	}
	mf::LogDebug(uniqueLabel()) << "ADT: srcHost=" << srcHost << ", destHost=" << destHost;
	if(srcHost == destHost) {
	  mf::LogDebug(uniqueLabel()) << "ADT: Constructing ShmemTransfer";
	  theTransfer_.reset(new ShmemTransfer(pset, role));
	} else {
	  mf::LogDebug(uniqueLabel()) << "ADT: Constructing TCPSocketTransfer";
	  theTransfer_.reset(new TCPSocketTransfer(pset,role));
	}
}

DEFINE_ARTDAQ_TRANSFER(artdaq::AutodetectTransfer)