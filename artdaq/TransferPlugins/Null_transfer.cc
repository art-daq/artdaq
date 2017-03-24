#include "artdaq/TransferPlugins/TransferInterface.hh"

namespace artdaq
{
	class NullTransfer : public TransferInterface
	{
	public:
		NullTransfer(const fhicl::ParameterSet&, Role);

		~NullTransfer() = default;

		virtual int receiveFragment(artdaq::Fragment&,
		                            size_t)
		{
			return source_rank();
		}

		virtual CopyStatus copyFragment(artdaq::Fragment&,
		                                size_t = std::numeric_limits<size_t>::max())
		{
			return CopyStatus::kSuccess;
		}

		virtual CopyStatus moveFragment(artdaq::Fragment&&,
		                                size_t = std::numeric_limits<size_t>::max())
		{
			return CopyStatus::kSuccess;
		}

	private:
		std::unique_ptr<TransferInterface> theTransfer_;
	};
}

artdaq::NullTransfer::NullTransfer(const fhicl::ParameterSet& pset, Role role)
	: TransferInterface(pset, role) {}

DEFINE_ARTDAQ_TRANSFER(artdaq::NullTransfer)
