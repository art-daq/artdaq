#ifndef artdaq_ArtModules_TransferInterface_hh
#define artdaq_ArtModules_TransferInterface_hh

#include "artdaq-core/Data/Fragment.hh"
#include "fhiclcpp/ParameterSet.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include <limits>
#include <iostream>
#include <sstream>

namespace artdaq {

	class TransferInterface {
	public:
		static const size_t RECV_TIMEOUT = 0xfedcba98;
		static size_t my_rank;

		enum class Role { kSend, kReceive };

		enum class CopyStatus { kSuccess, kTimeout, kErrorNotRequiringException };

		TransferInterface(const fhicl::ParameterSet& ps, Role role);

		TransferInterface(const TransferInterface&) = delete;
		TransferInterface& operator=(const TransferInterface&) = delete;

		virtual size_t receiveFragment(artdaq::Fragment& fragment,
			size_t receiveTimeout) = 0;

		// Copy fragment (maybe not reliable)
		virtual CopyStatus copyFragment(artdaq::Fragment& fragment,
			size_t send_timeout_usec = std::numeric_limits<size_t>::max()) = 0;

		// Move fragment (should be reliable)
		virtual CopyStatus moveFragment(artdaq::Fragment&& fragment,
			size_t send_timeout_usec = std::numeric_limits<size_t>::max()) = 0;

		std::string uniqueLabel() const { return unique_label_; }

		size_t source_rank() const { return source_rank_; }
		size_t destination_rank() const { return destination_rank_; }
	private:
		const Role role_;
		
		const size_t source_rank_;
		const size_t destination_rank_;
		const std::string unique_label_;

	protected:
		size_t buffer_count_;
		const size_t max_fragment_size_words_;
	protected:
		Role role() const { return role_; }
	};

}

#define DEFINE_ARTDAQ_TRANSFER(klass)                                \
  extern "C" std::unique_ptr<artdaq::TransferInterface> make(fhicl::ParameterSet const & ps, \
							     artdaq::TransferInterface::Role role) { \
    return std::unique_ptr<artdaq::TransferInterface>(new klass(ps, role)); \
}


#endif /* artdaq_ArtModules_TransferInterface.hh */

// Local Variables:
// mode: c++
// End:
