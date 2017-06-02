#ifndef artdaq_ArtModules_NetMonWrapper_hh
#define artdaq_ArtModules_NetMonWrapper_hh


#include "artdaq/ArtModules/NetMonTransportService.h"

#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "fhiclcpp/fwd.h"

#include <TBufferFile.h>

#include <string>
#include <memory>

namespace art
{
	/**
	 * \brief This class wraps NetMonTransportService so that it can act as an ArtdaqInput
	 * template class.
	 * 
	 * JCF, May-27-2016
	 *
	 * This class is written with functionality such that it satisfies the
	 * requirements needed to be a template in the ArtdaqInput class
	 */
	class NetMonWrapper
	{
	public:
		
		/**
		 * \brief NetMonWrapper Constructor
		 *
		 * JCF, May-31-2016
		 *
		 * Parameter set constructor argument is unused for now, but
		 * needed for this class to implement the interface the
		 * ArtdaqInput templatized input source expects
		 */
		NetMonWrapper(const fhicl::ParameterSet&)
		{
			ServiceHandle<NetMonTransportService> transport;
			transport->listen();
		}

		/**
		 * \brief NetMonWrapper Destructor
		 */
		~NetMonWrapper()
		{
			ServiceHandle<NetMonTransportService> transport;
			transport->disconnect();
		}

		/**
		 * \brief Receive a message from the NetMonTransportService
		 * \param[out] msg A pointer to the received message
		 */
		void receiveMessage(std::unique_ptr<TBufferFile>& msg);
	};
}

#endif /* artdaq_ArtModules_NetMonWrapper_hh */
