#ifndef artdaq_ArtModules_NetMonWrapper_hh
#define artdaq_ArtModules_NetMonWrapper_hh

#include "artdaq/ArtModules/NetMonTransportService.h"

#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "fhiclcpp/fwd.h"

#include <TBufferFile.h>

#include <memory>
#include <string>

namespace art {
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
		 * \param pset ParameterSet for NetMonWrapper
		 */
	NetMonWrapper(const fhicl::ParameterSet& pset)
	    : transport_(new NetMonTransportService(pset))
	{
		transport_->listen();

		try
		{
			if (metricMan)
			{
				metricMan->initialize(pset.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet()), app_name);
				metricMan->do_start();
			}
		}
		catch (...)
		{
			artdaq::ExceptionHandler(artdaq::ExceptionHandlerRethrow::no, "Error loading metrics in NetMonWrapper");
		}
	}

	/**
		 * \brief NetMonWrapper Destructor
		 */
	~NetMonWrapper()
	{
		transport_->disconnect();
		transport_.reset(nullptr);
		artdaq::Globals::CleanUpGlobals();
	}

	/**
		 * \brief Receive a message from the NetMonTransportService
		 * \param[out] msg A pointer to the received message
		 */
	void receiveMessage(std::list<std::unique_ptr<TBufferFile>>& msgs);

	/**
		* \brief Receive an init message from the NetMonTransportService
		* \param[out] msg A pointer to the received message
		*/
	void receiveInitMessage(std::unique_ptr<TBufferFile>& msg);

private:
	std::unique_ptr<NetMonTransportService> transport_;
};
}  // namespace art

#endif /* artdaq_ArtModules_NetMonWrapper_hh */
