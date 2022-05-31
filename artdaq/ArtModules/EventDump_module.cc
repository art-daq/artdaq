////////////////////////////////////////////////////////////////////////
// Class:       EventDump
// Module Type: analyzer
// File:        EventDump_module.cc
// Description: Prints out information about each event.
////////////////////////////////////////////////////////////////////////

#include "art/Framework/Core/EDAnalyzer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/Handle.h"
#include "canvas/Utilities/Exception.h"

#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <vector>

namespace artdaq {
class EventDump;
}

/**
 * \brief Write Event information to the console
 */
class artdaq::EventDump : public art::EDAnalyzer
{
public:
	/**
	 * \brief EventDump Constructor
	 * \param pset ParameterSet used to configure EventDump
	 * 
	 * \verbatim
	 * EventDump accepts the following Parameters:
	 * "raw_data_label" (Default: "daq"): The label used to store artdaq data
	 * "verbosity" (Default: 0): verboseness level
	 * \endverbatim
	 */
	explicit EventDump(fhicl::ParameterSet const& pset);

	/**
	 * \brief Default virtual Destructor
	 */
	~EventDump() override = default;

	/**
	 * \brief This method is called for each art::Event in a file or run
	 * \param e The art::Event to analyze
	 * 
	 * This module simply prints the event number, and art by default
	 * prints the products found in the event.
	 */
	void analyze(art::Event const& e) override;

private:
	EventDump(EventDump const&) = delete;
	EventDump(EventDump&&) = delete;
	EventDump& operator=(EventDump const&) = delete;
	EventDump& operator=(EventDump&&) = delete;

	std::string raw_data_label_;
	int verbosity_;
};

artdaq::EventDump::EventDump(fhicl::ParameterSet const& pset)
    : EDAnalyzer(pset)
    , raw_data_label_(pset.get<std::string>("raw_data_label", "daq"))
    , verbosity_(pset.get<int>("verbosity", 0)) {}

void artdaq::EventDump::analyze(art::Event const& e)
{
	if (verbosity_ > 0)
	{
		std::cout << "***** Start of EventDump for event " << e.event() << " *****" << std::endl;

		std::vector<art::Handle<detail::RawEventHeader>> header_handles;
		header_handles = e.getMany<detail::RawEventHeader>();

		for (auto const& header_handle : header_handles)
		{
			std::ostringstream ostr;
			header_handle->print(ostr);
			std::cout << "Event Header from " << header_handle.provenance()->processName() << ": " << ostr.str() << std::endl;
		}
		if (header_handles.empty())
		{
			std::cout << "Unable to read RawEventHeader for event " << e.event() << std::endl;
		}

		std::vector<art::Handle<std::vector<artdaq::Fragment>>> fragmentHandles;
		fragmentHandles = e.getMany<std::vector<artdaq::Fragment>>();

		for (auto const& handle : fragmentHandles)
		{
			if (!handle->empty())
			{
				std::string instance_name = handle.provenance()->productInstanceName();
				std::cout << instance_name << " fragments: " << std::endl;

				int jdx = 1;
				for (auto const& frag : *handle)
				{
					std::cout << "  " << jdx << ") fragment ID " << frag.fragmentID() << " has type "
					          << static_cast<int>(frag.type()) << ", timestamp " << frag.timestamp()
					          << ", has metadata " << std::boolalpha << frag.hasMetadata()
					          << ", and sizeBytes " << frag.sizeBytes()
					          << " (hdr=" << frag.headerSizeBytes()
					          << ", data=" << frag.dataSizeBytes()
					          << ", meta (calculated)=" << (frag.sizeBytes() - frag.headerSizeBytes() - frag.dataSizeBytes())
					          << ")";

					if (instance_name.compare(0, 9, "Container") == 0)
					{
						artdaq::ContainerFragment cf(frag);
						std::cout << " (contents: type = " << static_cast<int>(cf.fragment_type()) << ", count = "
						          << cf.block_count() << ", missing data = " << cf.missing_data()
						          << ")" << std::endl;
						;
						if (verbosity_ > 1)
						{
							for (size_t idx = 0; idx < cf.block_count(); ++idx)
							{
								auto thisFrag = cf.at(idx);
								std::cout << "    " << (idx + 1) << ") fragment type " << static_cast<int>(thisFrag->type())
								          << ", timestamp " << thisFrag->timestamp()
								          << ", has metadata " << std::boolalpha << thisFrag->hasMetadata()
								          << ", and sizeBytes " << thisFrag->sizeBytes()
								          << " (hdr=" << thisFrag->headerSizeBytes()
								          << ", data=" << thisFrag->dataSizeBytes()
								          << ", meta (calculated)=" << (thisFrag->sizeBytes() - thisFrag->headerSizeBytes() - thisFrag->dataSizeBytes())
								          << ")" << std::endl;
							}
						}
					}
					else
					{
						std::cout << std::endl;
					}
					++jdx;
				}
			}
		}

		std::cout << "***** End of EventDump for event " << e.event() << " *****" << std::endl;
	}
}

DEFINE_ART_MODULE(artdaq::EventDump)  // NOLINT(performance-unnecessary-value-param)
