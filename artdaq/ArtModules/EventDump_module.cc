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

#include "artdaq-core/Data/Fragment.hh"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <vector>
#include <iostream>

namespace artdaq
{
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
	 * \endverbatim
	 */
	explicit EventDump(fhicl::ParameterSet const& pset);

	/**
	 * \brief Default virtual Destructor
	 */
	virtual ~EventDump() = default;

	/**
	 * \brief This method is called for each art::Event in a file or run
	 * \param e The art::Event to analyze
	 * 
	 * This module simply prints the event number, and art by default
	 * prints the products found in the event.
	 */
	void analyze(art::Event const& e) override;

private:
	std::string raw_data_label_;
};


artdaq::EventDump::EventDump(fhicl::ParameterSet const& pset)
	: EDAnalyzer(pset)
	, raw_data_label_(pset.get<std::string>("raw_data_label", "daq")) {}

void artdaq::EventDump::analyze(art::Event const& e)
{
	mf::LogDebug("EventDump") << "Dumping Event " << e.event();
}

DEFINE_ART_MODULE(artdaq::EventDump)
