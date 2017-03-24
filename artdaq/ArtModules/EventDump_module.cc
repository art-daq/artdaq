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

namespace demo
{
	class EventDump;
}

class demo::EventDump : public art::EDAnalyzer
{
public:
	explicit EventDump(fhicl::ParameterSet const& pset);

	virtual ~EventDump();

	virtual void analyze(art::Event const&);

private:
	std::string raw_data_label_;
};


demo::EventDump::EventDump(fhicl::ParameterSet const& pset)
	: EDAnalyzer(pset)
	, raw_data_label_(pset.get<std::string>("raw_data_label")) {}

demo::EventDump::~EventDump() {}

void demo::EventDump::analyze(art::Event const&) {}

DEFINE_ART_MODULE(demo::EventDump)
