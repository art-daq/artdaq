////////////////////////////////////////////////////////////////////////
// Class:       OffsetPrescale
// Plugin Type: filter (Unknown Unknown)
// File:        OffsetPrescale_module.cc
//
// Generated at Mon Sep 11 12:28:36 2023 by Eric Flumerfelt using cetskelgen
// from  version .
////////////////////////////////////////////////////////////////////////

#include "artdaq/ArtModules/ArtdaqSharedMemoryServiceInterface.h"

#include "art/Framework/Core/EDFilter.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/Run.h"
#include "art/Framework/Principal/SubRun.h"
#include "canvas/Utilities/InputTag.h"
#include "fhiclcpp/ParameterSet.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include <memory>

namespace artdaq {
  class OffsetPrescale;
}


class artdaq::OffsetPrescale : public art::EDFilter {
public:
  explicit OffsetPrescale(fhicl::ParameterSet const& p);
  // The compiler-generated destructor is fine for non-base
  // classes without bare pointers or other resource use.

  // Plugins should not be copied or assigned.
  OffsetPrescale(OffsetPrescale const&) = delete;
  OffsetPrescale(OffsetPrescale&&) = delete;
  OffsetPrescale& operator=(OffsetPrescale const&) = delete;
  OffsetPrescale& operator=(OffsetPrescale&&) = delete;

  // Required functions.
  bool filter(art::Event& e) override;

private:

  // Declare member data here.
	size_t events_skip_;
	size_t offset_{0};

};


artdaq::OffsetPrescale::OffsetPrescale(fhicl::ParameterSet const& p)
  : EDFilter{p} 
	, events_skip_(p.get<size_t>("prescale", 1))
{
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
	offset_ = shm->GetMyId();
}

bool artdaq::OffsetPrescale::filter(art::Event& e)
{
	auto eid = e.event();
	return eid >= offset_ 
		&& (eid - offset_) % events_skip_ == 0;
}

DEFINE_ART_MODULE(artdaq::OffsetPrescale)
