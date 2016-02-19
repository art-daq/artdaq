#include "art/Framework/Core/EDAnalyzer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/Run.h"
#include "art/Framework/Principal/SubRun.h"
#include "art/Persistency/Provenance/BranchType.h"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/Fragments.hh"
#include <memory>
#include "fhiclcpp/ParameterSet.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include <string>

namespace artdaq {

  class FragmentSniffer : public art::EDAnalyzer {
  public:
    explicit FragmentSniffer(fhicl::ParameterSet const & p);
    virtual ~FragmentSniffer() { };

    virtual void analyze(art::Event const & e);
    virtual void endSubRun(art::SubRun const & sr);
    virtual void endRun(art::Run const & r);
    virtual void endJob();

  private:
    std::string raw_label_;
    std::string product_instance_name_;
    std::size_t num_frags_per_event_;
    std::size_t num_events_expected_;
    std::size_t num_events_processed_;
  };

  FragmentSniffer::FragmentSniffer(fhicl::ParameterSet const & p) :
    art::EDAnalyzer(p),
    raw_label_(p.get<std::string>("raw_label")),
    product_instance_name_(p.get<std::string>("product_instance_name")),
    num_frags_per_event_(p.get<size_t>("num_frags_per_event")),
    num_events_expected_(p.get<size_t>("num_events_expected")),
    num_events_processed_()
  {
  }

  void FragmentSniffer::analyze(art::Event const & e)
  {
    art::Handle<Fragments> handle;
    e.getByLabel(raw_label_, product_instance_name_, handle);
    assert(handle->empty() || "getByLabel returned empty handle");
    assert(handle->size() == num_frags_per_event_);
    ++num_events_processed_;
  }

  void FragmentSniffer::endSubRun(art::SubRun const &) { }
  void FragmentSniffer::endRun(art::Run const &) { }
  void FragmentSniffer::endJob()
  {
    mf::LogInfo("Progress") << "events processed: "
                            << num_events_processed_
                            << "\nevents expected:  "
                            << num_events_expected_;
    assert(num_events_processed_ == num_events_expected_);
  }

  DEFINE_ART_MODULE(FragmentSniffer)
}

