#ifndef artdaq_Application_CompositeDriver_hh
#define artdaq_Application_CompositeDriver_hh

#include "fhiclcpp/fwd.h"
#include "artdaq/Application/CommandableFragmentGenerator.hh"
#include "artdaq-core/Data/Fragments.hh"
#include <vector>

namespace artdaq {
  // CompositeDriver handles a set of lower-level generators
  class CompositeDriver : public CommandableFragmentGenerator {
  public:
	explicit CompositeDriver(fhicl::ParameterSet const &);
	virtual ~CompositeDriver() noexcept;

	void start() override;
	void stopNoMutex() override;
	void stop() override;
	void pause() override;
	void resume() override;

    private:
      std::vector<artdaq::Fragment::fragment_id_t> fragmentIDs() override;
      bool getNext_(artdaq::FragmentPtrs & output) override;

      bool makeChildGenerator_(fhicl::ParameterSet const &);

      std::vector<std::unique_ptr<CommandableFragmentGenerator>> generator_list_;
      std::vector<bool> generator_active_list_;

  };
}
#endif /* artdaq_Application_CompositeDriver_hh */
