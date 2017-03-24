#ifndef artdaq_Application_GeneratorMacros_hh
#define artdaq_Application_GeneratorMacros_hh

#include "artdaq/Application/CommandableFragmentGenerator.hh"
#include "fhiclcpp/fwd.h"

#include <memory>

namespace artdaq
{
	typedef std::unique_ptr<artdaq::CommandableFragmentGenerator>
	(makeFunc_t)(fhicl::ParameterSet const& ps);
}

#define DEFINE_ARTDAQ_COMMANDABLE_GENERATOR(klass)                    \
  extern "C"                                                          \
  std::unique_ptr<artdaq::CommandableFragmentGenerator>               \
  make(fhicl::ParameterSet const & ps) {                              \
    return std::unique_ptr<artdaq::CommandableFragmentGenerator>(new klass(ps)); \
  }

#endif /* artdaq_Application_GeneratorMacros_hh */
