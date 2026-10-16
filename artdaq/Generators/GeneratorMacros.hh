#ifndef ARTDAQ_ARTDAQ_GENERATORS_GENERATORMACROS_HH_
#define ARTDAQ_ARTDAQ_GENERATORS_GENERATORMACROS_HH_

#include "artdaq/Generators/CommandableFragmentGenerator.hh"

namespace fhicl {
class ParameterSet;
}  // namespace fhicl

#include "cetlib/compiler_macros.h"

#include <memory>

namespace artdaq {
/**
 * \brief Constructs a CommandableFragmentGenerator instance, and returns a pointer to it
 * \param ps Parameter set for initializing the CommandableFragmentGenerator
 * \return A smart pointer to the CommandableFragmentGenerator
 */
typedef std::unique_ptr<artdaq::CommandableFragmentGenerator> makeFunc_t(fhicl::ParameterSet const& ps);
}  // namespace artdaq

#ifndef EXTERN_C_FUNC_DECLARE_START
// NOLINTNEXTLINE
#define EXTERN_C_FUNC_DECLARE_START extern "C" {
#endif

// NOLINTNEXTLINE
#define DEFINE_ARTDAQ_COMMANDABLE_GENERATOR(klass)                                   \
	EXTERN_C_FUNC_DECLARE_START                                                      \
	std::unique_ptr<artdaq::CommandableFragmentGenerator>                            \
	make(fhicl::ParameterSet const& ps)                                              \
	{                                                                                \
		return std::unique_ptr<artdaq::CommandableFragmentGenerator>(new klass(ps)); \
	}                                                                                \
	}

#endif  // ARTDAQ_ARTDAQ_GENERATORS_GENERATORMACROS_HH_
