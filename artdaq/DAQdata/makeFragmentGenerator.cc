#include "artdaq/DAQdata/makeFragmentGenerator.hh"

#include "cetlib/LibraryManager.h"
#include "art/Utilities/Exception.h"
#include "artdaq/DAQdata/GeneratorMacros.hh"
#include "fhiclcpp/ParameterSet.h"

std::unique_ptr<artdaq::FragmentGenerator>
artdaq::makeFragmentGenerator(std::string const & generator_plugin_spec,
                              fhicl::ParameterSet const & ps)
{
  static cet::LibraryManager lm("generator");
  artdaq::makeFunc_t *makeFunc (nullptr);
  try {
    lm.getSymbolByLibspec(generator_plugin_spec,
                          "make",
                          makeFunc);
  }
  catch (art::Exception & e) {
    // FIXME: this is mostly the contents of
    // art::detail::wrapLibraryManagerException, excpt that we can't use
    // that for linkage reasons.
    if (e.categoryCode() == art::errors::LogicError) {
      // Rethrow.
      throw;
    } else {
      // Wrap and throw.
      throw art::Exception(art::errors::Configuration,
                           "Unknown generator",
                           e)
        << "Generator " << generator_plugin_spec
        << " was not registered.\n"
        << "Perhaps your generator type was misspelled or is not a framework plugin.";
    }
  }
  if (makeFunc == nullptr) {
    throw art::Exception(art::errors::Configuration, "BadPluginLibrary")
      << "Generator "
      << generator_plugin_spec
      << " has internal symbol definition problems: consult an expert.";
  }
  return makeFunc(ps);
}
