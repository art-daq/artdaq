#include "artdaq/Application/makeTriggeredFragmentGenerator.hh"

#include "artdaq/Application/GeneratorMacros.hh"
#include "fhiclcpp/ParameterSet.h"
#include "cetlib/BasicPluginFactory.h"

std::unique_ptr<artdaq::TriggeredFragmentGenerator>
artdaq::makeTriggeredFragmentGenerator(std::string const & generator_plugin_spec,
                              fhicl::ParameterSet const & ps)
{
  static cet::BasicPluginFactory bpf("tgdgenerator", "make");

  return bpf.makePlugin<std::unique_ptr<artdaq::TriggeredFragmentGenerator>,
    fhicl::ParameterSet const &>(generator_plugin_spec, ps);
}
