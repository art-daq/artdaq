#ifndef artdaq_DAQrate_detail_MergeParameterSets_hh
#define artdaq_DAQrate_detail_MergeParameterSets_hh

#include <fhiclcpp/ParameterSet.h>

namespace artdaq {
static fhicl::ParameterSet merge(fhicl::ParameterSet first, fhicl::ParameterSet second)
{
	auto first_str = first.to_string();
	auto second_str = second.to_string();

	auto combined = first_str + " " + second_str;

	return fhicl::ParameterSet::make(combined);
}
}  // namespace artdaq

#endif  // artdaq_DAQrate_detail_MergeParameterSets_hh
