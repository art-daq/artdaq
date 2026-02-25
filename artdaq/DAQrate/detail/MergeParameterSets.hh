#ifndef artdaq_DAQrate_detail_MergeParameterSets_hh
#define artdaq_DAQrate_detail_MergeParameterSets_hh

#include <fhiclcpp/ParameterSet.h>

namespace artdaq {
/**
 * \brief Merge two FHiCL ParameterSets into one
 * \param first The first fhicl::ParameterSet (keys in \a second override those in \a first)
 * \param second The second fhicl::ParameterSet
 * \return A new fhicl::ParameterSet containing all keys from both inputs
 */
inline fhicl::ParameterSet merge(fhicl::ParameterSet const& first, fhicl::ParameterSet const& second)
{
	auto first_str = first.to_string();
	auto second_str = second.to_string();

	auto combined = first_str + " " + second_str;

	return fhicl::ParameterSet::make(combined);
}
}  // namespace artdaq

#endif  // artdaq_DAQrate_detail_MergeParameterSets_hh
