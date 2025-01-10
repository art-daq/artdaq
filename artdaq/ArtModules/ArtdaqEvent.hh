#ifndef artdaq_ArtModules_ArtdaqEvent_hh
#define artdaq_ArtModules_ArtdaqEvent_hh

#include "artdaq-core/Data/RawEvent.hh"

/**
 * \brief An Artdaq Event, consisting of a header and a map of contained Fragments
 */
struct ArtdaqEvent
{
	std::shared_ptr<artdaq::detail::RawEventHeader> header;
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> fragments;

	ArtdaqEvent()
	    : header(nullptr), fragments() {}

	artdaq::Fragment::type_t FirstFragmentType() const
	{
		if (fragments.empty()) return artdaq::Fragment::InvalidFragmentType;
		return fragments.begin()->first;
	}
};

inline bool operator<(ArtdaqEvent const& l, ArtdaqEvent const& r)
{
	auto left = l.FirstFragmentType();
	auto right = r.FirstFragmentType();
	// Init Fragments are always first, EndOfData Fragments are always last
	if (left == artdaq::Fragment::InitFragmentType || right == artdaq::Fragment::InitFragmentType || left == artdaq::Fragment::EndOfDataFragmentType || right == artdaq::Fragment::EndOfDataFragmentType)
	{
		if (left == right)
		{
			// Stable ordering
			return l.header->sequence_id < r.header->sequence_id;
		}
		else
		{
			return left == artdaq::Fragment::InitFragmentType || right == artdaq::Fragment::EndOfDataFragmentType;
		}
	}
	else if (l.header->run_id == r.header->run_id)
	{
		// StartRun are first within a run, EndRun are last
		if (left == artdaq::Fragment::StartOfRunFragmentType || right == artdaq::Fragment::StartOfRunFragmentType || left == artdaq::Fragment::EndOfRunFragmentType || right == artdaq::Fragment::EndOfRunFragmentType || left == artdaq::Fragment::RunDataFragmentType || right == artdaq::Fragment::RunDataFragmentType)
		{
			if (left == right)
			{
				// Stable ordering
				return l.header->sequence_id < r.header->sequence_id;
			}
			else
			{
				return left == artdaq::Fragment::StartOfRunFragmentType || right == artdaq::Fragment::EndOfRunFragmentType || right == artdaq::Fragment::RunDataFragmentType;
			}
		}

		if (l.header->subrun_id == r.header->subrun_id)
		{
			// StartSubrun are first within a subrun, EndSubrun are last
			if (left == artdaq::Fragment::StartOfSubrunFragmentType || right == artdaq::Fragment::StartOfSubrunFragmentType || left == artdaq::Fragment::EndOfSubrunFragmentType || right == artdaq::Fragment::EndOfSubrunFragmentType || left == artdaq::Fragment::SubrunDataFragmentType || right == artdaq::Fragment::SubrunDataFragmentType)
			{
				if (left == right)
				{
					// Stable ordering
					return l.header->sequence_id < r.header->sequence_id;
				}
				else
				{
					return left == artdaq::Fragment::StartOfSubrunFragmentType || right == artdaq::Fragment::EndOfSubrunFragmentType || right == artdaq::Fragment::SubrunDataFragmentType;
				}
			}

			// Order by sequence ID if not a recognized system broadcast
			return l.header->sequence_id < r.header->sequence_id;
		}
		else
		{
			return l.header->subrun_id < r.header->subrun_id;
		}
	}
	else
	{
		return l.header->run_id < r.header->run_id;
	}
}

inline bool operator<(std::shared_ptr<ArtdaqEvent> const& l, std::shared_ptr<ArtdaqEvent> const& r)
{
	return *l < *r;
}

#endif /* artdaq_ArtModules_ArtdaqEvent_hh */

// Local Variables:
// mode: c++
// End:
