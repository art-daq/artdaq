#ifndef artdaq_ArtModules_ArtdaqEvent_hh
#define artdaq_ArtModules_ArtdaqEvent_hh

#include "artdaq-core/Data/RawEvent.hh"

#include <unordered_map>

/**
 * \brief An Artdaq Event, consisting of a header and a map of contained Fragments
 */
struct ArtdaqEvent
{
	std::shared_ptr<artdaq::detail::RawEventHeader> header;
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> fragments;

	/**
	 * \brief Default constructor
	 */
	ArtdaqEvent()
	    : header(nullptr), fragments() {}

	/**
	 * \brief Get the type of the first Fragment in the event
	 * \return The Fragment type of the first entry, or artdaq::Fragment::InvalidFragmentType if empty
	 */
	artdaq::Fragment::type_t FirstFragmentType() const
	{
		if (fragments.empty()) return artdaq::Fragment::InvalidFragmentType;
		return fragments.begin()->first;
	}

	/**
	 * \brief Get the total number of Fragments in the event across all types
	 * \return Total Fragment count
	 */
	size_t size() const
	{
		size_t output = 0;

		for (auto& type_pair : fragments)
		{
			output += type_pair.second->size();
		}

		return output;
	}
};

/**
 * \brief Less-than comparison operator for ArtdaqEvent objects
 *
 * Orders events such that Init Fragments come first, EndOfData Fragments come last,
 * and regular events are ordered by run ID, subrun ID, and sequence ID.
 * \param l Left-hand ArtdaqEvent
 * \param r Right-hand ArtdaqEvent
 * \return true if \a l should be ordered before \a r
 */
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
		// EndRun are last in run
		if (left == artdaq::Fragment::EndOfRunFragmentType || right == artdaq::Fragment::EndOfRunFragmentType || left == artdaq::Fragment::RunDataFragmentType || right == artdaq::Fragment::RunDataFragmentType)
		{
			if (left == right)
			{
				// Stable ordering
				return l.header->sequence_id < r.header->sequence_id;
			}
			else
			{
				return right == artdaq::Fragment::EndOfRunFragmentType || right == artdaq::Fragment::RunDataFragmentType;
			}
		}

		if (l.header->subrun_id == r.header->subrun_id)
		{
			// EndSubrun are last within a subrun
			if (left == artdaq::Fragment::EndOfSubrunFragmentType || right == artdaq::Fragment::EndOfSubrunFragmentType || left == artdaq::Fragment::SubrunDataFragmentType || right == artdaq::Fragment::SubrunDataFragmentType)
			{
				if (left == right)
				{
					// Stable ordering
					return l.header->sequence_id < r.header->sequence_id;
				}
				else
				{
					return right == artdaq::Fragment::EndOfSubrunFragmentType || right == artdaq::Fragment::SubrunDataFragmentType;
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

/**
 * \brief Less-than comparison operator for shared pointers to ArtdaqEvent objects
 * \param l Left-hand shared pointer to ArtdaqEvent
 * \param r Right-hand shared pointer to ArtdaqEvent
 * \return true if the event pointed to by \a l should be ordered before the one pointed to by \a r
 */
inline bool operator<(std::shared_ptr<ArtdaqEvent> const& l, std::shared_ptr<ArtdaqEvent> const& r)
{
	return *l < *r;
}

#endif /* artdaq_ArtModules_ArtdaqEvent_hh */

// Local Variables:
// mode: c++
// End:
