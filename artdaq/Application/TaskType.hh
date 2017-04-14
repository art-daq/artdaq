#ifndef artdaq_Application_TaskType_hh
#define artdaq_Application_TaskType_hh

namespace artdaq
{
	namespace detail
	{
		enum TaskType : int
		{
			BoardReaderTask=1,
			EventBuilderTask=2,
			AggregatorTask=3,
			RoutingMasterTask=4
		};
	}

	using detail::TaskType; // Require C++2011 scoping, hide C++03 scoping.
}

#endif /* artdaq_Application_TaskType_hh */
