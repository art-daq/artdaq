#ifndef artdaq_Application_TaskType_hh
#define artdaq_Application_TaskType_hh

/**
 * \brief The artdaq namespace
 */
namespace artdaq
{
	/**
	 * \brief The artdaq::detail namespace contains internal implementation details for some classes
	 */
	namespace detail
	{
		/**
		 * \brief The types of applications in artdaq
		 */
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
