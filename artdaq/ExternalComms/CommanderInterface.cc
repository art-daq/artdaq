#include "CommanderInterface.hh"

namespace artdaq {
	inline std::string CommanderInterface::send_init(fhicl::ParameterSet, uint64_t, uint64_t)
	{
#pragma message "Using default implementation of send_init!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_soft_init(fhicl::ParameterSet, uint64_t, uint64_t)
	{
#pragma message "Using default implementation of send_soft_init!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_reinit(fhicl::ParameterSet, uint64_t, uint64_t)
	{
#pragma message "Using default implementation of send_reinit!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_start(art::RunID, uint64_t, uint64_t)
	{
#pragma message "Using default implementation of send_start!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_pause(uint64_t, uint64_t)
	{
#pragma message "Using default implementation of send_pause!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_resume(uint64_t, uint64_t)
	{
#pragma message "Using default implementation of send_resume!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_stop(uint64_t, uint64_t)
	{
#pragma message "Using default implementation of send_stop!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_shutdown(uint64_t)
	{
#pragma message "Using default implementation of send_shutdown!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_status()
	{
#pragma message "Using default implementation of send_status!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_report(std::string)
	{
#pragma message "Using default implementation of send_report!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_legal_commands()
	{
#pragma message "Using default implementation of send_legal_commands!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_register_monitor(std::string)
	{
#pragma message "Using default implementation of send_register_monitor!"
		return "NOT IMPLEMENTED";
	}

	inline std::string CommanderInterface::send_unregister_monitor(std::string)
	{
#pragma message "Using default implementation of send_unregister_monitor!"
		return "NOT IMPLEMENTED";
	}

} //namespace artdaq
