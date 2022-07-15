#include "CommanderInterface.hh"

namespace artdaq {
CommanderInterface::~CommanderInterface() = default;

inline std::string CommanderInterface::send_init(fhicl::ParameterSet const& /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/)
{
#pragma message "Using default implementation of send_init!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_soft_init(fhicl::ParameterSet const& /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/)
{
#pragma message "Using default implementation of send_soft_init!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_reinit(fhicl::ParameterSet const& /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/)
{
#pragma message "Using default implementation of send_reinit!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_start(art::RunID /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/)
{
#pragma message "Using default implementation of send_start!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_pause(uint64_t /*unused*/, uint64_t /*unused*/)
{
#pragma message "Using default implementation of send_pause!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_resume(uint64_t /*unused*/, uint64_t /*unused*/)
{
#pragma message "Using default implementation of send_resume!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_stop(uint64_t /*unused*/, uint64_t /*unused*/)
{
#pragma message "Using default implementation of send_stop!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_shutdown(uint64_t /*unused*/)
{
#pragma message "Using default implementation of send_shutdown!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_status()
{
#pragma message "Using default implementation of send_status!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_report(std::string const& /*unused*/)
{
#pragma message "Using default implementation of send_report!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_legal_commands()
{
#pragma message "Using default implementation of send_legal_commands!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_register_monitor(std::string const& /*unused*/)
{
#pragma message "Using default implementation of send_register_monitor!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_unregister_monitor(std::string const& /*unused*/)
{
#pragma message "Using default implementation of send_unregister_monitor!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_trace_get(std::string const& /*unused*/)
{
#pragma message "Using default implementation of send_trace_get!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_trace_set(std::string const& /*unused*/, std::string const& /*unused*/, std::string const& /*unused*/)
{
#pragma message "Using default implementation of send_trace_set!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_meta_command(std::string const& /*unused*/, std::string const& /*unused*/)
{
#pragma message "Using default implementation of send_meta_command!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_rollover_subrun(uint64_t /*unused*/, uint32_t /*unused*/)
{
#pragma message "Using default implementation of send_rollover_subrun!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::add_config_archive_entry(std::string const& /*unused*/, std::string const& /*unused*/)
{
#pragma message "Using default implementation of add_config_archive_entry!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::clear_config_archive()
{
#pragma message "Using default implementation of clear_config_archive!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_override_fragment_ids(uint64_t, std::vector<uint32_t>)
{
#pragma message "Using default implementation of override_fragment_ids!"
	return "NOT IMPLEMENTED";
}

inline std::string CommanderInterface::send_update_default_fragment_ids(uint64_t, std::vector<uint32_t>)
{
#pragma message "Using default implementation of update_default_fragment_ids!"
	return "NOT IMPLEMENTED";
}
}  //namespace artdaq
