#include "proto/artdaqapp.hh"
#include "artdaq/Application/LoadParameterSet.hh"

int main(int argc, char* argv[])
{
	fhicl::ParameterSet config_ps = LoadParameterSet(argc, argv);
	artdaq::detail::TaskType task = artdaq::detail::UnknownTask;

	if (config_ps.has_key("app_type"))
	{
		task = artdaq::detail::StringToTaskType(config_ps.get<std::string>("app_type", ""));
		if (task == artdaq::detail::TaskType::UnknownTask) {
			task = artdaq::detail::IntToTaskType(config_ps.get<int>("app_type"));
		}
	}
	else if (config_ps.has_key("application_type"))
	{
		task = artdaq::detail::StringToTaskType(config_ps.get<std::string>("application_type", ""));
		if (task == artdaq::detail::TaskType::UnknownTask) {
			task = artdaq::detail::IntToTaskType(config_ps.get<int>("application_type"));
		}
	}
	artdaq::artdaqapp::runArtdaqApp(task, config_ps);
}
