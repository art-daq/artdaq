#include "artdaq/Application/LoadParameterSet.hh"
#include "proto/artdaqapp.hh"

int main(int argc, char* argv[]) try
{
	fhicl::ParameterSet config_ps = LoadParameterSet<artdaq::artdaqapp::Config>(argc, argv, "routing_manager", "This is the artdaq Routing Manager application\nThe Routing Manager receives tokens from the receivers, builds Routing Tables using those tokens and a Routing Policy plugin, then sends the routing tables to the senders.");

	artdaq::detail::TaskType task = artdaq::detail::TaskType::RoutingManagerTask;

	artdaq::artdaqapp::runArtdaqApp(task, config_ps);

	return 0;
}
catch (...)
{
	return -1;
}