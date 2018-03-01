#include "proto/artdaqapp.hh"
#include "artdaq/Application/LoadParameterSet.hh"

int main(int argc, char* argv[])
{
	fhicl::ParameterSet config_ps = LoadParameterSet(argc, argv);
	artdaq::detail::TaskType task = artdaq::detail::TaskType::DataLoggerTask;

	artdaq::artdaqapp::runArtdaqApp(task, config_ps);
}