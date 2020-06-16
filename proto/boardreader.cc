#include "artdaq/Application/LoadParameterSet.hh"
#include "proto/artdaqapp.hh"

int main(int argc, char* argv[])
try
{
	fhicl::ParameterSet config_ps = LoadParameterSet<artdaq::artdaqapp::Config>(argc, argv, "boardreader", "The BoardReader is responsible for reading out one or more pieces of connected hardware, creating Fragments and sending them to the EventBuilders.");
	artdaq::detail::TaskType task = artdaq::detail::TaskType::BoardReaderTask;

	artdaq::artdaqapp::runArtdaqApp(task, config_ps);

	return 0;
}
catch (...)
{
	return -1;
}