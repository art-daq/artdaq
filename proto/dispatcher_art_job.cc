#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/ArtModules/detail/TransferWrapper.hh"

int main(int argc, char* argv[])
try
{
	std::ostringstream descstr;
	descstr << argv[0]  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	        << " <-c <config>> <other-options> [<source-file>]+";
	bpo::options_description desc(descstr.str());
	desc.add_options()("config,c", bpo::value<std::string>(), "Configuration")("stop,s", "Stop an existing dispatcher art job")("dispatcherHost,d", bpo::value<std::string>()->default_value("localhost"), "Dispatcher host")("dispatcherPort,p", bpo::value<int>()->default_value(5266), "Dispatcher port")("unique_label,l", bpo::value<std::string>()->default_value("default_art_job"), "Unique label for this art job")("help,h", "produce help message");
	bpo::variables_map vm;
	try
	{
		bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
		bpo::notify(vm);
	}
	catch (bpo::error const& e)
	{
		TLOG_ERROR() << "Exception from command line processing in " << argv[0]  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
		             << ": " << e.what() << "\n";
		exit(-1);
	}
	if (vm.count("help"))
	{
		std::cout << desc << std::endl;
		exit(1);
	}

	fhicl::ParameterSet art_pset;

	if (vm.count("config"))
	{
		std::string config = vm["config"].as<std::string>();

		if (config == "-" || config == "--")
		{
			TLOG_ERROR() << "Reading configuration from standard input. Press Ctrl-D to end" << std::endl;
			std::stringstream ss;
			std::string line;
			while (std::getline(std::cin, line))
			{
				ss << line << std::endl;
			}
			std::cin.clear();

			art_pset = fhicl::ParameterSet::make(ss.str());
		}
		else
		{
			TLOG(TLVL_DEBUG + 32) << config << std::endl;
			auto pset_tmp = LoadParameterSet(config);
			if (pset_tmp.has_key("art")) { art_pset = pset_tmp.get<fhicl::ParameterSet>("art"); }
			else
			{
				art_pset = pset_tmp;
			}
		}
	}
	else if (!vm.count("stop"))
	{
		TLOG_ERROR() << "Exception from command line processing in " << argv[0]  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
		             << ": no configuration given.\n"
		             << "For usage and an options list, please do '"
		             << argv[0] << " --help"  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
		             << "'.\n";
		exit(2);
	}

	if (!art_pset.has_key("path") || !art_pset.has_key("filter_paths"))
	{
		TLOG_ERROR() << "The art configuration must include 'path' and 'filter_paths' parameters to run correctly on a Dispatcher.";
		TLOG_INFO() << "Path is a list of producer and analyzer modules to run, the Dispatcher only supports a single processing path.";
		TLOG_INFO() << "filter_paths is a list of Fhicl tables, e.g. filter_paths: [ {name: pmod path: [prescale]} ], and can be empty. Any SelectEvents calls must use one of the named filter_paths";
		exit(-3);
	}

	fhicl::ParameterSet transfer_wrapper_pset;
	transfer_wrapper_pset.put<std::string>("dispatcherHost", vm["dispatcherHost"].as<std::string>());
	transfer_wrapper_pset.put<int>("dispatcherPort", vm["dispatcherPort"].as<int>());
	transfer_wrapper_pset.put<fhicl::ParameterSet>("dispatcher_config", art_pset);
	transfer_wrapper_pset.put<std::string>("unique_label", vm["unique_label"].as<std::string>());

	auto transfer_wrapper = std::make_unique<artdaq::TransferWrapper>(transfer_wrapper_pset);
	if (vm.count("stop"))
	{
		transfer_wrapper->unregisterMonitor(true, vm["unique_label"].as<std::string>());
	}
	else
	{
		transfer_wrapper->registerMonitor(false);
		transfer_wrapper->detachMonitor();
	}

	return 0;
}
catch (std::exception& ex)
{
	std::cerr << "Exception caught: " << ex.what() << std::endl;
	return -1;
}
catch (...)
{
	return -1;
}
