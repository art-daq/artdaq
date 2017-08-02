#include "proto/MPIProg.hh"
#include <iostream>
#include <boost/program_options.hpp>
namespace bpo = boost::program_options;


int main(int argc, char* argv[])
{
	MPI_Init(&argc, &argv);
	int yes;
	MPI_Initialized(&yes);
	if (!yes) MPI_Init(&argc, &argv);

	std::ostringstream descstr;
	descstr << argv[0]
		<< "<--send|--receive>";
	bpo::options_description desc(descstr.str());
	desc.add_options()
		("send,s", "Process is sender")
		("receive,r", "Process is receiver")
		("count,c", bpo::value<int>(), "Sender count (default: 1)")
		("help,h", "produce help message");
	bpo::variables_map vm;
	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
		bpo::notify(vm);
	}
	catch (bpo::error const & e) {
		std::cerr << "Exception from command line processing in " << argv[0]
			<< ": " << e.what() << "\n";
		exit(-1);
	}
	if (vm.count("help")) {
		std::cout << desc << std::endl;
		exit(1);
	}
	auto sender = vm.count("send") > 0;
	auto receiver = vm.count("receive") > 0;
	if ((sender && receiver) || !(sender || receiver)) {
		std::cerr << "One (and only one) of --sender and --receiver MUST be specified!" << std::endl;
		exit(-2);
	}
	int count = 1;
	if (vm.count("count")) {
		count = vm["count"].as<int>();
	}


	MPI_Comm intercomm, intercomm2;
	char port_name[MPI_MAX_PORT_NAME], port_name2[MPI_MAX_PORT_NAME];
		if (sender)
		{
			std::cout << "Starting sender process" << std::endl;;
			std::cout << "Looking up name" << std::endl;
			MPI_Lookup_name("transfer_between_0_and_1", MPI_INFO_NULL, port_name);
			MPI_Lookup_name("test2", MPI_INFO_NULL, port_name2);
			std::cout << "Connecting comm" << std::endl;
			MPI_Comm_connect(port_name, MPI_INFO_NULL, 0, MPI_COMM_WORLD, &intercomm);
			std::cout << "Connecting comm2" << std::endl;
			MPI_Comm_connect(port_name2, MPI_INFO_NULL, 0, MPI_COMM_WORLD, &intercomm2);

			std::string test_string = "mpi_reconnection_test: ";
			for (int ii = 23; ii < 99; ++ii) {
				test_string += '*';
			}
			test_string += '\0';

			std::cout << "Sending: " << test_string << std::endl;
			MPI_Send(test_string.c_str(), 100, MPI_CHAR, 0, 0, intercomm);
			MPI_Send(test_string.c_str(), 100, MPI_CHAR, 0, 0, intercomm2);

			std::cout << "Disconnecting comm" << std::endl;
			MPI_Comm_disconnect(&intercomm);
			MPI_Comm_disconnect(&intercomm2);
			std::cout << "SENDER DONE" << std::endl;
		}
		else if (receiver)
		{
			std::cout << "Starting receiver process" << std::endl;
			std::cout << "Opening port" << std::endl;
			MPI_Open_port(MPI_INFO_NULL, port_name);
			MPI_Open_port(MPI_INFO_NULL, port_name2);
			std::cout << "Publishing name" << std::endl;
			MPI_Publish_name("transfer_between_0_and_1", MPI_INFO_NULL, port_name);
			MPI_Publish_name("test2", MPI_INFO_NULL, port_name2);
			std::cout << "Accepting comm" << std::endl;
			MPI_Comm_accept(port_name, MPI_INFO_NULL, 0, MPI_COMM_WORLD, &intercomm);
			std::cout << "Accepting comm2" << std::endl;
			MPI_Comm_accept(port_name2, MPI_INFO_NULL, 0, MPI_COMM_WORLD, &intercomm2);

			for (int ii = 0; ii < count; ++ii) {
				char strbuf[100];
				std::cout << "Calling Recv" << std::endl;
				MPI_Status status;
				int recvSts = MPI_Recv(&strbuf, 100, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, intercomm, &status);
				std::cout << "Received: " << strbuf << " ( sts=" << recvSts << ", rank=" << status.MPI_SOURCE << ", tag=" << status.MPI_TAG << " ) on intercomm" << std::endl;
				recvSts = MPI_Recv(&strbuf, 100, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, intercomm2, &status);
				std::cout << "Received: " << strbuf << " ( sts=" << recvSts << ", rank=" << status.MPI_SOURCE << ", tag=" << status.MPI_TAG << " ) on intercomm2" << std::endl;

			}
			std::cout << "Unpublishing name" << std::endl;
			MPI_Unpublish_name("transfer_between_0_and_1", MPI_INFO_NULL, port_name);
			MPI_Unpublish_name("test2", MPI_INFO_NULL, port_name2);
			std::cout << "RECEIVER ALL DONE" << std::endl;
		}

	MPI_Finalize();

	return 0;
}
