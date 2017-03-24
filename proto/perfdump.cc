#include <fstream>
#include <iostream>
#include <vector>

#include "artdaq/DAQrate/Perf.hh"

using namespace std;

enum State
{
	NoJob = 0,
	InJob = 1
};

State state = NoJob;
JobStartMeas saved_start;

template <class T>
void handle_sub(void* d, ostream& ost)
{
	T* a = (T *)d;
	if (state == InJob)
	{
		ost << saved_start.run_ << " " << saved_start.rank_ << " ";
	}
	ost << *a << "\n";
}

void handle_start(void* d, ostream& ost)
{
	JobStartMeas* j = (JobStartMeas *)d;
	state = InJob;
	saved_start = *j;
	ost << *j << "\n";
}

void handle_end(void* d, ostream& ost)
{
	JobEndMeas* j = (JobEndMeas *)d;
	if (state == InJob)
	{
		ost << saved_start.run_ << " " << saved_start.rank_ << " ";
	}
	ost << *j << "\n";
	state = NoJob;
}

int main(int argc, char* argv[])
{
	if (argc < 2)
	{
		cerr << "Argument perf_file missing\n";
		return -1;
	}
	std::vector<char> data(500);
	ifstream infile(argv[1], ifstream::binary);
	if (!infile)
	{
		std::cerr << "Unable to open file " << argv[1] << ".\n";
		exit(1);
	}
	while (1)
	{
		infile.read(&data[0], sizeof(Header));
		if (infile.eof()) { break; }
		Header* head = (Header *)&data[0];
		infile.read(&data[sizeof(Header)], head->len_);
		switch (head->id_)
		{
		case PERF_SEND: handle_sub<SendMeas>(&data[0], cout);
			break;
		case PERF_RECV: handle_sub<RecvMeas>(&data[0], cout);
			break;
		case PERF_JOB_START: handle_start(&data[0], cout);
			break;
		case PERF_JOB_END: handle_end(&data[0], cout);
			break;
		case PERF_EVENT: handle_sub<EventMeas>(&data[0], cout);
			break;
		default: break;
		}
	}
	return 0;
}
