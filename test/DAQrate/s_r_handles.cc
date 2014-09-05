#include "artdaq-core/Data/Fragments.hh"
#include "artdaq/DAQrate/RHandles.hh"
#include "artdaq/DAQrate/SHandles.hh"

#include "artdaq/DAQdata/Debug.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "trace.h"

#include <iostream>
#include <mpi.h>
#include <stdlib.h> // for putenv

#define SND_BUFFER_COUNT 10
#define RCV_BUFFER_COUNT SND_BUFFER_COUNT /* snd/rcv may be different */
#define MAX_PAYLOAD_SIZE 0x100000-artdaq::detail::RawFragmentHeader::num_words()

void do_sending(  int my_rank, int num_senders, int num_receivers
		, int sends_each_sender )
{
    TRACE( 7, "do_sending entered RawFragmentHeader::num_words()=%lu"
	  , artdaq::detail::RawFragmentHeader::num_words() );
    artdaq::SHandles sender(  SND_BUFFER_COUNT, MAX_PAYLOAD_SIZE
			    , num_receivers // dest_count
			    , num_senders // dest_start
			    , false ); // broadcast_sends
  
    std::vector<artdaq::Fragment> frags(SND_BUFFER_COUNT,artdaq::Fragment());

    for (int ii=0; ii<sends_each_sender; ++ii)
    {
	unsigned data_size = MAX_PAYLOAD_SIZE;
	if (data_size < 8) data_size=8;  // min size
	TRACE( 6, "sender rank %d #%u resize datsz=%u",my_rank,ii,data_size );
	frags[ii%SND_BUFFER_COUNT].resize(data_size);
	TRACE( 7, "sender rank %d #%u resized",my_rank,ii );

	unsigned sndDatSz=data_size;
	frags[ii%SND_BUFFER_COUNT].setSequenceID(ii);
	frags[ii%SND_BUFFER_COUNT].setFragmentID(my_rank);

	artdaq::Fragment::iterator it=frags[ii%SND_BUFFER_COUNT].dataBegin();
	*it   = my_rank;
	*++it = ii;
	*++it = sndDatSz;

	sender.sendFragment( std::move(frags[ii%SND_BUFFER_COUNT]) );
	//usleep( (data_size*sizeof(artdaq::RawDataType))/233 );

	TRACE( 8, "sender rank %d #%u sent datSz=%u",my_rank,ii,sndDatSz );
	frags[ii%SND_BUFFER_COUNT] = artdaq::Fragment(); // replace/renew
	TRACE( 9, "sender rank %d frag replaced",my_rank );
    }

} // do_sending

void do_receiving(int /*my_rank*/, int num_senders)
{
  TRACE( 7, "do_receiving entered" );
  artdaq::RHandles receiver(RCV_BUFFER_COUNT,
                            MAX_PAYLOAD_SIZE,
                            num_senders, // src_count
                            0);          // src_start
  while (receiver.sourcesActive() > 0) {
    artdaq::Fragment junkFrag;
    receiver.recvFragment(junkFrag);
  }
}

int main(int argc, char * argv[])
{
  TRACE_CNTL("reset");
  TRACE( 10, "s_r_handles main enter" );
  char envvar[] = "MV2_ENABLE_AFFINITY=0";
  assert(putenv(envvar) == 0);
  auto const requested_threading = MPI_THREAD_SERIALIZED;
  int  provided_threading = -1;
  auto rc = MPI_Init_thread(&argc, &argv, requested_threading, &provided_threading);
  assert(rc == 0);
  assert(requested_threading == provided_threading);
  int my_rank = -1;
  rc = MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  assert(rc == 0);
  if (my_rank == 0) {
    std::cout << "argc:" << argc << std::endl;
    for (int i = 0; i < argc; ++i) {
      std::cout << "argv[" << i << "]: " << argv[i] << std::endl;
    }
  }
  if (argc < 2 || 3 < argc) {
    std::cerr << argv[0] << " requires 2 or 3 arguments, " << argc << " provided\n";
    return 1;
  }
  auto num_sending_ranks = atoi(argv[1]);
  int sends_each_sender=0; // besides "EOD" sends
  if (argc == 3) sends_each_sender = atoi(argv[2]);
  int total_ranks = -1;
  rc = MPI_Comm_size(MPI_COMM_WORLD, &total_ranks);
  auto num_receiving_ranks = total_ranks - num_sending_ranks;
  if (my_rank == 0) {
    std::cout << "Total number of ranks:       " << total_ranks <<"\n";
    std::cout << "Number of sending ranks:     " << num_sending_ranks <<"\n";
    std::cout << "Number of receiving ranks:   " << num_receiving_ranks <<"\n";
    std::cout << "Number of sends_each_sender: " << sends_each_sender <<"\n";
  }
  configureDebugStream(my_rank, 0);
  if (my_rank < num_sending_ranks) {
    do_sending(my_rank,num_sending_ranks,num_receiving_ranks,sends_each_sender);
  }
  else {
    do_receiving(my_rank, num_sending_ranks);
  }
  rc = MPI_Finalize();
  assert(rc == 0);
  TRACE( 11, "s_r_handles main return" );
  return 0;
}
