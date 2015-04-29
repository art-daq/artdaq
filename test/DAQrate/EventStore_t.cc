#include "artdaq/DAQrate/EventStore.hh"
#include <fhiclcpp/ParameterSet.h>

#include <thread>

#define BOOST_TEST_MODULE(EventStore_t)
#include "boost/test/auto_unit_test.hpp"

BOOST_AUTO_TEST_SUITE(EventStore_test)

/* This is responsible for taking the completed RawEvents passed via the
   GlobalQueue from the EventStore and holding onto them until the EndOfData
   signal is received.  At that point it will push all of the events back onto
   the GlobalQueue so that the unit test can pull them off and examine them.
*/
int bogusApp(int, char **)
{
  artdaq::RawEventQueue &queue(artdaq::getGlobalQueue());
  artdaq::RawEvent_ptr incomingEvent;
  std::vector<artdaq::RawEvent_ptr> receivedEvents;

  while(1) {
    queue.deqWait(incomingEvent);
    if (incomingEvent == nullptr) {
      for (std::vector<artdaq::RawEvent_ptr>::iterator it = receivedEvents.begin();
	   it != receivedEvents.end(); ++it) {
	queue.enqNowait(*it);
      }

      queue.enqNowait(artdaq::RawEvent_ptr(0));
      return 0;
    }

    receivedEvents.emplace_back(incomingEvent);
  }
}


BOOST_AUTO_TEST_CASE(Trivial)
{
  /* This will create an event store configured to build RawEvents that consist
     of four Fragments.  We'll insert the Fragments out of order to spice things
     up. 
  */
  std::unique_ptr<artdaq::EventStore> eventStore;
  artdaq::EventStore::ART_CMDLINE_FCN *bogusReader = &bogusApp;
  fhicl::ParameterSet pset;
  eventStore.reset(new artdaq::EventStore(pset, 4, 1, 0, 0, nullptr, bogusReader));

  int sequenceID[8] = {1, 2, 1, 2, 1, 2, 2, 1};
  int fragmentID[8] = {1, 2, 3, 4, 1, 2, 3, 4};
  std::unique_ptr<artdaq::Fragment> testFragment;
  for (int i = 0; i < 8; i++) {
    testFragment.reset(new artdaq::Fragment(sequenceID[i], fragmentID[i]));
    eventStore->insert(std::move(testFragment));
  }
  int readerReturnValue;
  eventStore->endOfData(readerReturnValue);

  artdaq::RawEventQueue &queue(artdaq::getGlobalQueue());
  
  artdaq::RawEvent_ptr r1;
  artdaq::RawEvent_ptr r2;
  artdaq::RawEvent_ptr r3;
  artdaq::RawEvent_ptr r4;

  BOOST_REQUIRE_EQUAL(queue.deqNowait(r1), true);
  BOOST_REQUIRE_EQUAL(queue.deqNowait(r2), true);
  BOOST_REQUIRE_EQUAL(queue.deqNowait(r3), true);
  BOOST_REQUIRE_EQUAL(queue.deqNowait(r4), false);

  BOOST_REQUIRE_EQUAL(r1->numFragments(), (size_t) 4);
  BOOST_REQUIRE_EQUAL(r2->numFragments(), (size_t) 4);
}

BOOST_AUTO_TEST_CASE(SequenceMod)
{
  /* Verify that the EventStore will correctly group Fragments with different
     sequence numbers into groups when configured to do so.
  */
  std::unique_ptr<artdaq::EventStore> eventStore;
  artdaq::EventStore::ART_CMDLINE_FCN *bogusReader = &bogusApp;
  fhicl::ParameterSet pset;
  eventStore.reset(new artdaq::EventStore(pset, 4, 1, 0, 0, nullptr, bogusReader));
  eventStore->setSeqIDModulus(4);

  int sequenceID[8] = {1, 5, 4, 6, 7, 2, 8, 3};
  int fragmentID[8] = {1, 2, 3, 4, 1, 2, 3, 4};
  std::unique_ptr<artdaq::Fragment> testFragment;
  for (int i = 0; i < 8; i++) {
    testFragment.reset(new artdaq::Fragment(sequenceID[i], fragmentID[i]));
    eventStore->insert(std::move(testFragment));
  }
  int readerReturnValue;
  eventStore->endOfData(readerReturnValue);

  artdaq::RawEventQueue &queue(artdaq::getGlobalQueue());
  
  artdaq::RawEvent_ptr r1;
  artdaq::RawEvent_ptr r2;
  artdaq::RawEvent_ptr r3;
  artdaq::RawEvent_ptr r4;

  BOOST_REQUIRE_EQUAL(queue.deqNowait(r1), true);
  BOOST_REQUIRE_EQUAL(queue.deqNowait(r2), true);
  BOOST_REQUIRE_EQUAL(queue.deqNowait(r3), true);
  BOOST_REQUIRE_EQUAL(queue.deqNowait(r4), false);

  BOOST_REQUIRE_EQUAL(r1->numFragments(), (size_t) 4);
  BOOST_REQUIRE_EQUAL(r2->numFragments(), (size_t) 4);

  /* The EventStore doesn't order anything so things should come out the same 
     order they went in.
  */
  std::unique_ptr<std::vector<artdaq::Fragment>> fragments1 = r1->releaseProduct();
  int sequenceIDb[8] = {5, 6, 7, 8};
  for (int i = 0; i < 4; i++) {
    BOOST_REQUIRE_EQUAL(sequenceIDb[i], (int)fragments1->at(i).sequenceID());
  }

  std::unique_ptr<std::vector<artdaq::Fragment>> fragments2 = r2->releaseProduct();
  int sequenceIDc[8] = {1, 4, 2, 3};
  for (int i = 0; i < 4; i++) {
    BOOST_REQUIRE_EQUAL(sequenceIDc[i], (int)fragments2->at(i).sequenceID());
  }
}

BOOST_AUTO_TEST_SUITE_END()
