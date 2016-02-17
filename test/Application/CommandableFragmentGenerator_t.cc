#define BOOST_TEST_MODULE ( CommandableFragmentGenerator_t )
#include "boost/test/auto_unit_test.hpp"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/Application/CommandableFragmentGenerator.hh"

namespace artdaqtest {
  class CommandableFragmentGeneratorTest;
}

class artdaqtest::CommandableFragmentGeneratorTest :
  public artdaq::CommandableFragmentGenerator {
public:
  CommandableFragmentGeneratorTest();
  bool getNext_(artdaq::FragmentPtrs &) override;
  std::vector<artdaq::Fragment::fragment_id_t> fragmentIDs() override;
  void start() override;
  void stopNoMutex() override;
  void stop() override;
  void pause() override;
  void resume() override;
};

artdaqtest::CommandableFragmentGeneratorTest::CommandableFragmentGeneratorTest()

  :
  CommandableFragmentGenerator()
{
}

bool
artdaqtest::CommandableFragmentGeneratorTest::getNext_(artdaq::FragmentPtrs & frags)
{
  frags.emplace_back(new artdaq::Fragment);
  return true;
}

std::vector<artdaq::Fragment::fragment_id_t>
artdaqtest::CommandableFragmentGeneratorTest::
fragmentIDs()
{
  return { 1 };
}

void
artdaqtest::CommandableFragmentGeneratorTest::start()
{ }

void
artdaqtest::CommandableFragmentGeneratorTest::stopNoMutex()
{ }

void
artdaqtest::CommandableFragmentGeneratorTest::stop()
{ }

void
artdaqtest::CommandableFragmentGeneratorTest::pause()
{ }

void
artdaqtest::CommandableFragmentGeneratorTest::resume()
{ }

BOOST_AUTO_TEST_SUITE(CommandableFragmentGenerator_t)

BOOST_AUTO_TEST_CASE(Simple)
{
  artdaqtest::CommandableFragmentGeneratorTest testGen;
  artdaq::CommandableFragmentGenerator & baseGen(testGen);
  artdaq::FragmentPtrs fps;
  baseGen.getNext(fps);
  BOOST_REQUIRE_EQUAL(fps.size(), 1u);
}

BOOST_AUTO_TEST_SUITE_END()
