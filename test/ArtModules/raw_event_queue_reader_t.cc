#include "artdaq/ArtModules/detail/RawEventQueueReader.hh"

#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/RootDictionaryManager.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Persistency/Provenance/BranchIDListHelper.h"
#include "art/Persistency/Provenance/EventID.h"
#include "art/Persistency/Provenance/FileFormatVersion.h"
#include "art/Persistency/Provenance/MasterProductRegistry.h"
#include "art/Persistency/Provenance/ModuleDescription.h"
#include "art/Persistency/Provenance/Parentage.h"
#include "art/Persistency/Provenance/ProcessConfiguration.h"
#include "art/Persistency/Provenance/ProductMetaData.h"
#include "art/Persistency/Provenance/RunID.h"
#include "art/Persistency/Provenance/SubRunID.h"
#include "art/Persistency/Provenance/Timestamp.h"
#include "art/Utilities/Exception.h"
#include "art/Utilities/GetPassID.h"
#include "art/Version/GetReleaseVersion.h"
#include "artdaq-core/Core/GlobalQueue.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "cetlib/make_unique.h"
#include "fhiclcpp/make_ParameterSet.h"

#define BOOST_TEST_MODULE ( raw_event_queue_reader_t )
#include "boost/test/auto_unit_test.hpp"

#include <iostream>
#include <memory>
#include <string>

using std::string;

using art::EventID;
using art::EventPrincipal;
using art::FileBlock;
using art::FileFormatVersion;
using art::ModuleDescription;
using art::SourceHelper;
using art::ProcessConfiguration;
using art::ProductRegistryHelper;
using art::RunID;
using art::RunPrincipal;
using art::SubRunID;
using art::SubRunPrincipal;
using art::Timestamp;
using artdaq::Fragment;
using artdaq::RawEvent;
using artdaq::RawEvent_ptr;
using artdaq::detail::RawEventQueueReader;
using fhicl::ParameterSet;

class MPRGlobalTestFixture {
public:
  MPRGlobalTestFixture();

  typedef std::map<std::string, art::BranchKey> BKmap_t;

  BKmap_t branchKeys_;
  std::map<std::string, std::unique_ptr<art::ProcessConfiguration> >processConfigurations_;

  art::ProcessConfiguration *
  fake_single_module_process(std::string const & tag,
                             std::string const & processName,
                             fhicl::ParameterSet const & moduleParams,
                             std::string const & release = art::getReleaseVersion(),
                             std::string const & pass = art::getPassID());

  std::unique_ptr<art::BranchDescription>
  fake_single_process_branch(std::string const & tag,
                             std::string const & processName,
                             std::string const & productInstanceName = std::string());

  void finalize();

  art::MasterProductRegistry  productRegistry_;
  art::RootDictionaryManager rdm_;
};

MPRGlobalTestFixture::MPRGlobalTestFixture()
  :
  branchKeys_(),
  processConfigurations_(),
  productRegistry_(),
  rdm_()
{
  // We can only insert products registered in the MasterProductRegistry.
  productRegistry_.addProduct(fake_single_process_branch("hlt",  "HLT"));
  productRegistry_.addProduct(fake_single_process_branch("prod", "PROD"));
  productRegistry_.addProduct(fake_single_process_branch("test", "TEST"));
  productRegistry_.addProduct(fake_single_process_branch("user", "USER"));
  productRegistry_.addProduct(fake_single_process_branch("rick", "USER2", "rick"));
}

void
MPRGlobalTestFixture::finalize()
{
  productRegistry_.setFrozen();
  art::BranchIDListHelper::updateRegistries(productRegistry_);
  art::ProductMetaData::create_instance(productRegistry_);
}

art::ProcessConfiguration *
MPRGlobalTestFixture::
fake_single_module_process(std::string const & tag,
                           std::string const & processName,
                           fhicl::ParameterSet const & moduleParams,
                           std::string const & release,
                           std::string const & pass)
{
  fhicl::ParameterSet processParams;
  processParams.put(processName, moduleParams);
  processParams.put<std::string>("process_name",
                                 processName);
  auto emplace_pair =
  processConfigurations_.emplace(tag,
                                 cet::make_unique<art::ProcessConfiguration>(processName, processParams.id(), release, pass));
  return emplace_pair.first->second.get();
}

std::unique_ptr<art::BranchDescription>
MPRGlobalTestFixture::
fake_single_process_branch(std::string const & tag,
                           std::string const & processName,
                           std::string const & productInstanceName)
{
  std::string moduleLabel = processName + "dummyMod";
  std::string moduleClass("DummyModule");
  fhicl::ParameterSet modParams;
  modParams.put<std::string>("module_type", moduleClass);
  modParams.put<std::string>("module_label", moduleLabel);
  art::ProcessConfiguration * process =
    fake_single_module_process(tag, processName, modParams);
  art::ModuleDescription mod(modParams.id(),
                             moduleClass,
                             moduleLabel,
                             *process);
  art::TypeID dummyType(typeid(int));
  art::BranchDescription * result =
    new art::BranchDescription(art::TypeLabel(art::InEvent,
                                              dummyType,
                                              productInstanceName),
                               mod);
  branchKeys_.insert(std::make_pair(tag, art::BranchKey(*result)));
  return std::unique_ptr<art::BranchDescription>(result);
}

struct REQRTestFixture {
  REQRTestFixture() {
    static bool once(true);
    if (once) {
      (void) reader(); // Force initialization.
      ModuleDescription md(ParameterSet().id(),
                           "_NAMEERROR_",
                           "_LABELERROR_",
                           *gf().processConfigurations_["daq"]);
      // These _xERROR_ strings should never appear in branch names; they
      // are here as tracers to help identify any failures in coding.
      helper().registerProducts(gf().productRegistry_, md);
      gf().finalize();
      once = false;
    }
  }

  MPRGlobalTestFixture & gf() {
    static MPRGlobalTestFixture mpr;
    return mpr;
  }

  ProductRegistryHelper & helper() {
    static ProductRegistryHelper s_helper;
    return s_helper;
  }

  art::SourceHelper & source_helper() {
    static std::unique_ptr<art::SourceHelper>
      s_source_helper;
    if (!s_source_helper) {
      ParameterSet sourceParams;
      std::string moduleType { "DummySource" };
      std::string moduleLabel { "daq" };
      sourceParams.put<std::string>("module_type", moduleType);
      sourceParams.put<std::string>("module_label", moduleLabel);
      auto pc_ptr = gf().fake_single_module_process(moduleLabel,
                                                    "TEST",
                                                    sourceParams);
      art::ModuleDescription md(sourceParams.id(),
                                moduleType,
                                moduleLabel,
                                *pc_ptr);
      s_source_helper = cet::make_unique<art::SourceHelper>(md);
    }
    return *s_source_helper;
  }

  RawEventQueueReader & reader() {
    fhicl::ParameterSet pset;
    fhicl::make_ParameterSet("fragment_type_map: [[1, \"ABCDEF\"]]", pset);
    static RawEventQueueReader
    s_reader(pset,
             helper(),
             source_helper(),
       gf().productRegistry_);
    return s_reader;
  }
};

BOOST_FIXTURE_TEST_SUITE(raw_event_queue_reader_t, REQRTestFixture)

namespace {
  void basic_test(RawEventQueueReader & reader,
                  std::unique_ptr<RunPrincipal> && run,
                  std::unique_ptr<SubRunPrincipal> && subrun,
                  EventID const & eventid)
  {
    BOOST_REQUIRE(run || subrun == nullptr); // Sanity check.
    std::shared_ptr<RawEvent> event(new RawEvent(eventid.run(), eventid.subRun(), eventid.event()));
    std::vector<Fragment::value_type> fakeData { 1, 2, 3, 4 };
    std::unique_ptr<Fragment>
      tmpFrag(new Fragment(std::move(Fragment::dataFrag(eventid.event(),
                                                        0,
                                                        fakeData.begin(),
                                                        fakeData.end()))));
    tmpFrag->setUserType(1);
    event->insertFragment(std::move(tmpFrag));
    event->markComplete();
    artdaq::getGlobalQueue().enqNowait(event);
    EventPrincipal * newevent = nullptr;
    SubRunPrincipal * newsubrun = nullptr;
    RunPrincipal  *  newrun = nullptr;
    bool rc = reader.readNext(run.get(), subrun.get(), newrun, newsubrun, newevent);
    BOOST_REQUIRE(rc);
    if (run.get() && run->run() == eventid.run()) {
      BOOST_CHECK(newrun == nullptr);
    }
    else {
      BOOST_CHECK(newrun);
      BOOST_CHECK(newrun->id() == eventid.runID());
    }
    if (!newrun && subrun.get() && subrun->subRun() == eventid.subRun()) {
      BOOST_CHECK(newsubrun == nullptr);
    }
    else {
      BOOST_CHECK(newsubrun);
      BOOST_CHECK(newsubrun->id() == eventid.subRunID());
    }
    BOOST_CHECK(newevent);
    BOOST_CHECK(newevent->id() == eventid);
    art::Event e(*newevent, ModuleDescription());
    art::Handle<std::vector<Fragment>> h;
    e.getByLabel("daq", "ABCDEF", h);
    BOOST_CHECK(h.isValid());
    BOOST_CHECK(h->size() == 1);
    BOOST_CHECK(std::equal(fakeData.begin(),
                           fakeData.end(),
                           h->front().dataBegin()));
    delete(newrun);
    delete(newsubrun);
    delete(newevent);
  }
}

BOOST_AUTO_TEST_CASE(nonempty_event)
{
  EventID eventid(2112, 1, 3);
  Timestamp now;
  basic_test(reader(),
             std::unique_ptr<RunPrincipal>(source_helper().makeRunPrincipal(eventid.run(), now)),
             std::unique_ptr<SubRunPrincipal>(source_helper().makeSubRunPrincipal(eventid.run(), eventid.subRun(), now)),
             eventid);
}

BOOST_AUTO_TEST_CASE(first_event)
{
  EventID eventid(2112, 1, 3);
  Timestamp now;
  basic_test(reader(),
             nullptr,
             nullptr,
             eventid);
}

BOOST_AUTO_TEST_CASE(new_subrun)
{
  EventID eventid(2112, 1, 3);
  Timestamp now;
  basic_test(reader(),
             std::unique_ptr<RunPrincipal>(source_helper().makeRunPrincipal(eventid.run(), now)),
             std::unique_ptr<SubRunPrincipal>(source_helper().makeSubRunPrincipal(eventid.run(), 0, now)),
             eventid);
}

BOOST_AUTO_TEST_CASE(new_run)
{
  EventID eventid(2112, 1, 3);
  Timestamp now;
  basic_test(reader(),
             std::unique_ptr<RunPrincipal>(source_helper().makeRunPrincipal(eventid.run() - 1, now)),
             std::unique_ptr<SubRunPrincipal>(source_helper().makeSubRunPrincipal(eventid.run() - 1,
                 eventid.subRun(),
                 now)),
             eventid);
}

BOOST_AUTO_TEST_CASE(end_of_data)
{
  // Tell 'reader' the name of the file we are to read. This is pretty
  // much irrelevant for RawEventQueueReader, but we'll stick to the
  // interface demanded by Source<T>...
  string const fakeFileName("no such file exists");
  FileBlock * pFile = nullptr;
  reader().readFile(fakeFileName, pFile);
  BOOST_CHECK(pFile);
  BOOST_CHECK(pFile->fileFormatVersion() == FileFormatVersion(1, "RawEvent2011"));
  BOOST_CHECK(pFile->tree() == nullptr);
  
  // These members have been removed as of art v1_16
  #if ART_MAJOR_VERSION <= 1 && ART_MINOR_VERSION < 16
  BOOST_CHECK(pFile->metaTree() == nullptr);
  BOOST_CHECK(pFile->subRunTree() == nullptr);
  BOOST_CHECK(pFile->subRunMetaTree() == nullptr);
  BOOST_CHECK(pFile->runTree() == nullptr);
  BOOST_CHECK(pFile->runMetaTree() == nullptr);
  #endif

  BOOST_CHECK(!pFile->fastClonable());
  // Test the end-of-data handling. Reading an end-of-data should result in readNext() returning false,
  // and should return null pointers for new-run, -subrun and -event.
  // Prepare our 'previous run/subrun/event'..
  RunID runid(2112);
  SubRunID subrunid(2112, 1);
  EventID eventid(2112, 1, 3);
  Timestamp now;
  std::unique_ptr<RunPrincipal>    run(source_helper().makeRunPrincipal(runid.run(), now));
  std::unique_ptr<SubRunPrincipal> subrun(source_helper().makeSubRunPrincipal(runid.run(), subrunid.subRun(), now));
  std::unique_ptr<EventPrincipal>  event(source_helper().makeEventPrincipal(runid.run(),
                                         subrunid.subRun(),
                                         eventid.event(),
                                         now));
  artdaq::getGlobalQueue().enqNowait(std::shared_ptr<RawEvent>(nullptr)); // insert end-of-data marker
  EventPrincipal * newevent = nullptr;
  SubRunPrincipal * newsubrun = nullptr;
  RunPrincipal  *  newrun = nullptr;
  bool rc = reader().readNext(run.get(), subrun.get(), newrun, newsubrun, newevent);
  BOOST_CHECK(!rc);
  BOOST_CHECK(newrun == nullptr);
  BOOST_CHECK(newsubrun == nullptr);
  BOOST_CHECK(newevent == nullptr);
}

BOOST_AUTO_TEST_SUITE_END()
