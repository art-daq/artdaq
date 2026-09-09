#include "TRACE/tracemf.h"

#include "artdaq/ArtModules/ArtdaqRunInfoServiceInterface.h"

#include "art/Framework/Services/Registry/ServiceDefinitionMacros.h"

#include "fhiclcpp/ParameterSet.h"

#include <cstddef>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <string>

#define TRACE_NAME "ArtdaqRunInfoService"

class ArtdaqRunInfoService : public ArtdaqRunInfoServiceInterface
{
public:
	ArtdaqRunInfoService(fhicl::ParameterSet const& pset, art::ActivityRegistry&);
	~ArtdaqRunInfoService() override = default;

	bool addSubrunRecord(
	    art::RunNumber_t run,
	    art::SubRunNumber_t subrun,
	    size_t nEvents,
	    art::EventNumber_t firstEvent,
	    art::EventNumber_t lastEvent,
	    std::string const& datastream) override;

	bool addFileSummary(
	    std::string const& fileName,
	    art::RunNumber_t run,
	    art::SubRunNumber_t firstSubrun,
	    art::SubRunNumber_t lastSubrun,
	    size_t nEvents,
	    size_t fileSize,
	    std::string const& metadata,
	    std::string const& datastream) override;

private:
	std::string summaryDir_;
};

DECLARE_ART_SERVICE_INTERFACE_IMPL(ArtdaqRunInfoService, ArtdaqRunInfoServiceInterface, LEGACY)

ArtdaqRunInfoService::ArtdaqRunInfoService(fhicl::ParameterSet const& pset, art::ActivityRegistry& /*unused*/)
    : summaryDir_(pset.get<std::string>("summaryDir", ""))
{
	TLOG(TLVL_INFO) << "ArtdaqRunInfoService: summaryDir=\"" << summaryDir_ << "\"";
}

bool ArtdaqRunInfoService::addSubrunRecord(
    art::RunNumber_t run,
    art::SubRunNumber_t subrun,
    size_t nEvents,
    art::EventNumber_t firstEvent,
    art::EventNumber_t lastEvent,
    std::string const& datastream)
{
	if (summaryDir_.empty()) { return true; }

	std::ostringstream fname;
	fname << summaryDir_;
	if (summaryDir_.back() != '/') { fname << '/'; }
	fname << "subrun_record_run" << std::setw(6) << std::setfill('0') << run << ".csv";

	std::ostringstream row;
	row << run << "," << subrun << "," << nEvents << "," << firstEvent << "," << lastEvent
	    << "," << datastream << "\n";

	return appendToCsv(fname.str(),
	    "run,subrun,n_events,first_event,last_event,datastream\n", row.str());
}

bool ArtdaqRunInfoService::addFileSummary(
    std::string const& fileName,
    art::RunNumber_t run,
    art::SubRunNumber_t firstSubrun,
    art::SubRunNumber_t lastSubrun,
    size_t nEvents,
    size_t fileSize,
    std::string const& /*metadata*/,
    std::string const& datastream)
{
	if (summaryDir_.empty()) { return true; }

	std::ostringstream fname;
	fname << summaryDir_;
	if (summaryDir_.back() != '/') { fname << '/'; }
	fname << "file_summary_run" << std::setw(6) << std::setfill('0') << run << ".csv";

	std::string const outputFile = std::filesystem::path(fileName).filename().string();

	std::ostringstream row;
	row << outputFile << "," << run << "," << firstSubrun << "," << lastSubrun
	    << "," << nEvents << "," << fileSize << "," << datastream << "\n";

	return appendToCsv(fname.str(),
	    "file_name,run,first_subrun,last_subrun,n_events,file_size,datastream\n", row.str());
}

DEFINE_ART_SERVICE_INTERFACE_IMPL(ArtdaqRunInfoService, ArtdaqRunInfoServiceInterface)
