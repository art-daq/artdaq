////////////////////////////////////////////////////////////////////////
// Class:       PrintBuildInfo
// Module Type: analyzer
// File:        PrintBuildInfo_module.cc
//
// Generated at Fri Aug 15 21:05:07 2014 by lbnedaq using artmod
// from cetpkgsupport v1_05_02.
////////////////////////////////////////////////////////////////////////

#include "TRACE/tracemf.h"

#include "artdaq-core/Data/PackageBuildInfo.hh"

#include "art/Framework/Core/EDAnalyzer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/Run.h"
#include "art/Framework/Principal/SubRun.h"
#include "canvas/Utilities/InputTag.h"
#include "fhiclcpp/ParameterSet.h"

#include <iostream>

#define TRACE_NAME "PrintBuildInfo"

namespace artdaq {
class PrintBuildInfo;
}

/**
 * \brief An art::EDAnalyzer which prints any artdaq::BuildInfo objects stored in the run
 */
class artdaq::PrintBuildInfo : public art::EDAnalyzer
{
public:
	/**
	 * \brief PrintBuildInfo Constructor
	 * \param p ParameterSet used to configure PrintBuildInfo
	 *
	 * \verbatim
	 * PrintBuildInfo accepts the following Parameters:
	 * "buildinfo_module_label" (REQUIRED): The module label for the BuildInfo objects
	 * "buildinfo_instance_label" (REQUIRED): The instance label for the BuildInfo objects
	 * \endverbatim
	 *
	 * These parameters should match those given to the BuildInfo module
	 */
	explicit PrintBuildInfo(fhicl::ParameterSet const& p);

	/**
	 * \brief Default virtual Destructor
	 */
	~PrintBuildInfo() override = default;

	/**
	 * \brief Called for each event. Required overload for art::EDAnalyzer, No-Op here.
	 */
	void analyze(art::Event const& /*unused*/) override {}

	/**
	 * \brief Perform actions at the beginning of the run
	 * \param run art::Run object
	 *
	 * This function pretty-prints the BuildInfo information form the run object with the
	 * configured module label and instance label.
	 */
	void beginRun(art::Run const& run) override;

private:
	PrintBuildInfo(PrintBuildInfo const&) = delete;
	PrintBuildInfo(PrintBuildInfo&&) = delete;
	PrintBuildInfo& operator=(PrintBuildInfo const&) = delete;
	PrintBuildInfo& operator=(PrintBuildInfo&&) = delete;

	std::string buildinfo_module_label_;
	std::string buildinfo_instance_label_;
};

artdaq::PrintBuildInfo::PrintBuildInfo(fhicl::ParameterSet const& pset)
    : EDAnalyzer(pset)
    , buildinfo_module_label_(pset.get<std::string>("buildinfo_module_label"))
    , buildinfo_instance_label_(pset.get<std::string>("buildinfo_instance_label")) {}

void artdaq::PrintBuildInfo::beginRun(art::Run const& run)
{
	art::Handle<std::vector<artdaq::PackageBuildInfo>> raw;

	TLOG(TLVL_INFO) << "Calling getByLabel: module label: " << buildinfo_module_label_ << ", instance label: " << buildinfo_instance_label_;
	run.getByLabel(buildinfo_module_label_, buildinfo_instance_label_, raw);

	if (raw.isValid())
	{
		std::cout << "--------------------------------------------------------------" << std::endl;
		std::cout.width(20);
		std::cout << std::left << "Package"
		          << "|";
		std::cout.width(20);
		std::cout << std::left << "Version"
		          << "|";
		std::cout.width(20);
		std::cout << std::left << "Timestamp" << std::endl;

		for (const auto& pkg : *raw)
		{
			std::cout.width(20);
			std::cout << std::left << pkg.getPackageName() << "|";
			std::cout.width(20);
			std::cout << std::left << pkg.getPackageVersion() << "|";
			std::cout.width(20);
			std::cout << std::left << pkg.getBuildTimestamp() << std::endl;
		}

		std::cout << "--------------------------------------------------------------" << std::endl;
	}
	else
	{
		std::cerr << "\n"
		          << std::endl;
		std::cerr << "Warning in artdaq::PrintBuildInfo module: Run " << run.run() << " appears not to have found product instance \"" << buildinfo_instance_label_ << "\" of module \"" << buildinfo_module_label_ << "\"" << std::endl;
		std::cerr << "\n"
		          << std::endl;
	}
}

DEFINE_ART_MODULE(artdaq::PrintBuildInfo)  // NOLINT(performance-unnecessary-value-param)
