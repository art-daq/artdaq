#ifndef ARTDAQ_ARTDAQ_ARTMODULES_BUILDINFO_MODULE_HH_
#define ARTDAQ_ARTDAQ_ARTMODULES_BUILDINFO_MODULE_HH_

#include "TRACE/tracemf.h"
#define TRACE_NAME "BuildInfo"

#include "art/Framework/Core/EDProducer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Run.h"
#include "artdaq-core/Data/PackageBuildInfo.hh"
#include "fhiclcpp/ParameterSet.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace artdaq {
/**
 * \brief BuildInfo is an art::EDProducer which saves information about package builds to the data file
 * \tparam instanceName Tag which the BuildInfo objects will be saved under
 * \tparam Pkgs List of package BuildInfo types
 */
template<std::string* instanceName, typename... Pkgs>
class BuildInfo : public art::EDProducer
{
public:
	/**
	 * \brief BuildInfo module Constructor
	 * \param p ParameterSet used to configure BuildInfo module
	 *
	 * BuildInfo_module expects the following Parameters:
	 * "instance_name": Name which the BuildInfo information will be saved under
	 */
	explicit BuildInfo(fhicl::ParameterSet const& p);

	/**
	 * \brief Default Destructor
	 */
	virtual ~BuildInfo() = default;

	/**
	 * \brief Perform actions at the beginning of the Run
	 * \param r art::Run object
	 *
	 * The BuildInfo information is stored in the Run-level provenance, so this
	 * method performs most of the "work" for this module.
	 */
	void beginRun(art::Run& r) override;

	/**
	 * \brief Perform actions for each event
	 * \param e art::Event object
	 *
	 * This function is a required override for EDProducer, and is a No-Op in BuildInfo_module.
	 */
	void produce(art::Event& e) final override;

private:
	BuildInfo(BuildInfo const&) = delete;
	BuildInfo(BuildInfo&&) = delete;
	BuildInfo& operator=(BuildInfo const&) = delete;
	BuildInfo& operator=(BuildInfo&&) = delete;

	std::unique_ptr<std::vector<PackageBuildInfo>> packages_;
	std::string instanceName_;

	template<typename... Args>
	struct fill_packages;

	template<typename Arg>
	struct fill_packages<Arg>
	{
		static void doit(std::vector<PackageBuildInfo>& packages)
		{
			packages.emplace_back(Arg::getPackageBuildInfo());
		}
	};

	template<typename Arg, typename... Args>
	struct fill_packages<Arg, Args...>
	{
		static void doit(std::vector<PackageBuildInfo>& packages)
		{
			packages.emplace_back(Arg::getPackageBuildInfo());
			fill_packages<Args...>::doit(packages);
		}
	};
};

template<std::string* instanceName, typename... Pkgs>
BuildInfo<instanceName, Pkgs...>::BuildInfo(fhicl::ParameterSet const& ps)
    : EDProducer(ps)
    , packages_(new std::vector<PackageBuildInfo>())
    , instanceName_(ps.get<std::string>("instance_name", *instanceName))
{
	fill_packages<Pkgs...>::doit(*packages_);

	produces<std::vector<PackageBuildInfo>, art::InRun>(instanceName_);
}

template<std::string* instanceName, typename... Pkgs>
void BuildInfo<instanceName, Pkgs...>::beginRun(art::Run& r)
{
	// JCF, 9/22/14

	// Previously, the vector pointed to by the member variable
	// packages_ itself got stored in output on the call to "r.put()"
	// below; what would then happen is that at the start of a new run
	// or subrun, when r.put() got called again an exception would be
	// thrown because packages_ would now be null thanks to the
	// previous call to std::move. To make sure this doesn't happen, I
	// now stash a copy of the vector pointed to by packages_, not the
	// original member vector

	auto packages_deep_copy_ptr = std::make_unique<std::vector<PackageBuildInfo>>(*packages_);

	TLOG(TLVL_INFO, "BuildInfo") << "Placing BuildInfo with instance name \"" << instanceName_ << "\"";
	for (auto& pbi : *packages_)
	{
		TLOG(TLVL_DEBUG) << "Package " << pbi.getPackageName() << ": version " << pbi.getPackageVersion() << " built at " << pbi.getBuildTimestamp();
	}
	r.put(std::move(packages_deep_copy_ptr), instanceName_, art::fullRun());
}

template<std::string* instanceName, typename... Pkgs>
void BuildInfo<instanceName, Pkgs...>::produce(art::Event&)
{
	// nothing to be done for individual events
}
}  // namespace artdaq

#endif  // ARTDAQ_ARTDAQ_ARTMODULES_BUILDINFO_MODULE_HH_
