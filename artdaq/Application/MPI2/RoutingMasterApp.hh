#ifndef artdaq_Application_MPI2_RoutingMasterApp_hh
#define artdaq_Application_MPI2_RoutingMasterApp_hh

#include <future>
#include <thread>

#include "artdaq/Application/Commandable.hh"
#include "artdaq/Application/MPI2/RoutingMasterCore.hh"

namespace artdaq
{
	class RoutingMasterApp;
}

class artdaq::RoutingMasterApp : public artdaq::Commandable
{
public:
	RoutingMasterApp(MPI_Comm local_group_comm, std::string name);

	RoutingMasterApp(RoutingMasterApp const&) = delete;

	virtual ~RoutingMasterApp() = default;

	RoutingMasterApp& operator=(RoutingMasterApp const&) = delete;

	// these methods provide the operations that are used by the state machine
	bool do_initialize(fhicl::ParameterSet const&, uint64_t, uint64_t) override;

	bool do_start(art::RunID, uint64_t, uint64_t) override;

	bool do_stop(uint64_t, uint64_t) override;

	bool do_pause(uint64_t, uint64_t) override;

	bool do_resume(uint64_t, uint64_t) override;

	bool do_shutdown(uint64_t) override;

	bool do_soft_initialize(fhicl::ParameterSet const&, uint64_t, uint64_t) override;

	bool do_reinitialize(fhicl::ParameterSet const&, uint64_t, uint64_t) override;

	void BootedEnter() override;

	/* Report_ptr */
	std::string report(std::string const&) const override;

private:
	MPI_Comm local_group_comm_;
	std::unique_ptr<artdaq::RoutingMasterCore> routing_master_ptr_;
	std::string name_;
	std::future<size_t> routing_master_future_;
};

#endif /* artdaq_Application_MPI2_RoutingMasterApp_hh */
