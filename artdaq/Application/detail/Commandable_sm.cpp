//
// ex: set ro:
// DO NOT EDIT.
// generated by smc (http://smc.sourceforge.net/)
// from file : Commandable.sm
//

#include "artdaq/Application/detail/Commandable_sm.h"
#include "artdaq/Application/Commandable.hh"

using namespace statemap;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

namespace artdaq {
// Static class declarations.
Main_Booted Main::Booted("Main::Booted", 0);
Main_Initialized Main::Initialized("Main::Initialized", 1);
InitializedMap_Ready InitializedMap::Ready("InitializedMap::Ready", 2);
InitializedMap_InRun InitializedMap::InRun("InitializedMap::InRun", 3);
InRunMap_Running InRunMap::Running("InRunMap::Running", 4);
InRunMap_Paused InRunMap::Paused("InRunMap::Paused", 5);
InRunMap_InRunError InRunMap::InRunError("InRunMap::InRunError", 6);

void CommandableState::in_run_failure(CommandableContext& context)
{
	Default(context);
}

void CommandableState::init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Default(context);
}

void CommandableState::pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Default(context);
}

void CommandableState::reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Default(context);
}

void CommandableState::resume(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Default(context);
}

void CommandableState::shutdown(CommandableContext& context, uint64_t timeout)
{
	Default(context);
}

void CommandableState::soft_init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Default(context);
}

void CommandableState::start(CommandableContext& context, art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	Default(context);
}

void CommandableState::stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Default(context);
}

void CommandableState::Default(CommandableContext& context)
{
	throw(
	    TransitionUndefinedException(
	        context.getState().getName(),
	        context.getTransition()));
}

void Main_Default::init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void Main_Default::start(CommandableContext& context, art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void Main_Default::stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void Main_Default::pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void Main_Default::resume(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void Main_Default::shutdown(CommandableContext& context, uint64_t timeout)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void Main_Default::soft_init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void Main_Default::reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void Main_Booted::Entry(CommandableContext& context)

{
	Commandable& ctxt = context.getOwner();

	ctxt.BootedEnter();
}

void Main_Booted::init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	if (ctxt.do_initialize(pset, timeout, timestamp))
	{
		context.getState().Exit(context);
		// No actions.
		context.setState(Main::Initialized);

		context.getState().Entry(context);
		context.pushState(InitializedMap::Ready);
		context.getState().Entry(context);
	}
	else
	{
	}
}

void Main_Booted::reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	if (ctxt.do_initialize(pset, timeout, timestamp))
	{
		context.getState().Exit(context);
		// No actions.
		context.setState(Main::Initialized);

		context.getState().Entry(context);
		context.pushState(InitializedMap::Ready);
		context.getState().Entry(context);
	}
	else
	{
	}
}

void Main_Booted::shutdown(CommandableContext& context, uint64_t timeout)
{
	context.getState().Exit(context);
	context.setState(Main::Booted);
	context.getState().Entry(context);
}

void Main_Initialized::init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	if (ctxt.do_initialize(pset, timeout, timestamp))
	{
		context.getState().Exit(context);
		// No actions.
		context.setState(Main::Initialized);

		context.getState().Entry(context);
		context.pushState(InitializedMap::Ready);
		context.getState().Entry(context);
	}
	else
	{
		context.getState().Exit(context);
		context.setState(Main::Booted);
		context.getState().Entry(context);
	}
}

void Main_Initialized::reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	if (ctxt.do_initialize(pset, timeout, timestamp))
	{
		context.getState().Exit(context);
		// No actions.
		context.setState(Main::Initialized);

		context.getState().Entry(context);
		context.pushState(InitializedMap::Ready);
		context.getState().Entry(context);
	}
	else
	{
		context.getState().Exit(context);
		context.setState(Main::Booted);
		context.getState().Entry(context);
	}
}

void Main_Initialized::shutdown(CommandableContext& context, uint64_t timeout)
{
	Commandable& ctxt = context.getOwner();

	if (ctxt.do_shutdown(timeout))
	{
		context.getState().Exit(context);
		// No actions.
		context.setState(Main::Booted);
		context.getState().Entry(context);
	}
	else
	{
		Main_Default::shutdown(context, timeout);
	}
}

void InitializedMap_Default::init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InitializedMap_Default::start(CommandableContext& context, art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InitializedMap_Default::stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InitializedMap_Default::pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InitializedMap_Default::resume(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InitializedMap_Default::shutdown(CommandableContext& context, uint64_t timeout)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InitializedMap_Default::soft_init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InitializedMap_Default::reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InitializedMap_Ready::init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	context.getState().Exit(context);
	context.popState();
	context.init(pset, timeout, timestamp);
}

void InitializedMap_Ready::reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.do_reinitialize(pset, timeout, timestamp);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InitializedMap_Ready::shutdown(CommandableContext& context, uint64_t timeout)
{
	context.getState().Exit(context);
	context.popState();
	context.shutdown(timeout);
}

void InitializedMap_Ready::soft_init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.do_soft_initialize(pset, timeout, timestamp);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InitializedMap_Ready::start(CommandableContext& context, art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	if (ctxt.do_start(id, timeout, timestamp))
	{
		context.getState().Exit(context);
		// No actions.
		context.setState(InitializedMap::InRun);

		context.getState().Entry(context);
		context.pushState(InRunMap::Running);
		context.getState().Entry(context);
	}
	else
	{
	}
}

void InitializedMap_InRun::Exit(CommandableContext& context)

{
	Commandable& ctxt = context.getOwner();

	ctxt.InRunExit();
}

void InitializedMap_InRun::stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	if (ctxt.do_stop(timeout, timestamp))
	{
		context.getState().Exit(context);
		// No actions.
		context.setState(InitializedMap::Ready);
		context.getState().Entry(context);
	}
	else
	{
		InitializedMap_Default::stop(context, timeout, timestamp);
	}
}

void InRunMap_Default::init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InRunMap_Default::start(CommandableContext& context, art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InRunMap_Default::stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InRunMap_Default::pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InRunMap_Default::resume(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InRunMap_Default::shutdown(CommandableContext& context, uint64_t timeout)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InRunMap_Default::soft_init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InRunMap_Default::reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	CommandableState& endState = context.getState();

	context.clearState();
	try
	{
		ctxt.badTransition(__func__);
		context.setState(endState);
	}
	catch (...)
	{
		context.setState(endState);
		throw;
	}
}

void InRunMap_Running::in_run_failure(CommandableContext& context)
{
	context.getState().Exit(context);
	context.setState(InRunMap::InRunError);
	context.getState().Entry(context);
}

void InRunMap_Running::pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	if (ctxt.do_pause(timeout, timestamp))
	{
		context.getState().Exit(context);
		// No actions.
		context.setState(InRunMap::Paused);
		context.getState().Entry(context);
	}
	else
	{
	}
}

void InRunMap_Running::stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	context.getState().Exit(context);
	context.popState();
	context.stop(timeout, timestamp);
}

void InRunMap_Paused::resume(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	if (ctxt.do_resume(timeout, timestamp))
	{
		context.getState().Exit(context);
		// No actions.
		context.setState(InRunMap::Running);
		context.getState().Entry(context);
	}
	else
	{
	}
}

void InRunMap_Paused::stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	context.getState().Exit(context);
	context.popState();
	context.stop(timeout, timestamp);
}

void InRunMap_InRunError::pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	Commandable& ctxt = context.getOwner();

	if (ctxt.do_pause(timeout, timestamp))
	{
		context.getState().Exit(context);
		// No actions.
		context.setState(InRunMap::Paused);
		context.getState().Entry(context);
	}
	else
	{
	}
}

void InRunMap_InRunError::stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp)
{
	context.getState().Exit(context);
	context.popState();
	context.stop(timeout, timestamp);
}
}  // namespace artdaq

#pragma GCC diagnostic pop
//
// Local variables:
//  buffer-read-only: t
// End:
//
