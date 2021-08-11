//
// ex: set ro:
// DO NOT EDIT.
// generated by smc (http://smc.sourceforge.net/)
// from file : Commandable.sm
//

#ifndef COMMANDABLE_SM_H
#define COMMANDABLE_SM_H

#define SMC_USES_IOSTREAMS

#include <artdaq/Application/detail/statemap.h>
#include <canvas/Persistency/Provenance/RunID.h>
#include <fhiclcpp/ParameterSet.h>

namespace artdaq {
// Forward declarations.
class Main;
class Main_Booted;
class Main_Initialized;
class Main_Default;
class InitializedMap;
class InitializedMap_Ready;
class InitializedMap_InRun;
class InitializedMap_Default;
class InRunMap;
class InRunMap_Running;
class InRunMap_Paused;
class InRunMap_InRunError;
class InRunMap_Default;
class CommandableState;
class CommandableContext;
class Commandable;

class CommandableState : public statemap::State
{
public:
	CommandableState(const char* const name, const int stateId)
	    : statemap::State(name, stateId){};

	virtual void Entry(CommandableContext&){};
	virtual void Exit(CommandableContext&){};

	virtual void in_run_failure(CommandableContext& context);
	virtual void init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void resume(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void shutdown(CommandableContext& context, uint64_t timeout);
	virtual void soft_init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void start(CommandableContext& context, art::RunID id, uint64_t timeout, uint64_t timestamp);
	virtual void stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp);

protected:
	virtual void Default(CommandableContext& context);
};

class Main
{
public:
	static Main_Booted Booted;
	static Main_Initialized Initialized;
};

class Main_Default : public CommandableState
{
public:
	Main_Default(const char* const name, const int stateId)
	    : CommandableState(name, stateId){};

	virtual void init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void start(CommandableContext& context, art::RunID id, uint64_t timeout, uint64_t timestamp);
	virtual void stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void resume(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void shutdown(CommandableContext& context, uint64_t timeout);
	virtual void soft_init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
};

class Main_Booted : public Main_Default
{
public:
	Main_Booted(const char* const name, const int stateId)
	    : Main_Default(name, stateId){};

	virtual void Entry(CommandableContext&);
	virtual void init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void shutdown(CommandableContext& context, uint64_t timeout);
};

class Main_Initialized : public Main_Default
{
public:
	Main_Initialized(const char* const name, const int stateId)
	    : Main_Default(name, stateId){};

	virtual void init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void shutdown(CommandableContext& context, uint64_t timeout);
};

class InitializedMap
{
public:
	static InitializedMap_Ready Ready;
	static InitializedMap_InRun InRun;
};

class InitializedMap_Default : public CommandableState
{
public:
	InitializedMap_Default(const char* const name, const int stateId)
	    : CommandableState(name, stateId){};

	virtual void init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void start(CommandableContext& context, art::RunID id, uint64_t timeout, uint64_t timestamp);
	virtual void stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void resume(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void shutdown(CommandableContext& context, uint64_t timeout);
	virtual void soft_init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
};

class InitializedMap_Ready : public InitializedMap_Default
{
public:
	InitializedMap_Ready(const char* const name, const int stateId)
	    : InitializedMap_Default(name, stateId){};

	virtual void init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void shutdown(CommandableContext& context, uint64_t timeout);
	virtual void soft_init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void start(CommandableContext& context, art::RunID id, uint64_t timeout, uint64_t timestamp);
};

class InitializedMap_InRun : public InitializedMap_Default
{
public:
	InitializedMap_InRun(const char* const name, const int stateId)
	    : InitializedMap_Default(name, stateId){};

	virtual void Exit(CommandableContext&);
	virtual void stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
};

class InRunMap
{
public:
	static InRunMap_Running Running;
	static InRunMap_Paused Paused;
	static InRunMap_InRunError InRunError;
};

class InRunMap_Default : public CommandableState
{
public:
	InRunMap_Default(const char* const name, const int stateId)
	    : CommandableState(name, stateId){};

	virtual void init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void start(CommandableContext& context, art::RunID id, uint64_t timeout, uint64_t timestamp);
	virtual void stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void resume(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void shutdown(CommandableContext& context, uint64_t timeout);
	virtual void soft_init(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
	virtual void reinit(CommandableContext& context, fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);
};

class InRunMap_Running : public InRunMap_Default
{
public:
	InRunMap_Running(const char* const name, const int stateId)
	    : InRunMap_Default(name, stateId){};

	virtual void in_run_failure(CommandableContext& context);
	virtual void pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
};

class InRunMap_Paused : public InRunMap_Default
{
public:
	InRunMap_Paused(const char* const name, const int stateId)
	    : InRunMap_Default(name, stateId){};

	virtual void resume(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
};

class InRunMap_InRunError : public InRunMap_Default
{
public:
	InRunMap_InRunError(const char* const name, const int stateId)
	    : InRunMap_Default(name, stateId){};

	virtual void pause(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
	virtual void stop(CommandableContext& context, uint64_t timeout, uint64_t timestamp);
};

class CommandableContext : public statemap::FSMContext
{
public:
	explicit CommandableContext(Commandable& owner)
	    : FSMContext(Main::Booted), _owner(owner){};

	CommandableContext(Commandable& owner, const statemap::State& state)
	    : FSMContext(state), _owner(owner){};

	virtual void enterStartState()
	{
		getState().Entry(*this);
		return;
	}

	inline Commandable& getOwner()
	{
		return (_owner);
	};

	inline CommandableState& getState()
	{
		if (_state == NULL)
		{
			throw statemap::StateUndefinedException();
		}

		return dynamic_cast<CommandableState&>(*_state);
	};

	inline void in_run_failure()
	{
		getState().in_run_failure(*this);
	};

	inline void init(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
	{
		getState().init(*this, pset, timeout, timestamp);
	};

	inline void pause(uint64_t timeout, uint64_t timestamp)
	{
		getState().pause(*this, timeout, timestamp);
	};

	inline void reinit(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
	{
		getState().reinit(*this, pset, timeout, timestamp);
	};

	inline void resume(uint64_t timeout, uint64_t timestamp)
	{
		getState().resume(*this, timeout, timestamp);
	};

	inline void shutdown(uint64_t timeout)
	{
		getState().shutdown(*this, timeout);
	};

	inline void soft_init(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
	{
		getState().soft_init(*this, pset, timeout, timestamp);
	};

	inline void start(art::RunID id, uint64_t timeout, uint64_t timestamp)
	{
		getState().start(*this, id, timeout, timestamp);
	};

	inline void stop(uint64_t timeout, uint64_t timestamp)
	{
		getState().stop(*this, timeout, timestamp);
	};

private:
	Commandable& _owner;
};
}  // namespace artdaq

#endif  // COMMANDABLE_SM_H

//
// Local variables:
//  buffer-read-only: t
// End:
//
