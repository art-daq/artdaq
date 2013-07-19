/* DarkSide 50 DAQ program
 * This file add the xmlrpc commander as a client to the SC
 * Author: Alessandro Razeto <Alessandro.Razeto@ge.infn.it>
 */
#include <xmlrpc-c/base.hpp>
#include <xmlrpc-c/registry.hpp>
#include <xmlrpc-c/server_abyss.hpp>
#include <stdexcept>
#include <iostream>
#include "art/Persistency/Provenance/RunID.h"
#include "artdaq/ExternalComms/xmlrpc_commander.hh"
#include "fhiclcpp/make_ParameterSet.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

namespace {
  std::string exception_msg (const std::runtime_error &er,
                             const std::string &helpText="execute request") { 
    std::string msg("Exception when trying to ");
    msg.append(helpText);
    msg.append(": ");
    msg.append(er.what ()); //std::string(er.what ()).substr (2);
    if (msg[msg.size() - 1] == '\n') msg.erase(msg.size() - 1);
    return msg;
  }
  std::string exception_msg (const art::Exception &er,
                             const std::string &helpText) { 
    std::string msg("Exception when trying to ");
    msg.append(helpText);
    msg.append(": ");
    msg.append(er.what ());
    if (msg[msg.size() - 1] == '\n') msg.erase(msg.size() - 1);
    return msg;
  }
  std::string exception_msg (const cet::exception &er,
                             const std::string &helpText) { 
    std::string msg("Exception when trying to ");
    msg.append(helpText);
    msg.append(": ");
    msg.append(er.what ());
    if (msg[msg.size() - 1] == '\n') msg.erase(msg.size() - 1);
    return msg;
  }
  std::string exception_msg (const std::string &erText,
                             const std::string &helpText) { 
    std::string msg("Exception when trying to ");
    msg.append(helpText);
    msg.append(": ");
    msg.append(erText);
    if (msg[msg.size() - 1] == '\n') msg.erase(msg.size() - 1);
    return msg;
  }

  class cmd_: public xmlrpc_c::method {
    public:
      cmd_ (xmlrpc_commander& c, const std::string& signature, const std::string& description): _c(c) { _signature = signature; _help = description; }
      void execute (xmlrpc_c::paramList const& paramList, xmlrpc_c::value * const retvalP) {
	std::unique_lock<std::mutex> lk(_c.mutex_, std::try_to_lock);
	if (lk.owns_lock ()) execute_ (paramList, retvalP);
	else *retvalP = xmlrpc_c::value_string ("busy");
      }
    protected:
      xmlrpc_commander& _c;
      virtual void execute_ (xmlrpc_c::paramList const& paramList, xmlrpc_c::value * const retvalP) = 0;
  };

  class init_: public cmd_ {
    public:
      init_ (xmlrpc_commander& c):
        cmd_(c, "s:s", "initialize the program") {}
      void execute_ (xmlrpc_c::paramList const& paramList, xmlrpc_c::value * const retvalP) override try {
        if (paramList.size() > 0) {
          std::string configString = paramList.getString(0);
          fhicl::ParameterSet pset;
          fhicl::make_ParameterSet(configString, pset);
          if (_c._commandable.initialize(pset)) {
            *retvalP = xmlrpc_c::value_string ("Success"); 
          }
          else {
            std::string problemReport = _c._commandable.report("all");
            *retvalP = xmlrpc_c::value_string (problemReport); 
          }
        }
        else {
          *retvalP = xmlrpc_c::value_string ("The init message requires a single argument that is a string containing the initialization ParameterSet."); 
        }
      } catch (std::runtime_error &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (art::Exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (cet::exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (...) { 
        std::string msg = exception_msg ("Unknown exception", _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } 
  };

  class soft_init_: public cmd_ {
    public:
      soft_init_ (xmlrpc_commander& c):
        cmd_(c, "s:s", "initialize software components in the program") {}
      void execute_ (xmlrpc_c::paramList const& paramList, xmlrpc_c::value * const retvalP) override try {
        if (paramList.size() > 0) {
          std::string configString = paramList.getString(0);
          fhicl::ParameterSet pset;
          fhicl::make_ParameterSet(configString, pset);
          if (_c._commandable.soft_initialize(pset)) {
           *retvalP = xmlrpc_c::value_string ("Success"); 
          }
          else {
            std::string problemReport = _c._commandable.report("all");
            *retvalP = xmlrpc_c::value_string (problemReport); 
          }
        }
        else {
          *retvalP = xmlrpc_c::value_string ("The soft_init message requires a single argument that is a string containing the initialization ParameterSet."); 
        }
      } catch (std::runtime_error &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (art::Exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (cet::exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (...) { 
        std::string msg = exception_msg ("Unknown exception", _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } 
  };

  class reinit_: public cmd_ {
    public:
      reinit_ (xmlrpc_commander& c):
        cmd_(c, "s:s", "re-initialize the program") {}
      void execute_ (xmlrpc_c::paramList const& paramList, xmlrpc_c::value * const retvalP) override try {
        if (paramList.size() > 0) {
          std::string configString = paramList.getString(0);
          fhicl::ParameterSet pset;
          fhicl::make_ParameterSet(configString, pset);
          if (_c._commandable.reinitialize(pset)) {
            *retvalP = xmlrpc_c::value_string ("Success"); 
          }
          else {
            std::string problemReport = _c._commandable.report("all");
            *retvalP = xmlrpc_c::value_string (problemReport); 
          }
        }
        else {
          *retvalP = xmlrpc_c::value_string ("The reinit message requires a single argument that is a string containing the initialization ParameterSet."); 
        }
      } catch (std::runtime_error &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (art::Exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (cet::exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (...) { 
        std::string msg = exception_msg ("Unknown exception", _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } 
  };

  class start_: public cmd_ {
    public:
      start_ (xmlrpc_commander& c):
        cmd_(c, "s:i", "start the run") {}
      void execute_ (xmlrpc_c::paramList const& paramList, xmlrpc_c::value * const retvalP) override try {
        if (paramList.size() > 0) {
          std::string run_number_string = paramList.getString(0);
          art::RunNumber_t run_number =
            boost::lexical_cast<art::RunNumber_t>(run_number_string);
          art::RunID run_id(run_number);
          if (_c._commandable.start(run_id)) {
            *retvalP = xmlrpc_c::value_string ("Success"); 
          }
          else {
            std::string problemReport = _c._commandable.report("all");
            *retvalP = xmlrpc_c::value_string (problemReport); 
          }
        }
        else {
          *retvalP = xmlrpc_c::value_string ("The start message requires the run number as an argument."); 
        }
      } catch (std::runtime_error &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (art::Exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (cet::exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (...) { 
        std::string msg = exception_msg ("Unknown exception", _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } 
  };

  class status_: public cmd_ {
    public:
      status_ (xmlrpc_commander& c):
        cmd_(c, "s:s", "report the current state") {}
      void execute_ (xmlrpc_c::paramList const&, xmlrpc_c::value * const retvalP) override try {
        *retvalP = xmlrpc_c::value_string (_c._commandable.status());
      } catch (std::runtime_error &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (art::Exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (cet::exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (...) { 
        std::string msg = exception_msg ("Unknown exception", _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } 
  };

  class report_: public cmd_ {
    public:
      report_ (xmlrpc_commander& c):
        cmd_(c, "s:s", "report statistics") {}
      void execute_ (xmlrpc_c::paramList const& paramList, xmlrpc_c::value * const retvalP) override try {
        if (paramList.size() > 0) {
          std::string which = paramList.getString(0);
          *retvalP = xmlrpc_c::value_string (_c._commandable.report(which));
        }
        else {
          *retvalP = xmlrpc_c::value_string ("The report message requires a single argument that selects the type of statistics to be reported."); 
        }
      } catch (std::runtime_error &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (art::Exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (cet::exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (...) { 
        std::string msg = exception_msg ("Unknown exception", _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } 
  };

  class reset_stats_: public cmd_ {
    public:
      reset_stats_ (xmlrpc_commander& c):
        cmd_(c, "s:s", "reset statistics") {}
      void execute_ (xmlrpc_c::paramList const& paramList, xmlrpc_c::value * const retvalP) override try {
        if (paramList.size() > 0) {
          std::string which = paramList.getString(0);
          if (_c._commandable.reset_stats(which)) {
            *retvalP = xmlrpc_c::value_string ("Success"); 
          }
          else {
            std::string problemReport = _c._commandable.report("all");
            *retvalP = xmlrpc_c::value_string (problemReport); 
          }
        }
        else {
          *retvalP = xmlrpc_c::value_string ("The reset_stats message requires a single argument that selects the type of statistics to be reset."); 
        }
      } catch (std::runtime_error &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (art::Exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (cet::exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (...) { 
        std::string msg = exception_msg ("Unknown exception", _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } 
  };

  class legal_commands_: public cmd_ {
    public:
      legal_commands_ (xmlrpc_commander& c):
        cmd_(c, "s:n", "return the currently legal commands") {}
      void execute_ (xmlrpc_c::paramList const&, xmlrpc_c::value * const retvalP) override try {
        std::vector<std::string> cmdList = _c._commandable.legal_commands();
        std::string resultString;
        for (unsigned int idx = 0; idx < cmdList.size(); ++idx) {
          if (idx > 0) {resultString.append(" ");}
          resultString.append(cmdList[idx]);
          if (cmdList[idx] == "shutdown") {
            resultString.append(" reset");
          }
        }
        *retvalP = xmlrpc_c::value_string (resultString);
      } catch (std::runtime_error &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (art::Exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (cet::exception &er) { 
        std::string msg = exception_msg (er, _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } catch (...) { 
        std::string msg = exception_msg ("Unknown exception", _help);
        *retvalP = xmlrpc_c::value_string (msg); 
        mf::LogError ("XMLRPC_Commander") << msg;
      } 
  };

#define generate_noarg_class(name, description) \
  class name ## _: public cmd_ { \
    public: \
      name ## _(xmlrpc_commander& c):\
          cmd_(c, "s:n", description) {}\
      void execute_ (xmlrpc_c::paramList const&, xmlrpc_c::value * const retvalP) override try { \
        if (_c._commandable.name()) { \
          *retvalP = xmlrpc_c::value_string ("Success"); \
        } \
        else { \
          std::string problemReport = _c._commandable.report("all"); \
          *retvalP = xmlrpc_c::value_string (problemReport); \
        } \
      } catch (std::runtime_error &er) { \
        std::string msg = exception_msg (er, _help); \
        *retvalP = xmlrpc_c::value_string (msg);  \
        mf::LogError ("XMLRPC_Commander") << msg; \
      } catch (art::Exception &er) {  \
        std::string msg = exception_msg (er, _help); \
        *retvalP = xmlrpc_c::value_string (msg);  \
        mf::LogError ("XMLRPC_Commander") << msg; \
      } catch (cet::exception &er) {  \
        std::string msg = exception_msg (er, _help); \
        *retvalP = xmlrpc_c::value_string (msg);  \
        mf::LogError ("XMLRPC_Commander") << msg; \
      } catch (...) {  \
        std::string msg = exception_msg ("Unknown exception", _help); \
        *retvalP = xmlrpc_c::value_string (msg);  \
        mf::LogError ("XMLRPC_Commander") << msg; \
      } \
  } 

  generate_noarg_class(stop, "stop the run");
  generate_noarg_class(pause, "pause the run");
  generate_noarg_class(resume, "resume the run");
  generate_noarg_class(shutdown, "shut down the program");

#undef generate_noarg_class

#if 0
  class shutdown_: public xmlrpc_c::registry::shutdown {
    public:
      shutdown_ (xmlrpc_c::serverAbyss *server): _server(server) {}

      virtual void doit (const std::string& paramString, void*) const {
        mf::LogInfo("XMLRPC_Commander") << "A shutdown command was sent "
                                        << "with parameter "
                                        << paramString << "\"";
	_server->terminate ();
      }
    private:
      xmlrpc_c::serverAbyss *_server;
  };
#endif
}


xmlrpc_commander::xmlrpc_commander (int port, artdaq::Commandable& commandable):
  _port(port), _commandable(commandable)
{}

void xmlrpc_commander::run() try {
  xmlrpc_c::registry registry;

#define register_method(m) \
  xmlrpc_c::methodPtr const ptr_ ## m(new m ## _(*this));\
  registry.addMethod ("daq." #m, ptr_ ## m);

  register_method(init);
  register_method(soft_init);
  register_method(reinit);
  register_method(start);
  register_method(status);
  register_method(report);
  register_method(stop);
  register_method(pause);
  register_method(resume);
  register_method(reset_stats);
  register_method(legal_commands);

  register_method(shutdown);

  // alias "daq.reset" to the internal shutdown transition
  xmlrpc_c::methodPtr const ptr_reset(new shutdown_(*this));
  registry.addMethod ("daq.reset", ptr_reset);
 
#undef register_method

  xmlrpc_c::serverAbyss server(xmlrpc_c::serverAbyss::constrOpt ().registryP (&registry).portNumber (_port));

#if 0
  shutdown_ shutdown_obj(&server);
  registry.setShutdown (&shutdown_obj);
#endif

  mf::LogDebug ("XMLRPC_Commander") << "running server" << std::endl;

  server.run();

  mf::LogDebug ("XMLRPC_Commander") << "server terminated" << std::endl;

} catch (...) {
  throw;
}