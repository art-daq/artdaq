#include "artdaq/DAQdata/configureMessageFacility.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"
#if ART_HEX_VERSION >= 0x20703	// art v2_07_03 means a new versions of fhicl, boost, etc
# include "fhiclcpp/ParameterSet.h"
# include <boost/lexical_cast.hpp>
#endif
#include "fhiclcpp/make_ParameterSet.h"
#include "cetlib_except/exception.h"
#include <boost/filesystem.hpp>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include "trace.h"				// TRACE_CNTL, TRACE

namespace BFS = boost::filesystem;

void artdaq::configureMessageFacility(char const* progname, bool useConsole)
{
	std::string logPathProblem = "";
	std::string logfileName = "";
	char* logRootString = getenv("ARTDAQ_LOG_ROOT");
	char* logFhiclCode = getenv("ARTDAQ_LOG_FHICL");
	char* artdaqMfextensionsDir = getenv("ARTDAQ_MFEXTENSIONS_DIR");

#if 0
	setenv( "TRACE_LVLS", "0xf", 0/*0 = no overwrite*/ );
	unsigned long long lvls=strtoull( getenv("TRACE_LVLS"), NULL, 0 );
	// NOTE: If TRACEs occur before this, they would be done with a different lvl S mask.
	//       To check this, one could: treset;tonMg 0-63; tcntl trig -nTRACE 4 50; <app>
    //       checking for TRACEs before the TRACE below.
	//       If an existing trace file is used, the value of modeS is unchanged.
	TRACE_CNTL( "lvlmskSg", lvls );
	TRACE( 4, "configureMessageFacilit lvlmskSg set to 0x%llx", lvls );
#endif

	if (logRootString != nullptr)
	{
		if (!BFS::exists(logRootString))
		{
			logPathProblem = "Log file root directory ";
			logPathProblem.append(logRootString);
			logPathProblem.append(" does not exist!");
		}
		else
		{
			std::string logfileDir(logRootString);
			logfileDir.append("/");
			logfileDir.append(progname);
			if (!BFS::exists(logfileDir))
			{
				logPathProblem = "Log file directory ";
				logPathProblem.append(logfileDir);
				logPathProblem.append(" does not exist!");
			}
			else
			{
				time_t rawtime;
				struct tm* timeinfo;
				char timeBuff[256];
				time(&rawtime);
				timeinfo = localtime(&rawtime);
				strftime(timeBuff, 256, "%Y%m%d%H%M%S", timeinfo);

				char hostname[256];
				std::string hostString = "";
				if (gethostname(&hostname[0], 256) == 0)
				{
					std::string tmpString(hostname);
					hostString = tmpString;
					size_t pos = hostString.find(".");
					if (pos != std::string::npos && pos > 2)
					{
						hostString = hostString.substr(0, pos);
					}
				}

				logfileName.append(logfileDir);
				logfileName.append("/");
				logfileName.append(progname);
				logfileName.append("-");
				logfileName.append(timeBuff);
				logfileName.append("-");
				if (hostString.size() > 0)
				{
					logfileName.append(hostString);
					logfileName.append("-");
				}
				logfileName.append(boost::lexical_cast<std::string>(getpid()));
				logfileName.append(".log");
			}
		}
	}

	std::ostringstream ss;
	ss << "debugModules:[\"*\"]  statistics:[\"stats\"] "
		<< "  destinations : { ";

	if (useConsole) {
		if (artdaqMfextensionsDir != nullptr)
		{
			ss << "    console : { "
				<< "      type : \"ANSI\" threshold : \"DEBUG\" "
				<< "      noTimeStamps : true "
				<< "      bell_on_error: true "
				<< "    } ";
		}
		else
		{
			ss << "    console : { "
				<< "      type : \"cout\" threshold : \"DEBUG\" "
				<< "      noTimeStamps : true "
				<< "    } ";
		}
	}

	if (logfileName.length() > 0)
	{
		ss << "    file : { "
			<< "      type : \"file\" threshold : \"DEBUG\" "
			<< "      filename : \"" << logfileName << "\" "
			<< "      append : false "
			<< "    } ";
	}

	if (artdaqMfextensionsDir != nullptr)
	{
		ss << "    trace : { "
			<< "       type : \"TRACE\" threshold : \"DEBUG\" format:{noLineBreaks: true} lvls: 0x7 lvlm: 0xF"
			<< "    }";
	}

	if (logFhiclCode != nullptr)
	{
		std::ifstream logfhicl(logFhiclCode);

		if (logfhicl.is_open())
		{
			std::stringstream fhiclstream;
			fhiclstream << logfhicl.rdbuf();
			ss << fhiclstream.str();
		}
		else
		{
			throw cet::exception("configureMessageFacility") <<
			      "Unable to open requested fhicl file \"" <<
			      logFhiclCode << "\".";
		}
	}

	ss << "  } ";

	fhicl::ParameterSet pset;
	std::string pstr(ss.str());
	//std::cout << "Message Facility Config is: " << pstr << std::endl;
	fhicl::make_ParameterSet(pstr, pset);

#  if ART_HEX_VERSION >= 0x20703	// art v2_07_03 means a new versions of fhicl, boost, etc
	mf::StartMessageFacility(pset);

	mf::SetApplicationName(progname);
#  else
	mf::StartMessageFacility(mf::MessageFacilityService::MultiThread,
	                         pset);

	mf::SetModuleName(progname);
	mf::SetContext(progname);
#  endif

	if (logPathProblem.size() > 0)
	{
		mf::LogError(progname) << logPathProblem;
	}
}

void artdaq::setMsgFacAppName(const std::string& appType, unsigned short port)
{
	std::string appName(appType);

	char hostname[256];
	if (gethostname(&hostname[0], 256) == 0)
	{
		std::string hostString(hostname);
		size_t pos = hostString.find(".");
		if (pos != std::string::npos && pos > 2)
		{
			hostString = hostString.substr(0, pos);
		}
		appName.append("-");
		appName.append(hostString);
	}

	appName.append("-");
	appName.append(boost::lexical_cast<std::string>(port));

	mf::SetApplicationName(appName);
}
