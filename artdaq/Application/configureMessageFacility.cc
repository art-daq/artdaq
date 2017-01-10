#include "artdaq/Application/configureMessageFacility.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "fhiclcpp/make_ParameterSet.h"
#include <boost/filesystem.hpp>
#include <unistd.h>
#include <fstream>
#include <sstream>

namespace BFS = boost::filesystem;

void artdaq::configureMessageFacility(char const* progname)
{
	std::string logPathProblem = "";
	std::string logfileName = "";
	char* logRootString = getenv("ARTDAQ_LOG_ROOT");
	char* logFhiclCode = getenv("ARTDAQ_LOG_FHICL");
	char* artdaqMfextensionsDir = getenv("ARTDAQ_MFEXTENSIONS_DIR");

	if (logRootString != nullptr) {
		if (!BFS::exists(logRootString)) {
			logPathProblem = "Log file root directory ";
			logPathProblem.append(logRootString);
			logPathProblem.append(" does not exist!");
		}
		else {
			std::string logfileDir(logRootString);
			logfileDir.append("/");
			logfileDir.append(progname);
			if (!BFS::exists(logfileDir)) {
				logPathProblem = "Log file directory ";
				logPathProblem.append(logfileDir);
				logPathProblem.append(" does not exist!");
			}
			else {
				time_t rawtime;
				struct tm* timeinfo;
				char timeBuff[256];
				time(&rawtime);
				timeinfo = localtime(&rawtime);
				strftime(timeBuff, 256, "%Y%m%d%H%M%S", timeinfo);

				char hostname[256];
				std::string hostString = "";
				if (gethostname(&hostname[0], 256) == 0) {
					std::string tmpString(hostname);
					hostString = tmpString;
					size_t pos = hostString.find(".");
					if (pos != std::string::npos && pos > 2) {
						hostString = hostString.substr(0, pos);
					}
				}

				logfileName.append(logfileDir);
				logfileName.append("/");
				logfileName.append(progname);
				logfileName.append("-");
				logfileName.append(timeBuff);
				logfileName.append("-");
				if (hostString.size() > 0) {
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
		<< "  destinations : { "
		<< "    console : { "
		<< "      type : \"cout\" threshold : \"INFO\" "
		<< "      noTimeStamps : true "
		<< "    } ";

	if (logfileName.length() > 0) {
		ss << "    file : { "
			<< "      type : \"file\" threshold : \"DEBUG\" "
			<< "      filename : \"" << logfileName << "\" "
			<< "      append : false "
			<< "    } ";
	}

	if (artdaqMfextensionsDir != nullptr) {
		ss << "    trace : { "
			<< "       type : \"TRACE\" threshold : \"DEBUG\" "
			<< "    }";
	}

	if (logFhiclCode != nullptr) {
		std::ifstream logfhicl(logFhiclCode);

		if (logfhicl.is_open()) {
			std::stringstream fhiclstream;
			fhiclstream << logfhicl.rdbuf();
			ss << fhiclstream.str();
		}
		else {
			throw cet::exception("configureMessageFacility") <<
				"Unable to open requested fhicl file \"" <<
				logFhiclCode << "\".";
		}
	}

	ss << "  } ";

	fhicl::ParameterSet pset;
	std::string pstr(ss.str());
	fhicl::make_ParameterSet(pstr, pset);

	mf::StartMessageFacility(mf::MessageFacilityService::MultiThread,
		pset);

	mf::SetModuleName(progname);
	mf::SetContext(progname);

	if (logPathProblem.size() > 0) {
		mf::LogError(progname) << logPathProblem;
	}
}

void artdaq::setMsgFacAppName(const std::string& appType, unsigned short port)
{
	std::string appName(appType);

	char hostname[256];
	if (gethostname(&hostname[0], 256) == 0) {
		std::string hostString(hostname);
		size_t pos = hostString.find(".");
		if (pos != std::string::npos && pos > 2) {
			hostString = hostString.substr(0, pos);
		}
		appName.append("-");
		appName.append(hostString);
	}

	appName.append("-");
	appName.append(boost::lexical_cast<std::string>(port));

	mf::SetApplicationName(appName);
}
