#include "artdaq/DAQdata/Globals.hh"

#include "cetlib/PluginTypeDeducer.h"
#include "cetlib/ProvideMakePluginMacros.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/ConfigurationTable.h"

#include "cetlib/compiler_macros.h"
#include "messagefacility/MessageService/ELdestination.h"
#include "messagefacility/Utilities/ELseverityLevel.h"
#include "messagefacility/Utilities/exception.h"

#define TRACE_NAME "ArtdaqMetric"

namespace mfplugins {
using mf::ELseverityLevel;
using mf::ErrorObj;
using mf::service::ELdestination;

/// <summary>
/// Message Facility destination which logs messages to a TRACE buffer
/// </summary>
class ELArtdaqMetric : public ELdestination
{
public:
	/**
	 * \brief Configuration Parameters for ELArtdaqMetric
	 */
	struct Config
	{
		/// ELDestination common parameters
		fhicl::TableFragment<ELdestination::Config> elDestConfig;
		/// "showDebug" (Default: False): Send metrics for Debug messages
		fhicl::Atom<bool> showDebug =
		    fhicl::Atom<bool>{fhicl::Name{"showDebug"}, fhicl::Comment{"Send Metrics for Debug messages"}, false};
		/// "showInfo" (Default: False): Send metrics for Info messages
		fhicl::Atom<bool> showInfo =
		    fhicl::Atom<bool>{fhicl::Name{"showInfo"}, fhicl::Comment{"Send Metrics for Info messages"}, false};
		/// "showWarning" (Default: true): Send metrics for Warning messages
		fhicl::Atom<bool> showWarning =
		    fhicl::Atom<bool>{fhicl::Name{"showWarning"}, fhicl::Comment{"Send Metrics for Warning messages"}, true};
		/// "showError" (Default: true): Send metrics for Error messages
		fhicl::Atom<bool> showError =
		    fhicl::Atom<bool>{fhicl::Name{"showError"}, fhicl::Comment{"Send Metrics for Error messages"}, true};
		/// "messageLength" (Default: 40): Number of characters to use for metric title
		fhicl::Atom<size_t> messageLength = fhicl::Atom<size_t>(fhicl::Name{"messageLength"}, fhicl::Comment{"Maximum length of metric titles (0 for unlimited)"}, 40);
		/// "metricLevelOffset" (Default: 10): Offset for Metric Levels (+0: summary rates, +1 errors, ...)
		fhicl::Atom<size_t> metricLevelOffset = fhicl::Atom<size_t>(fhicl::Name{"metricLevelOffset"}, fhicl::Comment{"Offset for Metric Levels (+0: summary rates, +1 errors, ...)"}, 10);
	};
	/// Used for ParameterSet validation
	using Parameters = fhicl::WrappedTable<Config>;

public:
	/// <summary>
	/// ELArtdaqMetric Constructor
	/// </summary>
	/// <param name="pset">ParameterSet used to configure ELArtdaqMetric</param>
	ELArtdaqMetric(Parameters const& pset);

	/**
	 * \brief Fill the "Prefix" portion of the message
	 * \param o Output stringstream
	 * \param msg MessageFacility object containing header information
	 */
	void fillPrefix(std::ostringstream& o, const ErrorObj& msg) override;

	/**
	 * \brief Fill the "User Message" portion of the message
	 * \param o Output stringstream
	 * \param msg MessageFacility object containing header information
	 */
	void fillUsrMsg(std::ostringstream& o, const ErrorObj& msg) override;

	/**
	 * \brief Fill the "Suffix" portion of the message (Unused)
	 */
	void fillSuffix(std::ostringstream& /*unused*/, const ErrorObj& /*msg*/) override {}

	/**
	 * \brief Serialize a MessageFacility message to the output
	 * \param o Stringstream object containing message data
	 * \param msg MessageFacility object containing header information
	 */
	void routePayload(const std::ostringstream& o, const ErrorObj& msg) override;

private:
	std::string makeMetricName_(const ErrorObj& msg);
	bool showDebug_{false};
	bool showInfo_{false};
	bool showWarning_{true};
	bool showError_{true};
	size_t messageLength_{20};
	size_t metricLevelOffset_{10};
};

// END DECLARATION
//======================================================================
// BEGIN IMPLEMENTATION

//======================================================================
// ELArtdaqMetric c'tor
//======================================================================
ELArtdaqMetric::ELArtdaqMetric(Parameters const& pset)
    : ELdestination(pset().elDestConfig())
    , showDebug_(pset().showDebug())
    , showInfo_(pset().showInfo())
    , showWarning_(pset().showWarning())
    , showError_(pset().showError())
    , messageLength_(pset().messageLength())
    , metricLevelOffset_(pset().metricLevelOffset())
{
	TLOG(TLVL_INFO) << "ELArtdaqMetric MessageLogger destination plugin initialized.";
}

//======================================================================
// Message prefix filler ( overriddes ELdestination::fillPrefix )
//======================================================================
void ELArtdaqMetric::fillPrefix(std::ostringstream&, const ErrorObj&)
{
}

//======================================================================
// Message filler ( overriddes ELdestination::fillUsrMsg )
//======================================================================
void ELArtdaqMetric::fillUsrMsg(std::ostringstream& oss, const ErrorObj& msg)
{
	std::ostringstream tmposs;
	ELdestination::fillUsrMsg(tmposs, msg);

	// remove "\n" if present
	std::string tmpStr = tmposs.str();
	auto cpos = tmpStr.find(':');
	if (cpos != std::string::npos)
	{
		tmpStr.erase(0, cpos + 1);
	}

	// remove numbers
	std::string usrMsg;
	std::copy_if(tmpStr.begin(), tmpStr.end(),
	             std::back_inserter(usrMsg), isalpha);

	if (messageLength_ > 0 && usrMsg.size() > messageLength_)
	{
		usrMsg.resize(messageLength_);
	}

	oss << usrMsg;
}

//======================================================================
// Message router ( overriddes ELdestination::routePayload )
//======================================================================
void ELArtdaqMetric::routePayload(const std::ostringstream& oss, const ErrorObj& msg)
{
	const auto& xid = msg.xid();
	auto message = oss.str();

	auto level = xid.severity().getLevel();
	int lvlNum = -1;
	bool sendMessageMetric = false;

	switch (level)
	{
		case mf::ELseverityLevel::ELsev_success:
		case mf::ELseverityLevel::ELsev_zeroSeverity:
		case mf::ELseverityLevel::ELsev_unspecified:
			sendMessageMetric = showDebug_;
			lvlNum = 3;
			break;

		case mf::ELseverityLevel::ELsev_info:
			sendMessageMetric = showInfo_;
			lvlNum = 2;
			break;

		case mf::ELseverityLevel::ELsev_warning:
			sendMessageMetric = showWarning_;
			lvlNum = 1;
			break;
		default:
			sendMessageMetric = showError_;
			lvlNum = 0;
			break;
	}

	if (metricMan)
	{
		if (sendMessageMetric)
		{
			metricMan->sendMetric(message, 1, "messages", lvlNum + metricLevelOffset_ + 1, artdaq::MetricMode::Rate);
		}
		switch (lvlNum)
		{
			case 0:
				metricMan->sendMetric("Error Message Rate", 1, "messages", metricLevelOffset_, artdaq::MetricMode::Rate);
				break;
			case 1:
				metricMan->sendMetric("Warning Message Rate", 1, "messages", metricLevelOffset_, artdaq::MetricMode::Rate);
				break;
			case 2:
				metricMan->sendMetric("Info Message Rate", 1, "messages", metricLevelOffset_, artdaq::MetricMode::Rate);
				break;
			case 3:
				metricMan->sendMetric("Debug Message Rate", 1, "messages", metricLevelOffset_, artdaq::MetricMode::Rate);
				break;
		}
	}
}
}  // end namespace mfplugins

//======================================================================
//
// makePlugin function
//
//======================================================================

#ifndef EXTERN_C_FUNC_DECLARE_START
#define EXTERN_C_FUNC_DECLARE_START extern "C" {
#endif

EXTERN_C_FUNC_DECLARE_START
auto makePlugin(const std::string& /*unused*/, const fhicl::ParameterSet& pset)
{
	return std::make_unique<mfplugins::ELArtdaqMetric>(pset);
}
}

DEFINE_BASIC_PLUGINTYPE_FUNC(mf::service::ELdestination)
