#ifndef ARTDAQ_TRANSFERPLUGINS_DETAIL_HOSTMAP_HH
#define ARTDAQ_TRANSFERPLUGINS_DETAIL_HOSTMAP_HH

#include <string>
#include <vector>
#include <map>
#include "fhiclcpp/ParameterSet.h"
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "fhiclcpp/types/Sequence.h"
#include "fhiclcpp/types/Table.h"
#include "fhiclcpp/types/Atom.h"

namespace artdaq {

	struct HostMap
	{
		struct HostConfig
		{
			fhicl::Atom<int> rank{ fhicl::Name{"rank"}, fhicl::Comment{"Rank index"} };
			fhicl::Atom<std::string> host{ fhicl::Name{"host"}, fhicl::Comment{"Hostname for artdaq application with this rank"} };
			fhicl::Atom<int> portOffset{ fhicl::Name{"portOffset"},fhicl::Comment{"DEPRECATED: Port offset of this artdaq application"}, 5500 };
		};
		struct Config
		{
			fhicl::Sequence<fhicl::Table<HostConfig>> host_map{ fhicl::Name("host_map"), fhicl::Comment("List of artdaq applications by rank and location") };
		};
	};

	struct DestinationInfo
	{
		std::string hostname;
		int portOffset;
	};
	typedef std::map<int, DestinationInfo> hostMap_t;

	inline std::vector<fhicl::ParameterSet> MakeHostMapPset(std::map<int, DestinationInfo> input)
	{
		std::vector<fhicl::ParameterSet> output;
		for (auto& rank : input)
		{
			fhicl::ParameterSet rank_output;
			rank_output.put<int>("rank", rank.first);
			rank_output.put<std::string>("host", rank.second.hostname);
			rank_output.put<int>("portOffset", rank.second.portOffset);
			output.push_back(rank_output);
		}
		return output;
	}

	inline hostMap_t MakeHostMap(fhicl::ParameterSet pset, int masterPortOffset = 0, hostMap_t output = hostMap_t())
	{
		if (pset.has_key("host_map")) {
			auto hosts = pset.get<std::vector<fhicl::ParameterSet>>("host_map");
			for (auto& ps : hosts)
			{
				auto rank = ps.get<int>("rank", TransferInterface::RECV_TIMEOUT);
				DestinationInfo info;
				info.hostname = ps.get<std::string>("host", "localhost");
				info.portOffset = ps.get<int>("portOffset", 5500) + masterPortOffset;

				if (output.count(rank) && (output[rank].hostname != info.hostname || output[rank].portOffset != info.portOffset))
				{
					TLOG(TLVL_ERROR) << "Inconsistent host maps supplied! Check configuration! There may be TCPSocket-related failures!";
				}
				output[rank] = info;
			}
		}
		return output;
	}
}

#endif