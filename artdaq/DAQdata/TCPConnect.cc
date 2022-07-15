//  This file (TCPConnect.cc) was created by Ron Rechenmacher <ron@fnal.gov> on
//  Apr 26, 2010. "TERMS AND CONDITIONS" governing this file are in the README
//  or COPYING file. If you do not have such a file, one can be obtained by
//  contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
//  $RCSfile: TCPConnect.cpp,v $
//  rev="$Revision: 1.4 $$Date: 2010/06/24 03:49:45 $";

#include "artdaq/DAQdata/TCPConnect.hh"

#include "TRACE/tracemf.h"
#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_TCPConnect").c_str()

#include <arpa/inet.h>   // inet_aton
#include <netdb.h>       // gethostbyname
#include <netinet/in.h>  // struct sockaddr_in
#include <netinet/in.h>  // inet_aton
#include <sys/socket.h>  // socket, bind, listen, accept
#include <sys/socket.h>  // inet_aton
#include <sys/types.h>   // socket, bind, listen, accept
#include <unistd.h>      // close
#include <cstdio>        // printf
#include <cstdlib>       // exit
#include <cstring>       // bzero

#include <ifaddrs.h>
#include <linux/if_link.h>

#include <map>
#include <regex>
#include <string>

// Return sts, put result in addr
int ResolveHost(char const *host_in, in_addr &addr)
{
	std::string host;
	struct hostent *hostent_sp;
	std::cmatch mm;
	//  Note: the regex expression used by regex_match has an implied ^ and $
	//        at the beginning and end respectively.
	if (regex_match(host_in, mm, std::regex("([^:]+):(\\d+)")))
	{
		host = mm[1].str();
	}
	else if (regex_match(host_in, mm, std::regex(":{0,1}(\\d+)")))
	{
		host = std::string("127.0.0.1");
	}
	else if (regex_match(host_in, mm, std::regex("([^:]+):{0,1}")))
	{
		host = mm[1].str();
	}
	else
	{
		host = std::string("127.0.0.1");
	}
	TLOG(TLVL_INFO) << "Resolving host " << host;

	memset(static_cast<void *>(&addr), 0, sizeof(addr));

	if (regex_match(host.c_str(), mm, std::regex(R"(\d+(\.\d+){3})")))
	{
		inet_aton(host.c_str(), &addr);
	}
	else
	{
		hostent_sp = gethostbyname(host.c_str());
		if (hostent_sp == nullptr)
		{
			perror("gethostbyname");
			return (-1);
		}
		addr = *reinterpret_cast<struct in_addr *>(hostent_sp->h_addr_list[0]);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)
	}
	return 0;
}

int GetIPOfInterface(const std::string &interface_name, in_addr &addr)
{
	int sts = 0;

	TLOG(TLVL_INFO) << "Finding address for interface " << interface_name;

	memset(static_cast<void *>(&addr), 0, sizeof(addr));

	struct ifaddrs *ifaddr, *ifa;

	if (getifaddrs(&ifaddr) == -1)
	{
		perror("getifaddrs");
		return -1;
	}

	/* Walk through linked list, maintaining head pointer so we
	can free list later */

	for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next)
	{
		if (ifa->ifa_addr == nullptr)
		{
			continue;
		}

		/* For an AF_INET* interface address, display the address */

		if (ifa->ifa_addr->sa_family == AF_INET)
		{
			auto if_addr = reinterpret_cast<struct sockaddr_in *>(ifa->ifa_addr);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

			TLOG(TLVL_DEBUG + 35) << "IF: " << ifa->ifa_name << " Desired: " << interface_name << " IP: " << if_addr->sin_addr.s_addr;

			if (std::string(ifa->ifa_name) == interface_name)
			{
				TLOG(TLVL_INFO) << "Interface " << ifa->ifa_name << " matches " << interface_name << " IP: " << if_addr->sin_addr.s_addr;
				memcpy(&addr, &if_addr->sin_addr, sizeof(addr));
				break;
			}
		}
	}
	if (ifa == nullptr)
	{
		TLOG(TLVL_WARNING) << "No matches for if " << interface_name << ", using 0.0.0.0";
		inet_aton("0.0.0.0", &addr);
		sts = 2;
	}

	freeifaddrs(ifaddr);

	return sts;
}

int AutodetectPrivateInterface(in_addr &addr)
{
	int sts = 0;

	memset(static_cast<void *>(&addr), 0, sizeof(addr));

	struct ifaddrs *ifaddr, *ifa;

	enum ip_preference : int
	{
		IP_192 = 1,
		IP_172 = 2,
		IP_10 = 3,
		IP_131 = 4
	};

	struct in_addr addr_192, addr_172, addr_10, addr_131, nm_16, nm_12, nm_8;
	inet_aton("192.168.0.0", &addr_192);
	inet_aton("172.16.0.0", &addr_172);
	inet_aton("10.0.0.0", &addr_10);
	inet_aton("131.225.0.0", &addr_131);
	inet_aton("255.255.0.0", &nm_16);
	inet_aton("255.240.0.0", &nm_12);
	inet_aton("255.0.0.0", &nm_8);

	std::map<ip_preference, in_addr> preference_map;

	if (getifaddrs(&ifaddr) == -1)
	{
		perror("getifaddrs");
		return -1;
	}

	/* Walk through linked list, maintaining head pointer so we
	can free list later */

	for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next)
	{
		if (ifa->ifa_addr == nullptr)
		{
			continue;
		}

		/* For an AF_INET* interface address, display the address */

		if (ifa->ifa_addr->sa_family == AF_INET)
		{
			auto if_addr = reinterpret_cast<struct sockaddr_in *>(ifa->ifa_addr);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

			TLOG(TLVL_DEBUG + 35) << "IF: " << ifa->ifa_name << " IP: " << if_addr->sin_addr.s_addr;

			if (preference_map.count(IP_192) == 0 && (if_addr->sin_addr.s_addr & nm_16.s_addr) == addr_192.s_addr)
			{
				preference_map[IP_192];
				memcpy(&preference_map[IP_192], &if_addr->sin_addr, sizeof(addr));
			}
			else if (preference_map.count(IP_172) == 0 && (if_addr->sin_addr.s_addr & nm_12.s_addr) == addr_172.s_addr)
			{
				preference_map[IP_172];
				memcpy(&preference_map[IP_172], &if_addr->sin_addr, sizeof(addr));
			}
			else if (preference_map.count(IP_10) == 0 && (if_addr->sin_addr.s_addr & nm_8.s_addr) == addr_10.s_addr)
			{
				preference_map[IP_10];
				memcpy(&preference_map[IP_10], &if_addr->sin_addr, sizeof(addr));
			}
			else if (preference_map.count(IP_131) == 0 && (if_addr->sin_addr.s_addr & nm_16.s_addr) == addr_131.s_addr)
			{
				preference_map[IP_131];
				memcpy(&preference_map[IP_131], &if_addr->sin_addr, sizeof(addr));
			}
		}
	}

	if (preference_map.empty())
	{
		TLOG(TLVL_WARNING) << "AutodetectPrivateInterface: No matches, using 0.0.0.0";
		inet_aton("0.0.0.0", &addr);
		sts = 2;
	}
	else
	{
		TLOG(TLVL_INFO) << "AutodetectPrivateInterface: Using " << inet_ntoa(addr);
		memcpy(&addr, &preference_map.begin()->second, sizeof(addr));
	}

	freeifaddrs(ifaddr);

	return sts;
}

// Return sts, put result in addr
int GetInterfaceForNetwork(char const *host_in, in_addr &addr)
{
	std::string host;
	struct hostent *hostent_sp;
	std::cmatch mm;
	int sts = 0;
	//  Note: the regex expression used by regex_match has an implied ^ and $
	//        at the beginning and end respectively.
	if (regex_match(host_in, mm, std::regex("([^:]+):(\\d+)")))
	{
		host = mm[1].str();
	}
	else if (regex_match(host_in, mm, std::regex(":{0,1}(\\d+)")))
	{
		host = std::string("127.0.0.1");
	}
	else if (regex_match(host_in, mm, std::regex("([^:]+):{0,1}")))
	{
		host = mm[1].str();
	}
	else
	{
		host = std::string("127.0.0.1");
	}
	TLOG(TLVL_INFO) << "Resolving ip " << host;

	memset(static_cast<void *>(&addr), 0, sizeof(addr));

	if (regex_match(host.c_str(), mm, std::regex(R"(\d+(\.\d+){3})")))
	{
		in_addr desired_host;
		inet_aton(host.c_str(), &desired_host);
		struct ifaddrs *ifaddr, *ifa;

		if (getifaddrs(&ifaddr) == -1)
		{
			perror("getifaddrs");
			return -1;
		}

		/* Walk through linked list, maintaining head pointer so we
		can free list later */

		for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next)
		{
			if (ifa->ifa_addr == nullptr)
			{
				continue;
			}

			/* For an AF_INET* interface address, display the address */

			if (ifa->ifa_addr->sa_family == AF_INET)
			{
				auto if_addr = reinterpret_cast<struct sockaddr_in *>(ifa->ifa_addr);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
				auto sa = reinterpret_cast<struct sockaddr_in *>(ifa->ifa_netmask);    // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

				TLOG(TLVL_DEBUG + 35) << "IF: " << ifa->ifa_name << " Desired: " << desired_host.s_addr << " netmask: " << sa->sin_addr.s_addr << " this interface: " << if_addr->sin_addr.s_addr;

				if ((if_addr->sin_addr.s_addr & sa->sin_addr.s_addr) == (desired_host.s_addr & sa->sin_addr.s_addr))
				{
					TLOG(TLVL_INFO) << "Using interface " << ifa->ifa_name;
					memcpy(&addr, &if_addr->sin_addr, sizeof(addr));
					break;
				}
			}
		}
		if (ifa == nullptr)
		{
			if (host != std::string("0.0.0.0"))
			{
				TLOG(TLVL_WARNING) << "No matches for ip " << host << ", using 0.0.0.0";
			}
			inet_aton("0.0.0.0", &addr);
			sts = 2;
		}

		freeifaddrs(ifaddr);
	}
	else
	{
		hostent_sp = gethostbyname(host.c_str());
		if (hostent_sp == nullptr)
		{
			perror("gethostbyname");
			return (-1);
		}
		addr = *reinterpret_cast<struct in_addr *>(hostent_sp->h_addr_list[0]);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)
	}
	return sts;
}

// Return sts, put result in sin
int ResolveHost(char const *host_in, int dflt_port, sockaddr_in &sin)
{
	int port;
	std::string host;
	struct hostent *hostent_sp;
	std::cmatch mm;
	//  Note: the regex expression used by regex_match has an implied ^ and $
	//        at the beginning and end respectively.
	if (regex_match(host_in, mm, std::regex("([^:]+):(\\d+)")))
	{
		host = mm[1].str();
		port = strtoul(mm[2].str().c_str(), nullptr, 0);
	}
	else if (regex_match(host_in, mm, std::regex(":{0,1}(\\d+)")))
	{
		host = std::string("127.0.0.1");
		port = strtoul(mm[1].str().c_str(), nullptr, 0);
	}
	else if (regex_match(host_in, mm, std::regex("([^:]+):{0,1}")))
	{
		host = mm[1].str();
		port = dflt_port;
	}
	else
	{
		host = std::string("127.0.0.1");
		port = dflt_port;
	}
	TLOG(TLVL_INFO) << "Resolving host " << host << ", on port " << port;

	if (host == "localhost")
	{
		host = "127.0.0.1";
	}

	memset(static_cast<void *>(&sin), 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);  // just a guess at an open port

	if (regex_match(host.c_str(), mm, std::regex(R"(\d+(\.\d+){3})")))
	{
		inet_aton(host.c_str(), &sin.sin_addr);
	}
	else
	{
		hostent_sp = gethostbyname(host.c_str());
		if (hostent_sp == nullptr)
		{
			TLOG(TLVL_ERROR) << "Error calling gethostbyname: " << errno << " (" << strerror(errno) << ")";
			perror("gethostbyname");
			return (-1);
		}
		sin.sin_addr = *reinterpret_cast<struct in_addr *>(hostent_sp->h_addr_list[0]);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)
	}

	TLOG(TLVL_INFO) << "Host resolved as " << inet_ntoa(sin.sin_addr);
	return 0;
}
// return connection fd.
//
int TCPConnect(char const *host_in, int dflt_port, int64_t flags, int sndbufsiz)
{
	int s_fd, sts;
	struct sockaddr_in sin;

	s_fd = socket(PF_INET, SOCK_STREAM /*|SOCK_NONBLOCK*/, 0);  // man socket,man TCP(7P)

	if (s_fd == -1)
	{
		perror("socket error");
		return (-1);
	}

	sts = ResolveHost(host_in, dflt_port, sin);
	if (sts == -1)
	{
		close(s_fd);
		return -1;
	}

	sts = connect(s_fd, reinterpret_cast<struct sockaddr *>(&sin), sizeof(sin));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
	if (sts == -1)
	{
		// perror( "connect error" );
		close(s_fd);
		return (-1);
	}

	if (flags != 0)
	{
		sts = fcntl(s_fd, F_SETFL, flags);
		TLOG(TLVL_DEBUG + 33) << "TCPConnect fcntl(fd=" << s_fd << ",flags=0x" << std::hex << flags << std::dec << ") =" << sts;
	}

	if (sndbufsiz > 0)
	{
		int len;
		socklen_t lenlen = sizeof(len);
		len = 0;
		sts = getsockopt(s_fd, SOL_SOCKET, SO_SNDBUF, &len, &lenlen);
		TLOG(TLVL_DEBUG + 32) << "TCPConnect SNDBUF initial: " << len << " sts/errno=" << sts << "/" << errno << " lenlen=" << lenlen;
		len = sndbufsiz;
		sts = setsockopt(s_fd, SOL_SOCKET, SO_SNDBUF, &len, lenlen);
		if (sts == -1)
		{
			TLOG(TLVL_ERROR) << "Error with setsockopt SNDBUF " << errno;
		}
		len = 0;
		sts = getsockopt(s_fd, SOL_SOCKET, SO_SNDBUF, &len, &lenlen);
		if (len < (sndbufsiz * 2))
		{
			TLOG(TLVL_WARNING) << "SNDBUF " << len << " not expected (" << sndbufsiz << " sts/errno=" << sts << "/" << errno << ")";
		}
		else
		{
			TLOG(TLVL_DEBUG + 32) << "SNDBUF " << len << " sts/errno=" << sts << "/" << errno;
		}
	}
	return (s_fd);
}
