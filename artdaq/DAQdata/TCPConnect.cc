//  This file (TCPConnect.cc) was created by Ron Rechenmacher <ron@fnal.gov> on
//  Apr 26, 2010. "TERMS AND CONDITIONS" governing this file are in the README
//  or COPYING file. If you do not have such a file, one can be obtained by
//  contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
//  $RCSfile: TCPConnect.cpp,v $
//  rev="$Revision: 1.4 $$Date: 2010/06/24 03:49:45 $";

#include <stdio.h>		// printf
#include <sys/types.h>		// socket, bind, listen, accept
#include <sys/socket.h>		// socket, bind, listen, accept
#include <netinet/in.h>		// struct sockaddr_in
#include <stdlib.h>		// exit
#include <unistd.h>		// close
#include <string.h>		// bzero
#include <sys/socket.h>		// inet_aton
#include <netinet/in.h>		// inet_aton
#include <arpa/inet.h>		// inet_aton
#include <netdb.h>              // gethostbyname

#include <string>
#include <regex>

#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQdata/TCPConnect.hh"

// Return sts, put result in addr
int ResolveHost(char const* host_in, in_addr& addr)
{
	std::string host;
	struct hostent* hostent_sp;
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
		host = mm[1].str().c_str();
	}
	else
	{
		host = std::string("127.0.0.1");
	}
	TLOG_INFO("TCPConnect") << "Resolving host " << host << TLOG_ENDL;

	bzero((char *)&addr, sizeof(addr));

	if (regex_match(host.c_str(), mm, std::regex("\\d+(\\.\\d+){3}")))
		inet_aton(host.c_str(), &addr);
	else
	{
		hostent_sp = gethostbyname(host.c_str());
		if (!hostent_sp)
		{
			perror("gethostbyname");
			return (-1);
		}
		addr = *(struct in_addr *)(hostent_sp->h_addr_list[0]);
	}
	return 0;
}

// Return sts, put result in sin
int ResolveHost(char const* host_in, int dflt_port, sockaddr_in& sin)
{
	int port;
	std::string host;
	struct hostent* hostent_sp;
	std::cmatch mm;
	//  Note: the regex expression used by regex_match has an implied ^ and $
	//        at the beginning and end respectively.
	if (regex_match(host_in, mm, std::regex("([^:]+):(\\d+)")))
	{
		host = mm[1].str();
		port = strtoul(mm[2].str().c_str(), NULL, 0);
	}
	else if (regex_match(host_in, mm, std::regex(":{0,1}(\\d+)")))
	{
		host = std::string("127.0.0.1");
		port = strtoul(mm[1].str().c_str(), NULL, 0);
	}
	else if (regex_match(host_in, mm, std::regex("([^:]+):{0,1}")))
	{
		host = mm[1].str().c_str();
		port = dflt_port;
	}
	else
	{
		host = std::string("127.0.0.1");
		port = dflt_port;
	}
	TLOG_INFO("TCPConnect") << "Resolving host " << host << ", on port " << std::to_string(port) << TLOG_ENDL;

	if (host == "localhost") host = "127.0.0.1";

	bzero((char *)&sin, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port); // just a guess at an open port

	if (regex_match(host.c_str(), mm, std::regex("\\d+(\\.\\d+){3}")))
		inet_aton(host.c_str(), &sin.sin_addr);
	else
	{
		hostent_sp = gethostbyname(host.c_str());
		if (!hostent_sp)
		{
			perror("gethostbyname");
			return (-1);
		}
		sin.sin_addr = *(struct in_addr *)(hostent_sp->h_addr_list[0]);
	}
	return 0;
}
// return connection fd.
// 
int TCPConnect(char const* host_in
			   , int dflt_port
			   , long flags
			   , int sndbufsiz)
{
	int s_fd, sts;
	struct sockaddr_in sin;


	s_fd = socket(PF_INET, SOCK_STREAM/*|SOCK_NONBLOCK*/, 0); // man socket,man TCP(7P)

	if (s_fd == -1)
	{
		perror("socket error");
		return (-1);
	}

	sts = ResolveHost(host_in, dflt_port, sin);
	if(sts == -1)
	{
		close(s_fd);
		return -1;
	}

	sts = connect(s_fd, (struct sockaddr *)&sin, sizeof(sin));
	if (sts == -1)
	{
		//perror( "connect error" );
		close(s_fd);
		return (-1);
	}

	if (flags)
	{
		sts = fcntl(s_fd, F_SETFL, flags);
		TRACE( 4, "TCPConnect fcntl(fd=%d,flags=0x%lx)=%d",s_fd,flags,sts );
	}

	if (sndbufsiz > 0)
	{
		int len;
		socklen_t lenlen = sizeof(len);
		len = 0;
		sts = getsockopt(s_fd, SOL_SOCKET, SO_SNDBUF, &len, &lenlen);
		TRACE(3, "TCPConnect SNDBUF initial: %d sts/errno=%d/%d lenlen=%d", len, sts, errno, lenlen);
		len = sndbufsiz;
		sts = setsockopt(s_fd, SOL_SOCKET, SO_SNDBUF, &len, lenlen);
		if (sts == -1)
		TRACE(0, "Error with setsockopt SNDBUF %d", errno);
		len = 0;
		sts = getsockopt(s_fd, SOL_SOCKET, SO_SNDBUF, &len, &lenlen);
		if (len < (sndbufsiz * 2))
		TRACE(1, "SNDBUF %d not expected (%d) sts/errno=%d/%d"
			, len, sndbufsiz, sts, errno);
		else
		TRACE(3, "SNDBUF %d sts/errno=%d/%d", len, sts, errno);
	}
	return (s_fd);
}