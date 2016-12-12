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

#include "trace.h"				// TRACE

#include "artdaq/TransferPlugins/detail/TCPConnect.hh"

using namespace std;

// return connection fd.
// 
int TCPConnect(  char const *host_in
               , int	dflt_port
               , long   flags
               , int    *sndbufsizptr )
{
	int			s_fd, sts;
	int			port;
	std::string             host;
	struct sockaddr_in	sin;
        struct hostent          *hostent_sp;

    cmatch  mm;
    //  Note: the regex expression used by regex_match has an implied ^ and $
    //        at the beginning and end respectively.
    if      (regex_match( host_in, mm, regex("([^:]+):(\\d+)") )) {
		host = mm[1].str();
		port = strtoul( mm[2].str().c_str(), NULL, 0 );
    }
    else if (regex_match( host_in, mm, regex(":{0,1}(\\d+)") )) {
		host = std::string("127.0.0.1");
		port = strtoul( mm[1].str().c_str(), NULL, 0 );
    }
    else if (regex_match( host_in, mm, regex("([^:]+):{0,1}") )) {
		host = mm[1].str().c_str();
        port = dflt_port;
    }
    else {
		host = std::string("127.0.0.1");
		port = dflt_port;
    }

    s_fd = socket( PF_INET, SOCK_STREAM/*|SOCK_NONBLOCK*/, 0 );	// man socket,man TCP(7P)

    if (s_fd == -1) {
		perror( "socket error" );
		return (-1);
    }

    bzero( (char *)&sin, sizeof(sin) );
    sin.sin_family = AF_INET;
    sin.sin_port = htons( port ); // just a guess at an open port

    if (regex_match( host.c_str(),mm,regex("\\d+(\\.\\d+){3}") ))
		inet_aton( host.c_str(), &sin.sin_addr );
    else {
		hostent_sp = gethostbyname( host.c_str() );
		if (!hostent_sp) {
			perror( "gethostbyname" );
			close( s_fd );
			return (-1);
		}
		sin.sin_addr = *(struct in_addr *)(hostent_sp->h_addr_list[0]);
    }

    sts = connect( s_fd, (struct sockaddr *)&sin, sizeof(sin) );
    if (sts == -1) {
		//perror( "connect error" );
        close( s_fd );
		return (-1);
    }

	if (flags) {
		sts = fcntl( s_fd, F_SETFL, flags );
		TRACE( 4, "TCPConnect fcntl(fd=%d,flags=0x%lx)=%d",s_fd,flags,sts );
	}

	if (sndbufsizptr) {
		int len;
		socklen_t lenlen=sizeof(len);
		len = 0;
		sts = getsockopt( s_fd, SOL_SOCKET, SO_SNDBUF, &len, &lenlen );
		TRACE( 3,"TCPConnect SNDBUF initial: %d sts/errno=%d/%d lenlen=%d", len,sts,errno,lenlen );
		len=*sndbufsizptr;
		sts = setsockopt( s_fd, SOL_SOCKET, SO_SNDBUF, &len, lenlen );
		if (sts == -1) TRACE( 0, "Error with setsockopt SNDBUF %d", errno );
		len = 0;
		sts = getsockopt( s_fd, SOL_SOCKET, SO_SNDBUF, &len, &lenlen );
		if (len < (*sndbufsizptr*2))
			TRACE( 1,"SNDBUF %d not expected (%d) sts/errno=%d/%d"
			      ,len,*sndbufsizptr,sts,errno);
		else
			TRACE( 3,"SNDBUF %d sts/errno=%d/%d", len,sts,errno );
		*sndbufsizptr = len;
	}
    return (s_fd);
}
