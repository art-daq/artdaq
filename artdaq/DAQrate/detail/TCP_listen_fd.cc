//  This file (TCP_listen_fd.cpp) was created by Ron Rechenmacher <ron@fnal.gov> on
//  Apr 20, 2010. "TERMS AND CONDITIONS" governing this file are in the README
//  or COPYING file. If you do not have such a file, one can be obtained by
//  contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
//  $RCSfile: TCP_listen_fd.cpp,v $
//  rev="$Revision: 1.3 $$Date: 2010/06/04 14:00:32 $";

#include <stdio.h>		// printf
#include <stdlib.h>		// exit
#include <strings.h>		// bzero
#include <sys/socket.h>         /* inet_aton, socket, bind, listen, accept */
#include <netinet/in.h>         /* inet_aton, struct sockaddr_in */
#include <arpa/inet.h>          /* inet_aton */
#include <netdb.h>              /* gethostbyname */
#include <errno.h>				// errno
#include "trace.h"				// TRACE

int
TCP_listen_fd(  int	port, int rcvbuf )
{
	int			sts;
	int			listener_fd;
	struct sockaddr_in	sin;

    listener_fd = socket( PF_INET, SOCK_STREAM, 0 );   /* man TCP(7P) */
    if (listener_fd == -1)
    {   perror( "socket error" );
        exit (1);
    }

    int opt=1;    // SO_REUSEADDR - man socket(7)
    sts = setsockopt( listener_fd,SOL_SOCKET, SO_REUSEADDR, &opt,sizeof(opt) );
    if (sts == -1)
    {   perror( "setsockopt SO_REUSEADDR" );
	return (1);
    }

    bzero( (char *)&sin, sizeof(sin) );
    sin.sin_family      = AF_INET;
    sin.sin_port        = htons( port );
    sin.sin_addr.s_addr = INADDR_ANY;

    //printf( "bind..." );fflush(stdout);
    sts = bind( listener_fd, (struct sockaddr *)&sin, sizeof(sin) );
    if (sts == -1)
    {   perror( "bind error" );
	exit (1);
    }
    //printf( " OK\n" );

	int          len=0;
	socklen_t    arglen=sizeof(len);
	sts = getsockopt( listener_fd, SOL_SOCKET, SO_RCVBUF, &len, &arglen );
	TRACE( 1,"RCVBUF initial: %d sts/errno=%d/%d arglen=%d rcvbuf=%d listener_fd=%d"
	      ,len,sts,errno,arglen,rcvbuf,listener_fd );
	if (rcvbuf > 0) {
		len = rcvbuf;
		sts = setsockopt( listener_fd, SOL_SOCKET, SO_RCVBUF, &len, arglen );
		if (sts == -1) TRACE( 0, "Error with setsockopt SNDBUF %d", errno );
		len = 0;
		sts = getsockopt( listener_fd, SOL_SOCKET, SO_RCVBUF, &len, &arglen );
		if (len < (rcvbuf*2)) TRACE( 1,"RCVBUF %d not expected (%d) sts/errno=%d/%d"
		                            ,len,rcvbuf,sts,errno);
		else                  TRACE( 3,"RCVBUF %d sts/errno=%d/%d", len,sts,errno );
	}

    //printf( "listen..." );fflush(stdout);
    sts = listen( listener_fd, 5/*QLEN*/ );
    if (sts == -1)
    {   perror( "listen error" );
	exit (1);
    }
    //printf( " OK\n" );

    return (listener_fd);
}   // TCP_listen_fd
