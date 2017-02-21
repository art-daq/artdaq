#ifndef TCPConnect_hh
#define TCPConnect_hh
 // This file (TCPConnect.hh) was created by Ron Rechenmacher <ron@fnal.gov> on
 // Sep 15, 2016. "TERMS AND CONDITIONS" governing this file are in the README
 // or COPYING file. If you do not have such a file, one can be obtained by
 // contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
 // $RCSfile: .emacs.gnu,v $
 // rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

int TCPConnect( char const *host_in, int dflt_port, long flags=0, int sndbufsiz=0 );

#endif	// TCPConnect_hh
