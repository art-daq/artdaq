#define TRACE_NAME "NetMonWrapper"

#include "artdaq/ArtModules/NetMonWrapper.hh"

#include <TBufferFile.h>

void art::NetMonWrapper::receiveMessage(std::list<std::unique_ptr<TBufferFile>>& msg_ptr)
{
	TLOG(5) << "Receiving Fragment from NetMonTransportService" ;

	transport_->receiveMessage(msg_ptr);

	TLOG(5) << "Done Receiving Fragment from NetMonTransportService" ;
}

void art::NetMonWrapper::receiveInitMessage(std::list<std::unique_ptr<TBufferFile>>& msg_ptr)
{
	TLOG(5) << "Receiving Init Fragment from NetMonTransportService" ;

	transport_->receiveInitMessage(msg_ptr);

	TLOG(5) << "Done Receiving Init Fragment from NetMonTransportService" ;
}
