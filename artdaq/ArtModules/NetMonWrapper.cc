#include "artdaq/ArtModules/NetMonWrapper.hh"

#include <TBufferFile.h>

void art::NetMonWrapper::receiveMessage(std::unique_ptr<TBufferFile>& msg)
{
	TLOG_ARB(5,"NetMonWrapper") << "Receiving Fragment from NetMonTransportService" << TLOG_ENDL;
	TBufferFile* msg_ptr(nullptr);

	ServiceHandle<NetMonTransportService> transport;
	transport->receiveMessage(msg_ptr);

	msg.reset(msg_ptr);
	TLOG_ARB(5, "NetMonWrapper") << "Done Receiving Fragment from NetMonTransportService" << TLOG_ENDL;
}

void art::NetMonWrapper::receiveInitMessage(std::unique_ptr<TBufferFile>& msg)
{
	TLOG_ARB(5,"NetMonWrapper") << "Receiving Init Fragment from NetMonTransportService" << TLOG_ENDL;
	TBufferFile* msg_ptr(nullptr);

	ServiceHandle<NetMonTransportService> transport;
	transport->receiveInitMessage(msg_ptr);

	msg.reset(msg_ptr);
	TLOG_ARB(5,"NetMonWrapper") << "Done Receiving Init Fragment from NetMonTransportService" << TLOG_ENDL;
}