#include "artdaq/ArtModules/NetMonWrapper.hh"

#include "TBufferFile.h"

void art::NetMonWrapper::receiveMessage(std::unique_ptr<TBufferFile>& msg)
{
	TBufferFile* msg_ptr(nullptr);

	ServiceHandle<NetMonTransportService> transport;
	transport->receiveMessage(msg_ptr);

	msg.reset(msg_ptr);
}
