
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQrate/SharedMemoryEventReceiver.hh"

#include "artdaq-core/Data/Fragment.hh"
#include <sys/time.h>
#define TRACE_NAME "SharedMemoryEventReceiver"
#include "trace.h"

using std::string;

artdaq::SharedMemoryEventReceiver::SharedMemoryEventReceiver(int shm_key, size_t buffer_count, size_t max_buffer_size)
	: SharedMemoryManager(shm_key, buffer_count, max_buffer_size)
	, current_read_buffer_(-1)
{
}

std::shared_ptr<artdaq::detail::RawEventHeader> artdaq::SharedMemoryEventReceiver::ReadHeader(bool& err)
{
	if (current_read_buffer_ != -1) {
		err = !CheckBuffer(current_read_buffer_, BufferSemaphoreFlags::Reading);
		if (err) return nullptr;
	}
	if (current_header_) return current_header_;
	auto buf = GetBufferForReading();
	if (buf == -1) throw cet::exception("OutOfEvents") << "ReadHeader called but no events are ready! (Did you check ReadyForRead()?)";
	current_read_buffer_ = buf;
	ResetReadPos(current_read_buffer_);
	current_header_ = std::shared_ptr<detail::RawEventHeader>(reinterpret_cast<detail::RawEventHeader*>(GetReadPos(buf)));
	return current_header_;
}

std::set<artdaq::Fragment::type_t> artdaq::SharedMemoryEventReceiver::GetFragmentTypes(bool& err)
{
	if (current_read_buffer_ == -1) throw cet::exception("AccessViolation") << "Cannot call GetFragmentTypes when not currently reading a buffer! Call ReadHeader() first!";

	err = !CheckBuffer(current_read_buffer_, BufferSemaphoreFlags::Reading);
	if (err) return std::set<Fragment::type_t>();

	ResetReadPos(current_read_buffer_);
	IncrementReadPos(current_read_buffer_, sizeof(detail::RawEventHeader));
	auto output = std::set<Fragment::type_t>();

	while (MoreDataInBuffer(current_read_buffer_))
	{
		err = !CheckBuffer(current_read_buffer_, BufferSemaphoreFlags::Reading);
		if (err) return std::set<Fragment::type_t>();
		auto fragHdr = reinterpret_cast<artdaq::detail::RawFragmentHeader*>(GetReadPos(current_read_buffer_));
		output.insert(fragHdr->type);
		IncrementReadPos(current_read_buffer_, fragHdr->word_count * sizeof(RawDataType));
	}

	return output;
}

std::unique_ptr<artdaq::Fragments> artdaq::SharedMemoryEventReceiver::GetFragmentsByType(bool& err, Fragment::type_t type)
{
	if (current_read_buffer_ == -1) throw cet::exception("AccessViolation") << "Cannot call GetFragmentsByType when not currently reading a buffer! Call ReadHeader() first!";
	err = !CheckBuffer(current_read_buffer_, BufferSemaphoreFlags::Reading);
	if (err) return nullptr;

	ResetReadPos(current_read_buffer_);
	IncrementReadPos(current_read_buffer_, sizeof(detail::RawEventHeader));

	Fragments output;

	while (MoreDataInBuffer(current_read_buffer_))
	{
		err = !CheckBuffer(current_read_buffer_, BufferSemaphoreFlags::Reading);
		if (err) return nullptr;
		auto fragHdr = reinterpret_cast<artdaq::detail::RawFragmentHeader*>(GetReadPos(current_read_buffer_));
		if (fragHdr->type == type || type == Fragment::InvalidFragmentType) {
			output.emplace_back(fragHdr->word_count - detail::RawFragmentHeader::num_words());
			Read(current_read_buffer_, output.back().headerAddress(), fragHdr->word_count * sizeof(RawDataType));
		}
		IncrementReadPos(current_read_buffer_, fragHdr->word_count * sizeof(RawDataType));
	}

	return std::unique_ptr<Fragments>(&output);
}

void artdaq::SharedMemoryEventReceiver::ReleaseBuffer()
{
	try {
		MarkBufferEmpty(current_read_buffer_);
	}
	catch (cet::exception e)
	{
		mf::LogWarning("SharedMemoryEventReceiver") << "A cet::exception occured while trying to release the buffer: " << e;
	}
	current_read_buffer_ = -1;
	current_header_.reset();
}
