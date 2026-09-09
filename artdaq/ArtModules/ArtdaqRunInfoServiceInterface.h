#ifndef artdaq_ArtModules_ArtdaqRunInfoServiceInterface_h
#define artdaq_ArtModules_ArtdaqRunInfoServiceInterface_h

#include "art/Framework/Services/Registry/ServiceDeclarationMacros.h"
#include "canvas/Persistency/Provenance/IDNumber.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <string>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

class ArtdaqRunInfoServiceInterface
{
public:
	ArtdaqRunInfoServiceInterface() = default;

	virtual ~ArtdaqRunInfoServiceInterface() = default;

	virtual bool addSubrunRecord(
	    art::RunNumber_t run,
	    art::SubRunNumber_t subrun,
	    size_t nEvents,
	    art::EventNumber_t firstEvent,
	    art::EventNumber_t lastEvent,
	    std::string const& datastream = "default") = 0;

	virtual bool addFileSummary(
	    std::string const& fileName,
	    art::RunNumber_t run,
	    art::SubRunNumber_t firstSubrun,
	    art::SubRunNumber_t lastSubrun,
	    size_t nEvents,
	    size_t fileSize,
	    std::string const& metadata = "{}",
	    std::string const& datastream = "default") = 0;

protected:
	bool appendToCsv(std::string const& path, std::string const& header, std::string const& row)
	{
		int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
		if (fd < 0) { return false; }

		flock(fd, LOCK_EX);

		std::string buf;
		if (lseek(fd, 0, SEEK_END) == 0) { buf = header; }
		buf += row;

		ssize_t total = 0;
		auto remaining = static_cast<ssize_t>(buf.size());
		while (remaining > 0)
		{
			ssize_t written = ::write(fd, buf.c_str() + total, static_cast<size_t>(remaining));
			if (written < 0)
			{
				if (errno == EINTR) { continue; }
				flock(fd, LOCK_UN);
				close(fd);
				return false;
			}
			total += written;
			remaining -= written;
		}

		flock(fd, LOCK_UN);
		close(fd);
		return true;
	}

private:
	ArtdaqRunInfoServiceInterface(ArtdaqRunInfoServiceInterface const&) = delete;
	ArtdaqRunInfoServiceInterface(ArtdaqRunInfoServiceInterface&&) = delete;
	ArtdaqRunInfoServiceInterface& operator=(ArtdaqRunInfoServiceInterface const&) = delete;
	ArtdaqRunInfoServiceInterface& operator=(ArtdaqRunInfoServiceInterface&&) = delete;
};

DECLARE_ART_SERVICE_INTERFACE(ArtdaqRunInfoServiceInterface, LEGACY)

#endif /* artdaq_ArtModules_ArtdaqRunInfoServiceInterface_h */
