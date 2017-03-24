#ifndef artdaq_DAQrate_Perf_hh
#define artdaq_DAQrate_Perf_hh

#include <vector>
#include <string>
#include <ostream>
#include <iomanip>
#include <string>

// for handles use - sendFragment and recvFragment
#define PERF_SEND 1
#define PERF_FOUND 2
#define PERF_SENT 3
#define PERF_RECV 4
#define PERF_WOKE 5
#define PERF_POST 6
// for use in eventstore
#define PERF_EVENT 7
#define PERF_EVENT_END 8
#define PERF_JOB_START 9
#define PERF_JOB_END 10
#define PERF_ID_END 11

const char* PerfGetName(int id);

struct Header
{
	Header(): id_()
	        , len_() { }

	Header(unsigned char id, unsigned char len): id_(id)
	                                           , len_(len - sizeof(Header)) { }

	unsigned char id_; // type
	unsigned char len_; // not including the size of this header structure
};

template <typename T>
struct HeaderMeas
{
	HeaderMeas(): head_(T::ID, sizeof(T))
	            , call_(call_count++) { }

	~HeaderMeas() { }

	const char* id() const { return PerfGetName(head_.id_); }
	int len() const { return head_.len_; }

	Header head_;
	int call_;

	static int call_count;
};

template <typename T>
inline std::ostream& operator<<(std::ostream& ost, HeaderMeas<T> const& h)
{
	ost << h.id() << " " << h.call_;
	return ost;
}

template <typename T>
int HeaderMeas<T>::call_count = 0;

struct CommonMeas
{
	CommonMeas();

	~CommonMeas();

	void set(int event, int buf)
	{
		event_ = event;
		buf_ = buf;
	}

	void complete();

	int buf_; // use_me or which
	int event_;
	double enter_;
	double exit_;
};

inline std::ostream& operator<<(std::ostream& ost, CommonMeas const& h)
{
	ost << h.buf_ << " " << h.event_ << " "
		<< std::setprecision(14) << h.enter_ << " " << std::setprecision(14) << h.exit_;
	return ost;
}

struct SendMeas : HeaderMeas<SendMeas>
{
	enum
	{
		ID = PERF_SEND
	};

	SendMeas();

	~SendMeas();

	void print(std::ostream& ost) const
	{
		ost << id() << " " << call_ << " " << com_ << " ";
		ost << dest_ << " " << std::setprecision(14) << found_ << " ";
	}

	void found(int event, int buf, short dest);

	CommonMeas com_;
	short dest_;
	double found_;
};

inline std::ostream& operator<<(std::ostream& ost, SendMeas const& h)
{
	h.print(ost);
	return ost;
}

struct RecvMeas : HeaderMeas<RecvMeas>
{
	enum
	{
		ID = PERF_RECV
	};

	RecvMeas();

	~RecvMeas();

	void print(std::ostream& ost) const
	{
		ost << id() << " " << call_ << " " << com_ << " ";
		ost << from_ << " " << std::setprecision(14) << wake_;
	}

	void woke(int event, int which_buf);

	void post(int from) { from_ = from; }

	CommonMeas com_;
	short from_;
	double wake_;
};

inline std::ostream& operator<<(std::ostream& ost, RecvMeas const& h)
{
	h.print(ost);
	return ost;
}

struct JobStartMeas : HeaderMeas<JobStartMeas>
{
	enum
	{
		ID = PERF_JOB_START
	};

	JobStartMeas();

	~JobStartMeas();

	void print(std::ostream& ost) const
	{
		ost << run_ << " " << rank_ << " ";
		ost << id() << " " << call_ << " "
			<< "Type " << which_ << " "
			<< std::setprecision(14) << when_;
	}

	int run_;
	int rank_;
	int which_;
	double when_;
};

inline std::ostream& operator<<(std::ostream& ost, JobStartMeas const& h)
{
	h.print(ost);
	return ost;
}

struct JobEndMeas : HeaderMeas<JobEndMeas>
{
	enum
	{
		ID = PERF_JOB_END
	};

	void print(std::ostream& ost) const
	{
		ost << id() << " " << call_ << " ";
		ost << std::setprecision(14) << when_;
	}

	JobEndMeas();

	~JobEndMeas();

	double when_;
};

inline std::ostream& operator<<(std::ostream& ost, JobEndMeas const& h)
{
	h.print(ost);
	return ost;
}

struct EventMeas : HeaderMeas<EventMeas>
{
	enum
	{
		ID = PERF_EVENT
	};

	enum Type
	{
		START = 0,
		END = 1
	};

	EventMeas(Type id, int event);

	~EventMeas();

	void print(std::ostream& ost) const
	{
		ost << id() << " " << call_ << " ";
		ost << which_ << " " << event_ << " " << std::setprecision(14) << when_;
	}

	Type which_;
	int event_;
	double when_;
};

inline std::ostream& operator<<(std::ostream& ost, EventMeas const& h)
{
	h.print(ost);
	return ost;
}

void PerfConfigure(int rank,
                   int run,
                   int type,
                   int num_senders = 0,
                   int num_receivers = 0,
                   size_t expected_events = 0);

void PerfSetStartTime();

double PerfGetStartTime();

void PerfWriteJobStart();

void PerfWriteJobEnd();

void PerfWriteEvent(EventMeas::Type, int sequence_id);


#endif /* artdaq_DAQrate_Perf_hh */
