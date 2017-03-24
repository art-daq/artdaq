#ifndef artdaq_DAQrate_detail_FragCounter_hh
#define artdaq_DAQrate_detail_FragCounter_hh

#include <unordered_map>
#include <atomic>
#include <limits>
#include <mutex>

namespace artdaq
{
	namespace detail
	{
		class FragCounter;
	}
}

class artdaq::detail::FragCounter
{
public:
	explicit FragCounter();

	void incSlot(size_t slot);

	void incSlot(size_t slot, size_t inc);

	void setSlot(size_t slot, size_t val);

	size_t nSlots() const;

	size_t count() const;

	size_t slotCount(size_t slot) const;

	size_t minCount() const;

	size_t operator[](size_t slot) const { return slotCount(slot); }

private:
	mutable std::mutex receipts_mutex_;
	std::unordered_map<size_t, std::atomic<size_t>> receipts_;
};

inline
artdaq::detail::FragCounter::
FragCounter()
	: receipts_() {}

inline
void
artdaq::detail::FragCounter::
incSlot(size_t slot)
{
	incSlot(slot, 1);
}

inline
void
artdaq::detail::FragCounter::
incSlot(size_t slot, size_t inc)
{
	std::unique_lock<std::mutex> lk(receipts_mutex_);
	receipts_[slot].fetch_add(inc);
}

inline
void
artdaq::detail::FragCounter::
setSlot(size_t slot, size_t val)
{
	std::unique_lock<std::mutex> lk(receipts_mutex_);
	receipts_[slot] = val;
}

inline
size_t
artdaq::detail::FragCounter::
nSlots() const
{
	std::unique_lock<std::mutex> lk(receipts_mutex_);
	return receipts_.size();
}

inline
size_t
artdaq::detail::FragCounter::
count() const
{
	std::unique_lock<std::mutex> lk(receipts_mutex_);
	size_t acc = 0;
	for (auto& it : receipts_)
	{
		acc += it.second;
	}
	return acc;
}

inline
size_t
artdaq::detail::FragCounter::
slotCount(size_t slot) const
{
	std::unique_lock<std::mutex> lk(receipts_mutex_);
	return receipts_.count(slot) ? receipts_.at(slot).load() : 0;
}

inline
size_t
artdaq::detail::FragCounter::
minCount() const
{
	std::unique_lock<std::mutex> lk(receipts_mutex_);
	size_t min = std::numeric_limits<size_t>::max();
	for (auto& it : receipts_)
	{
		if (it.second < min) min = it.second;
	}
	return min;
}

#endif /* artdaq_DAQrate_detail_FragCounter_hh */
