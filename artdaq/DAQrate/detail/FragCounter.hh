#ifndef artdaq_DAQrate_detail_FragCounter_hh
#define artdaq_DAQrate_detail_FragCounter_hh

#include <unordered_map>
#include <atomic>

namespace artdaq {
  namespace detail {
    class FragCounter;
  }
}

class artdaq::detail::FragCounter {
public:
  explicit FragCounter();
  void incSlot(size_t slot);
  void incSlot(size_t slot, size_t inc);
  void setSlot(size_t slot, size_t val);

  size_t nSlots() const;
  size_t count() const;
  size_t slotCount(size_t slot) const;

  size_t operator[](size_t slot) const { return slotCount(slot); }

private:
  std::unordered_map<size_t, std::atomic<size_t>> receipts_;
};

inline
artdaq::detail::FragCounter::
FragCounter()
  : receipts_()
{
}

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
  receipts_[slot].fetch_add(inc);
}

inline
void
artdaq::detail::FragCounter::
setSlot(size_t slot, size_t val)
{
  receipts_[slot] = val;
}

inline
size_t
artdaq::detail::FragCounter::
nSlots() const
{
  return receipts_.size();
}

inline
size_t
artdaq::detail::FragCounter::
count() const
{
  size_t acc = 0;
  for(auto& it : receipts_) {
	acc+=it.second;
  }
  return acc;
}

inline
size_t
artdaq::detail::FragCounter::
slotCount(size_t slot) const
{
  return receipts_.count(slot) ? receipts_.at(slot).load() : 0;
}

#endif /* artdaq_DAQrate_detail_FragCounter_hh */
