

#include "artdaq/TransferPlugins/TransferInterface.hh"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "messagefacility/MessageLogger/MessageLogger.h"
#include "fhiclcpp/ParameterSet.h"

#include <boost/asio.hpp>
#include "boost/bind.hpp"

#include <iostream>
#include <vector>
#include <cassert>
#include <string>
#include <type_traits>
#include <bitset>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"


namespace artdaq {

class MulticastTransfer : public TransferInterface {

public:

  using byte_t = artdaq::Fragment::byte_t;

  ~MulticastTransfer() = default;
  MulticastTransfer(fhicl::ParameterSet const& ps, Role role);

  virtual size_t receiveFragment(artdaq::Fragment& fragment,
				   size_t receiveTimeout);

  virtual CopyStatus copyFragment(artdaq::Fragment& fragment,
				    size_t send_timeout_usec = std::numeric_limits<size_t>::max());
  virtual CopyStatus moveFragment(artdaq::Fragment&& fragment,
	  size_t send_timeout_usec = std::numeric_limits<size_t>::max());

private:

  void fill_staging_memory(const artdaq::Fragment& frag);

  template <typename T>
  void book_container_of_buffers(std::vector<T>& buffers,
				 const size_t fragment_size,
				 const size_t total_subfragments,
				 const size_t first_subfragment_num,
				 const size_t last_subfragment_num);

  void get_fragment_quantities( const boost::asio::mutable_buffer& buf, size_t& payload_size, size_t& fragment_size,
				size_t& expected_subfragments);

  void set_receive_buffer_size(size_t recv_buff_size);

  class subfragment_identifier {

  public:

    subfragment_identifier(size_t sequenceID, size_t fragmentID, size_t subfragment_number ) :
      sequenceID_(sequenceID),
      fragmentID_(fragmentID),
      subfragment_number_(subfragment_number)
    {
    }

    size_t sequenceID() const { return sequenceID_;}
    size_t fragmentID() const { return fragmentID_;}
    size_t subfragment_number() const { return subfragment_number_;}

  private:
    size_t sequenceID_;
    size_t fragmentID_;
    size_t subfragment_number_;
  };

  std::unique_ptr<boost::asio::io_service> io_service_;

  std::unique_ptr<boost::asio::ip::udp::endpoint> local_endpoint_;
  std::unique_ptr<boost::asio::ip::udp::endpoint> multicast_endpoint_;
  std::unique_ptr<boost::asio::ip::udp::endpoint> opposite_endpoint_;

  std::unique_ptr<boost::asio::ip::udp::socket> socket_;

  size_t subfragment_size_;
  size_t subfragments_per_send_;

  size_t max_fragment_size_;
  size_t pause_on_copy_usecs_;

  std::vector<byte_t> staging_memory_;

  std::vector<boost::asio::mutable_buffer> receive_buffers_;
};

}

artdaq::MulticastTransfer::MulticastTransfer(fhicl::ParameterSet const& pset, Role role) :
  TransferInterface(pset, role),
  io_service_(std::make_unique<std::remove_reference<decltype(*io_service_)>::type>()),
  local_endpoint_(nullptr),
  multicast_endpoint_(nullptr),
  opposite_endpoint_(std::make_unique<std::remove_reference<decltype(*opposite_endpoint_)>::type>()),
  socket_(nullptr),
  subfragment_size_(pset.get<size_t>("subfragment_size")),
  subfragments_per_send_(pset.get<size_t>("subfragments_per_send")),
  max_fragment_size_(pset.get<size_t>("max_fragment_size_words") * sizeof(artdaq::RawDataType)),
  pause_on_copy_usecs_(pset.get<size_t>("pause_on_copy_usecs", 0))
{

  try {

    auto port = pset.get<unsigned short>("multicast_port");
    auto multicast_address = boost::asio::ip::address::from_string(pset.get<std::string>("multicast_address"));
    auto local_address = boost::asio::ip::address::from_string(pset.get<std::string>("local_address"));

    mf::LogDebug(uniqueLabel()) << "multicast address is set to " << multicast_address;
    mf::LogDebug(uniqueLabel()) << "local address is set to " << local_address;

    if (TransferInterface::role() == Role::kSend) {

      local_endpoint_ = std::make_unique<std::remove_reference<decltype(*local_endpoint_)>::type>( local_address, 0 );
      multicast_endpoint_ = std::make_unique<std::remove_reference<decltype(*multicast_endpoint_)>::type>( multicast_address, port );

      socket_ = std::make_unique<std::remove_reference<decltype(*socket_)>::type>( *io_service_, 
										   multicast_endpoint_->protocol());
      socket_->bind(*local_endpoint_);

    } else {  // TransferInterface::role() == Role::kReceive

      // Create the socket so that multiple may be bound to the same address.  

      local_endpoint_ = std::make_unique<std::remove_reference<decltype(*local_endpoint_)>::type>( local_address, port );
      socket_ = std::make_unique<std::remove_reference<decltype(*socket_)>::type>( *io_service_,
										   local_endpoint_->protocol());

      boost::system::error_code ec;

      socket_->set_option(boost::asio::ip::udp::socket::reuse_address(true), ec);

      if (ec != 0) {
	std::cerr << "boost::system::error_code with value " << ec << " was found in setting reuse_address option" << std::endl;
      }

      set_receive_buffer_size( pset.get<size_t>("receive_buffer_size") );

      socket_->bind( boost::asio::ip::udp::endpoint( multicast_address, port ) );

      // Join the multicast group.

      socket_->set_option(boost::asio::ip::multicast::join_group(multicast_address), ec);

      if (ec != 0) {
	std::cerr << "boost::system::error_code with value " << ec << " was found in attempt to join multicast group" << std::endl;
      }
    }

  } catch (...) {
    ExceptionHandler(ExceptionHandlerRethrow::yes, "Problem setting up the socket in MulticastTransfer");
  }

  auto max_subfragments = 
    static_cast<size_t>(std::ceil(max_fragment_size_ / static_cast<float>(subfragment_size_) ) );
  
  staging_memory_.resize(max_subfragments * (sizeof(subfragment_identifier) + subfragment_size_));

  if (TransferInterface::role() == Role::kReceive) {
    book_container_of_buffers(receive_buffers_, max_fragment_size_, max_subfragments, 0, max_subfragments - 1);
  }

  mf::LogDebug(uniqueLabel()) << "max_subfragments is " << max_subfragments;
  mf::LogDebug(uniqueLabel()) << "Staging buffer size is " << staging_memory_.size();
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"

size_t artdaq::MulticastTransfer::receiveFragment(artdaq::Fragment& fragment,
						      size_t receiveTimeout) {

  assert(TransferInterface::role() == Role::kReceive);

  if (fragment.dataSizeBytes() > 0) {
    throw cet::exception("MulticastTransfer") << "Error in MulticastTransfer::receiveFragmentFrom: " <<
      "nonzero payload found in fragment passed as argument";
  }

  static bool print_warning = true;
  
  if (print_warning) {
    std::cerr << "Please note that MulticastTransfer::receiveFragmentFrom does not use its receiveTimeout argument" << std::endl;
    print_warning = false;
  }

  fragment.resizeBytes( max_fragment_size_ - sizeof(artdaq::detail::RawFragmentHeader) );

  static auto current_sequenceID = std::numeric_limits<Fragment::sequence_id_t>::max();
  static auto current_fragmentID = std::numeric_limits<Fragment::fragment_id_t>::max();

  size_t fragment_size = 0;
  size_t expected_subfragments = 0;
  size_t current_subfragments = 0;
  bool fragment_complete = false;
  bool last_fragment_truncated = false;

  while (true) {

    auto bytes_received = socket_->receive_from(receive_buffers_, *opposite_endpoint_);

    size_t bytes_processed = 0;
    
    for (auto& buf : receive_buffers_) {

      auto buf_size = boost::asio::buffer_size(buf);
      auto size_t_ptr = boost::asio::buffer_cast<const size_t*>(buf);
      auto seqID = *size_t_ptr;
      auto fragID = *(size_t_ptr + 1);
      auto subfragID = *(size_t_ptr + 2);

      if ( seqID != current_sequenceID || fragID != current_fragmentID ) {

	// JCF, Jun-22-2016
	// Code currently operates under the assumption that all subfragments from the call are from the same fragment

	assert(bytes_processed == 0); 

	if (current_subfragments < expected_subfragments) {

	  last_fragment_truncated = true;

	  if (expected_subfragments != std::numeric_limits<size_t>::max()) {
	    std::cerr << "Warning: only received " << current_subfragments << " subfragments for fragment with seqID = " <<
	      current_sequenceID << ", fragID = " << current_fragmentID << " (expected " << expected_subfragments << ")\n" 
		      << std::endl;
	  } else {
	    std::cerr << "Warning: only received " << current_subfragments << 
	      " subfragments for fragment with seqID = " <<
	      current_sequenceID << ", fragID = " << current_fragmentID << 
	      ", # of expected subfragments is unknown as fragment header was not received)\n" 
		      << std::endl;
	  }
      	}

	current_subfragments = 0;
	fragment_size = std::numeric_limits<size_t>::max();
	expected_subfragments = std::numeric_limits<size_t>::max();
	current_sequenceID = seqID;
	current_fragmentID = fragID;
      }

      auto ptr_into_fragment = fragment.headerBeginBytes() + subfragID * subfragment_size_;

      auto ptr_into_buffer = boost::asio::buffer_cast<const byte_t*>(buf) + sizeof(subfragment_identifier);

      std::copy(ptr_into_buffer, ptr_into_buffer + buf_size - sizeof(subfragment_identifier), ptr_into_fragment);

      if (subfragID == 0) {  

	if (buf_size >= sizeof(subfragment_identifier) + sizeof(artdaq::detail::RawFragmentHeader)) {

	  auto payload_size = std::numeric_limits<size_t>::max();
	  get_fragment_quantities(buf, payload_size, fragment_size, expected_subfragments);

	  fragment.resizeBytes( payload_size );

	} else {
	  throw cet::exception("MulticastTransfer") << "Buffer size is too small to completely contain an artdaq::Fragment header; " << 
	    "please increase the default size";
	} 
      }

      current_subfragments++;      

      if (current_subfragments == expected_subfragments) {	

	fragment_complete = true;
      }

      bytes_processed += buf_size;      

      if (bytes_processed >= bytes_received) {
	break;
      }
    }
    
    if (last_fragment_truncated) {

      // JCF, 7-7-2017

      // Don't yet have code to handle the scenario where the set of
      // subfragments received in the last iteration of the loop was
      // its own complete fragment, but we know the previous fragment
      // to be incomplete

      assert( !fragment_complete );
      mf::LogWarning(uniqueLabel()) << "Got an incomplete fragment";
      return artdaq::TransferInterface::RECV_TIMEOUT;
    }

    if (fragment_complete) {
      return source_rank();
    }
  }
  
  return TransferInterface::RECV_TIMEOUT;
}

#pragma GCC diagnostic pop

// Reliable transport is undefined for multicast; just use copy
artdaq::TransferInterface::CopyStatus
artdaq::MulticastTransfer::moveFragment(artdaq::Fragment&& f, size_t tmo) {
	return copyFragment(f, tmo);
}

artdaq::TransferInterface::CopyStatus
artdaq::MulticastTransfer::copyFragment(artdaq::Fragment& fragment,
					  size_t send_timeout_usec) {

  assert(TransferInterface::role() == Role::kSend);

  if ( fragment.sizeBytes() > max_fragment_size_) {
    throw cet::exception("MulticastTransfer") << "Error in MulticastTransfer::copyFragmentTo: " <<
      fragment.sizeBytes() << " byte fragment exceeds max_fragment_size of " << max_fragment_size_;
  }

  static size_t ncalls = 1;
  auto num_subfragments = static_cast<size_t>(std::ceil( fragment.sizeBytes() / static_cast<float>(subfragment_size_ )));

  ncalls++;

  fill_staging_memory(fragment);

  for (size_t batch_index = 0; ; batch_index++ ) {

    auto first_subfragment = batch_index * subfragments_per_send_;
    auto last_subfragment = (batch_index + 1) * subfragments_per_send_ >= num_subfragments ?
      num_subfragments - 1 :
      (batch_index + 1) * subfragments_per_send_ - 1;

    std::vector<boost::asio::const_buffer> buffers;

    book_container_of_buffers(buffers, fragment.sizeBytes(), num_subfragments, first_subfragment, last_subfragment);

    socket_->send_to(buffers, *multicast_endpoint_);

    usleep(pause_on_copy_usecs_);

    if (last_subfragment == num_subfragments - 1) {
      break;
    }
  }
  return CopyStatus::kSuccess;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

void artdaq::MulticastTransfer::fill_staging_memory(const artdaq::Fragment& fragment) {

  auto num_subfragments = static_cast<size_t>(std::ceil( fragment.sizeBytes() / static_cast<float>(subfragment_size_ )));
  mf::LogDebug(uniqueLabel()) << "# of subfragments to use is " << num_subfragments;

  for (auto i_s = 0; i_s < num_subfragments; ++i_s) {

    auto staging_memory_copyto = &staging_memory_.at( i_s * (sizeof(subfragment_identifier) + subfragment_size_));

    subfragment_identifier sfi(fragment.sequenceID(), fragment.fragmentID(), i_s);

    std::copy(reinterpret_cast<byte_t*>(&sfi),
	      reinterpret_cast<byte_t*>(&sfi) + sizeof(subfragment_identifier),
	      staging_memory_copyto );

    auto low_ptr_into_fragment = fragment.headerBeginBytes() + subfragment_size_ * i_s;

    auto high_ptr_into_fragment = (i_s == num_subfragments - 1) ?
      fragment.dataEndBytes() :
      fragment.headerBeginBytes() + subfragment_size_ * (i_s + 1);

    std::copy(low_ptr_into_fragment, 
	      high_ptr_into_fragment,
	      staging_memory_copyto + sizeof(subfragment_identifier));
  }
}

#pragma GCC diagnostic pop

// Note that book_container_of_buffers includes, rather than excludes,
// "last_subfragment_num"; in this regard it's different than the way
// STL functions receive iterators. Note also that the lowest possible
// value for "first_subfragment_num" is 0, not 1.

template <typename T>
void artdaq::MulticastTransfer::book_container_of_buffers(std::vector<T>& buffers,
							  const size_t fragment_size,
							  const size_t total_subfragments,
							  const size_t first_subfragment_num,
							  const size_t last_subfragment_num) {

  assert(staging_memory_.size() >= total_subfragments * (sizeof(subfragment_identifier) + subfragment_size_) );
  assert(buffers.size() == 0); 
  assert(last_subfragment_num < total_subfragments);

  for (auto i_f = first_subfragment_num; i_f <= last_subfragment_num; ++i_f) {

    auto bytes_to_store = (i_f == total_subfragments - 1) ?
      sizeof(subfragment_identifier) + (fragment_size - (total_subfragments - 1) * subfragment_size_) :
      sizeof(subfragment_identifier) + subfragment_size_;
      
    buffers.emplace_back( &staging_memory_.at( i_f * (sizeof(subfragment_identifier) + subfragment_size_) ), 
			  bytes_to_store);
  }
}


#pragma GCC diagnostic push  // Needed since profile builds will ignore the assert
#pragma GCC diagnostic ignored "-Wunused-variable"

void artdaq::MulticastTransfer::get_fragment_quantities( const boost::asio::mutable_buffer& buf, size_t& payload_size,
							 size_t& fragment_size,
							 size_t& expected_subfragments) {

  byte_t* buffer_ptr = boost::asio::buffer_cast<byte_t*>(buf);

  auto subfragment_num = *( reinterpret_cast<size_t*>(buffer_ptr) + 2 );

  assert( subfragment_num == 0 );

  artdaq::detail::RawFragmentHeader* header = 
    reinterpret_cast<artdaq::detail::RawFragmentHeader*>( buffer_ptr + sizeof(subfragment_identifier));

  fragment_size = header->word_count * sizeof(artdaq::RawDataType);

  auto metadata_size = header->metadata_word_count * sizeof(artdaq::RawDataType);
  payload_size = fragment_size - metadata_size - artdaq::detail::RawFragmentHeader::num_words() * 
    sizeof(artdaq::RawDataType);
  
  assert(fragment_size == 
	 artdaq::detail::RawFragmentHeader::num_words() * sizeof(artdaq::RawDataType) +
	 metadata_size +
	 payload_size);

  expected_subfragments = static_cast<size_t>(std::ceil( fragment_size / static_cast<float>(subfragment_size_ )));
}
#pragma GCC diagnostic pop

void artdaq::MulticastTransfer::set_receive_buffer_size(size_t recv_buff_size) {

  boost::asio::socket_base::receive_buffer_size actual_recv_buff_size;
  socket_->get_option(actual_recv_buff_size);

  mf::LogDebug(uniqueLabel()) << "Receive buffer size is currently " << actual_recv_buff_size.value() << 
    " bytes, will try to change it to " << recv_buff_size;

  boost::asio::socket_base::receive_buffer_size recv_buff_option(recv_buff_size);

  boost::system::error_code ec;
  socket_->set_option(recv_buff_option, ec);

  if (ec != 0) {
    std::cerr << "boost::system::error_code with value " << ec << 
      " was found in attempt to change receive buffer" << std::endl;
  }

  socket_->get_option(actual_recv_buff_size);
  mf::LogDebug(uniqueLabel()) << "After attempted change, receive buffer size is now " << actual_recv_buff_size.value();
}


#pragma GCC diagnostic pop

DEFINE_ARTDAQ_TRANSFER(artdaq::MulticastTransfer)
