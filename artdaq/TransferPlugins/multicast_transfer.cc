

#include "artdaq/TransferPlugins/TransferInterface.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "fhiclcpp/ParameterSet.h"

#include <boost/asio.hpp>
#include "boost/bind.hpp"

#include <iostream>
#include <vector>
#include <cassert>
#include <string>
#include <type_traits>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"


namespace artdaq {

class multicastTransfer : public TransferInterface {

public:

  using byte_t = artdaq::Fragment::byte_t;

  ~multicastTransfer() = default;
  multicastTransfer(fhicl::ParameterSet const& ps, Role role);

  virtual size_t receiveFragmentFrom(artdaq::Fragment& fragment,
				   size_t receiveTimeout);

  virtual void copyFragmentTo(bool& fragmentHasBeenCopied,
			      bool& esrHasBeenCopied,
			      bool& eodHasBeenCopied,
			      artdaq::Fragment& fragment,
			      size_t send_timeout_usec = std::numeric_limits<size_t>::max());

private:

  void fill_staging_memory(const artdaq::Fragment& frag);

  template <typename T>
  void book_container_of_buffers(std::vector<T>& buffers,
				 const size_t fragment_size,
				 const size_t total_subfragments,
				 const size_t first_subfragment_num,
				 const size_t last_subfragment_num);

  void async_send_handler(const boost::system::error_code& error);

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
  std::unique_ptr<boost::asio::ip::udp::endpoint> endpoint_;
  std::unique_ptr<boost::asio::ip::udp::endpoint> opposite_endpoint_;
  std::unique_ptr<boost::asio::ip::udp::socket> socket_;

  size_t subfragment_size_;
  size_t subfragments_per_send_;

  size_t max_fragment_size_;
  std::vector<byte_t> staging_memory_;

  std::vector<boost::asio::mutable_buffer> receive_buffers_;
};

}

artdaq::multicastTransfer::multicastTransfer(fhicl::ParameterSet const& pset, Role role) :
  TransferInterface(pset, role),
  io_service_(std::make_unique<std::remove_reference<decltype(*io_service_)>::type>()),
  endpoint_(nullptr),
  opposite_endpoint_(std::make_unique<std::remove_reference<decltype(*opposite_endpoint_)>::type>()),
  socket_(nullptr),
  subfragment_size_(pset.get<size_t>("subfragment_size")),
  subfragments_per_send_(pset.get<size_t>("subfragments_per_send")),
  max_fragment_size_(pset.get<size_t>("max_fragment_size_words") * sizeof(artdaq::RawDataType))
{
  


    std::unique_ptr<int> myptr( nullptr );

    myptr = std::make_unique<std::remove_reference<decltype(*myptr)>::type>(18);
    




  try {

    auto port = pset.get<unsigned short>("multicast_port");
    auto multicast_address = boost::asio::ip::address::from_string(pset.get<std::string>("multicast_address"));

    if (TransferInterface::role() == Role::send) {

      endpoint_ = std::make_unique<std::remove_reference<decltype(*endpoint_)>::type>( multicast_address, port );
      socket_ = std::make_unique<std::remove_reference<decltype(*socket_)>::type>( *io_service_, endpoint_->protocol());

    } else {

      // Create the socket so that multiple may be bound to the same address.  

      auto listen_address = boost::asio::ip::address::from_string("0.0.0.0");
      
      endpoint_ = std::make_unique<std::remove_reference<decltype(*endpoint_)>::type>( listen_address, port );
      socket_ = std::make_unique<std::remove_reference<decltype(*socket_)>::type>( *io_service_ );

      socket_->open( endpoint_->protocol() );
      socket_->set_option(boost::asio::ip::udp::socket::reuse_address(true));
      socket_->bind(*endpoint_);

      // Join the multicast group.

      socket_->set_option(boost::asio::ip::multicast::join_group(multicast_address));
    }

  } catch (...) {
    ExceptionHandler(ExceptionHandlerRethrow::yes, "Problem setting up the socket in multicastTransfer");
  }

  auto max_subfragments = 
    static_cast<size_t>(std::ceil(max_fragment_size_ / static_cast<float>(subfragment_size_) ) );
  
  staging_memory_.resize(max_subfragments * (sizeof(subfragment_identifier) + subfragment_size_));

  if (TransferInterface::role() == Role::receive) {
    book_container_of_buffers(receive_buffers_, max_fragment_size_, max_subfragments, 0, max_subfragments - 1);
  }

  std::cout << "max_subfragments is " << max_subfragments << std::endl;
  std::cout << "Staging buffer size is " << staging_memory_.size() << std::endl;
  std::cout << "receive_buffers_ size is " << receive_buffers_.size() << std::endl;
}


size_t artdaq::multicastTransfer::receiveFragmentFrom(artdaq::Fragment& fragment,
						      size_t receiveTimeout) {

  static size_t total_bytes_received = 0;

  assert(TransferInterface::role() == Role::receive);

  if (fragment.dataSizeBytes() > 0) {
    throw cet::exception("multicastTransfer") << "Error in multicastTransfer::receiveFragmentFrom: " <<
      "nonzero payload found in fragment passed as argument";
  }
  
  while (true) {
    auto bytes_received = socket_->receive_from(receive_buffers_, *opposite_endpoint_);
    total_bytes_received += bytes_received;
    std::cout << "Received " << bytes_received << " bytes; " << total_bytes_received << " bytes, total" << std::endl;
  }
  
  return 0;
}

void artdaq::multicastTransfer::copyFragmentTo(bool& fragmentWasCopied,
					       bool& esrWasCopied,
					       bool& eodWasCopied,
					       artdaq::Fragment& fragment,
					       size_t send_timeout_usec) {

  assert(TransferInterface::role() == Role::send);

  if ( fragment.sizeBytes() > max_fragment_size_) {
    throw cet::exception("multicastTransfer") << "Error in multicastTransfer::copyFragmentTo: " <<
      fragment.sizeBytes() << " byte fragment exceeds max_fragment_size of " << max_fragment_size_;
  }

  static size_t ncalls = 1;
  auto num_subfragments = static_cast<size_t>(std::ceil( fragment.sizeBytes() / static_cast<float>(subfragment_size_ )));

  std::cout << "Call #" << ncalls << ", fragment size is " << fragment.sizeBytes() << std::endl;
  ncalls++;

  fill_staging_memory(fragment);

  for (size_t batch_index = 0; ; batch_index++ ) {

    auto first_subfragment = batch_index * subfragments_per_send_;
    auto last_subfragment = (batch_index + 1) * subfragments_per_send_ >= num_subfragments ?
      num_subfragments - 1 :
      (batch_index + 1) * subfragments_per_send_ - 1;

    std::vector<boost::asio::const_buffer> buffers;

    book_container_of_buffers(buffers, fragment.sizeBytes(), num_subfragments, first_subfragment, last_subfragment);

    std::cout << "batch_index == " << batch_index << ", first_subfragment == " << first_subfragment <<
      ", last_subfragment == " << last_subfragment << std::endl;

    // JCF, Jun-19-2016

    // At least currently, it seems like all the bytes get through to
    // the multicast_receive process only if we're not using the
    // asynchronous version of the socket send function. Perhaps a
    // pause between asynchronous calls would do the equivalent

    //    socket_->async_send_to(
    //    			  buffers,
    //			  *endpoint_,
    //			  boost::bind(&artdaq::multicastTransfer::async_send_handler, this,
    //				      boost::asio::placeholders::error));

    socket_->send_to(buffers, *endpoint_);

    if (last_subfragment == num_subfragments - 1) {
      break;
    }
  }
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

void artdaq::multicastTransfer::fill_staging_memory(const artdaq::Fragment& fragment) {

  auto num_subfragments = static_cast<size_t>(std::ceil( fragment.sizeBytes() / static_cast<float>(subfragment_size_ )));
  std::cout << "# of subfragments to use is " << num_subfragments << std::endl;

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

    //    std::cout << "About to copy " << (ptr_into_fragment_end - ptr_into_fragment_begin) << 
    //      " bytes" << std::endl;

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
void artdaq::multicastTransfer::book_container_of_buffers(std::vector<T>& buffers,
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

void artdaq::multicastTransfer::async_send_handler(const boost::system::error_code& error) {

  if (error) {
    std::cerr << "error code received by handle_send_to, " << error << 
      " (" << error.message() << ")" << std::endl;
  }
}


#pragma GCC diagnostic pop

DEFINE_ARTDAQ_TRANSFER(artdaq::multicastTransfer)
