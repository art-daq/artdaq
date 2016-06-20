//
// receiver.cpp
// ~~~~~~~~~~~~
//
// Copyright (c) 2003-2014 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include "artdaq-core/Data/Fragment.hh"

#include "cetlib/exception.h"
#include "cetlib/filepath_maker.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"

#include <boost/asio.hpp>
#include "boost/bind.hpp"

#include <iostream>
#include <string>

const short multicast_port = 30001;
const bool gVerbose = true;

class receiver
{
public:
  receiver(boost::asio::io_service& io_service,
	   const boost::asio::ip::address& listen_address,
	   const boost::asio::ip::address& multicast_address,
	   const size_t fragment_size, const size_t subfragment_size
	   )
    : socket_(io_service),
      total_bytes_received_(0),
      fragment_size_(fragment_size),
      subfragment_size_(subfragment_size)
  {

    std::cout << "fragment_size is " << fragment_size_ << std::endl;
    std::cout << "subfragment_size is " << subfragment_size_ << std::endl;

    if (fragment_size % subfragment_size != 0) {
      throw cet::exception("receiver") << "Requested subfragment size of " << subfragment_size <<
	" does not divide evenly into requested fragment size of " << fragment_size;
    }

    memory_.resize( fragment_size_ );

    auto num_subfragments = fragment_size_ / subfragment_size_;

    for (size_t i_b = 0; i_b < num_subfragments; ++i_b) {
      buffers_.emplace_back( &memory_.at(i_b * subfragment_size_), subfragment_size_ );
    }

    
    // Create the socket so that multiple may be bound to the same address.
    boost::asio::ip::udp::endpoint listen_endpoint(
						   listen_address, multicast_port);
    socket_.open(listen_endpoint.protocol());
    socket_.set_option(boost::asio::ip::udp::socket::reuse_address(true));
    socket_.bind(listen_endpoint);

    // Join the multicast group.
    socket_.set_option(
		       boost::asio::ip::multicast::join_group(multicast_address));

    auto success = boost::system::error_code();
    handle_receive_from(success, 0);
  }

  void handle_receive_from(const boost::system::error_code& error,
			   size_t bytes_recvd)
  {
    if (!error)
      {

	total_bytes_received_ += bytes_recvd;

	if (gVerbose) {
	  std::cout << "Received " << bytes_recvd << " bytes" << std::endl;
	  std::cout << "Total bytes received == " << total_bytes_received_ << std::endl;
	}

	if (total_bytes_received_ == fragment_size_ + sizeof(artdaq::detail::RawFragmentHeader)) {

	  static size_t num_fragments_received = 1;

	  std::cout << "Received full fragment, #" << num_fragments_received << std::endl;

	  total_bytes_received_ = 0;
	  num_fragments_received++;
	}

	socket_.async_receive_from(
				   buffers_,
				   sender_endpoint_,
				   boost::bind(&receiver::handle_receive_from, this,
					       boost::asio::placeholders::error,
					       boost::asio::placeholders::bytes_transferred));
      } else {
      std::cerr << "error code received by handle_receive_from, " << error <<
        " (" << error.message() << ")" << std::endl;
    }
  }

private:
  boost::asio::ip::udp::socket socket_;
  boost::asio::ip::udp::endpoint sender_endpoint_;

  size_t total_bytes_received_;

  const size_t fragment_size_;
  const size_t subfragment_size_;

  std::vector<boost::asio::mutable_buffer> buffers_;
  std::vector<uint8_t> memory_;
};


// DUPLICATED CODE: also found in sender.cpp. Not as egregious as
// normal in that this function is unlikely to be changed, and this is
// a standalone app (not part of artdaq)

fhicl::ParameterSet ReadParameterSet() {
 
  if (std::getenv("FHICL_FILE_PATH") == nullptr) {
    std::cerr
      << "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"\n";
    setenv("FHICL_FILE_PATH", ".", 0);
  }

  fhicl::ParameterSet pset;
  cet::filepath_lookup_after1 lookup_policy("FHICL_FILE_PATH");
  fhicl::make_ParameterSet("multicast.fcl", lookup_policy, pset);

  return pset.get<fhicl::ParameterSet>("multicast");
}

int main(int argc, char* argv[])
{
  try
    {
      if (argc != 3)
	{
	  std::cerr << "Usage: receiver <listen_address> <multicast_address>\n";
	  std::cerr << "  Try:\n";
	  std::cerr << "    receiver 0.0.0.0 <multicast address used by sender>\n";
	  return 1;
	}

      fhicl::ParameterSet ps = ReadParameterSet();

      const size_t fragment_size( ps.get<size_t>("fragment_size") );
      const size_t subfragment_size( ps.get<size_t>("subfragment_size") );

      boost::asio::io_service io_service;
      receiver r(io_service,
		 boost::asio::ip::address::from_string(argv[1]),
		 boost::asio::ip::address::from_string(argv[2]),
		 fragment_size,
		 subfragment_size);

      io_service.run();
    }
  catch (std::exception& e)
    {
      std::cerr << "Exception: " << e.what() << "\n";
    }

  return 0;
}
