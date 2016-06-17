//
// sender.cpp
// ~~~~~~~~~~
//
// Copyright (c) 2003-2014 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include "artdaq-core/Data/Fragment.hh"

#include "cetlib/filepath_maker.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"

#include <boost/asio.hpp>
#include "boost/bind.hpp"
#include <boost/lexical_cast.hpp>

#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>

const short multicast_port = 30001;
const bool gVerbose = false;

class sender
{
public:
  sender(boost::asio::io_service& io_service,
	 const boost::asio::ip::address& multicast_address,
	 const size_t num_sends,
	 const size_t fragment_size, const size_t subfragment_size, const size_t subfragments_per_send)
    : endpoint_(multicast_address, multicast_port),
      socket_(io_service, endpoint_.protocol()),
      fragment_size_(fragment_size),
      subfragment_size_(subfragment_size),
      subfragments_per_send_(subfragments_per_send)
  {

    for (size_t i_i = 0; i_i < num_sends; ++i_i) {
      create_and_send_fragment();
    }
  }

  void handle_send_to(const boost::system::error_code& error)
  {
    if (error) {
      std::cerr << "error code received by handle_send_to, " << error << 
	" (" << error.message() << ")" << std::endl;
    }
  }

  void create_and_send_fragment() 
  {
	
    frag_.reset(nullptr);
    frag_ = std::move( artdaq::Fragment::FragmentBytes( fragment_size_ ) );

    std::vector<boost::asio::const_buffer> buffers;
    book_container_of_buffers( buffers );

    if (gVerbose) {
      std::cout << "Created a container of " << buffers.size() << " buffers" << std::endl;
    }

    auto buffer_iter = buffers.begin();

    do {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
	  
      auto subfragments_to_send = buffers.end() - buffer_iter > subfragments_per_send_ ?
	subfragments_per_send_ :
	buffers.end() - buffer_iter ;

#pragma GCC diagnostic pop
	  
      // Want to get rid of this extra memory copy...

      std::vector<boost::asio::const_buffer> subset_of_buffers(buffer_iter, 
							       buffer_iter + subfragments_to_send);
	  
      buffer_iter = buffer_iter + subfragments_to_send;

      if (gVerbose) {
	std::cout << "Calling async_send_to with " << subset_of_buffers.size() << " subfragments" << std::endl;
      }

      socket_.async_send_to(
			    subset_of_buffers,
			    endpoint_,
			    boost::bind(&sender::handle_send_to, this,
					boost::asio::placeholders::error));
    } while (buffer_iter != buffers.end());

  }

  void book_container_of_buffers(std::vector<boost::asio::const_buffer>& buffers) {

    if (buffers.size() != 0 ||
	!frag_) {
      std::cerr << "Unexpected input in " << __FUNCTION__ << std::endl;
    }

    const artdaq::Fragment::byte_t* ptr_to_frag_end( frag_->dataEndBytes() );
    artdaq::Fragment::byte_t* ptr_into_frag( frag_->headerBeginBytes() );

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

    while (ptr_into_frag < ptr_to_frag_end) {

      auto bytes_to_book = ptr_to_frag_end - ptr_into_frag > subfragment_size_ ?
	subfragment_size_ :
	ptr_to_frag_end - ptr_into_frag;
      
      buffers.emplace_back( ptr_into_frag, bytes_to_book );

      ptr_into_frag += bytes_to_book;
    }

#pragma GCC diagnostic pop

  }

private:
  boost::asio::ip::udp::endpoint endpoint_;
  boost::asio::ip::udp::socket socket_;
  std::unique_ptr<artdaq::Fragment> frag_;

  const size_t fragment_size_;
  const size_t subfragment_size_;
  const size_t subfragments_per_send_;
};

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
      if (argc < 2 || argc > 3)
	{
	  std::cerr << "Usage: sender <multicast_address> (number of sends - default is 1)\n";
	  std::cerr << "  Try 224.0.0.1 for the multicast address\n";
	  return 1;
	}

      auto ip_address = boost::asio::ip::address::from_string(argv[1]);
      size_t num_sends = (argc == 3) ? boost::lexical_cast<size_t>( argv[2] ) : 1;

      std::cout << "# of sends == " << num_sends << std::endl;

      fhicl::ParameterSet ps = ReadParameterSet();

      const size_t fragment_size( ps.get<size_t>("fragment_size") );
      const size_t subfragment_size( ps.get<size_t>("subfragment_size") );
      const size_t subfragments_per_send( ps.get<size_t>("subfragments_per_send") );
      
      std::cout << "Fragment size == " << fragment_size << std::endl;
      std::cout << "SubFragment size == " << subfragment_size << std::endl;
      std::cout << "SubFragments sent per async_send_to call == " << subfragments_per_send << std::endl;

      boost::asio::io_service io_service;

      sender s(io_service, ip_address, 
	       num_sends,
	       fragment_size,
	       subfragment_size,
	       subfragments_per_send
	       );

      io_service.run();
    }
  catch (std::exception& e)
    {
      std::cerr << "Exception: " << e.what() << "\n";
    }

  return 0;
}
