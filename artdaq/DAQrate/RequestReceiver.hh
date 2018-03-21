#ifndef ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH
#define ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH

#include <boost/thread.hpp>
#include "artdaq-core/Data/Fragment.hh"
#include "fhiclcpp/fwd.h"

#include <mutex>
#include <condition_variable>

namespace artdaq
{
	class RequestReceiver
	{
	public:
		RequestReceiver();
		RequestReceiver(const fhicl::ParameterSet& ps);
		virtual ~RequestReceiver();

		/**
		* \brief Opens the socket used to listen for data requests
		*/
		void setupRequestListener();

		/**
		* \brief Function that launches the data request receiver thread (receiveRequestsLoop())
		*/
		void startRequestReceiverThread();

		/**
		* \brief This function receives data request packets, adding new requests to the request list
		*/
		void receiveRequestsLoop();

		std::map<artdaq::Fragment::sequence_id_t, artdaq::Fragment::timestamp_t> GetRequests() const 
		{
			std::unique_lock<std::mutex> lk(request_mutex_);
			std::map<artdaq::Fragment::sequence_id_t, Fragment::timestamp_t> out;
			for (auto& in : requests_) {
				out[in.first] = in.second;
			}
			return out; 
		}

		void RemoveRequest(artdaq::Fragment::sequence_id_t reqID);

		bool isRunning() { return running_; }

		void ClearRequests() { 
			std::unique_lock<std::mutex> lk(request_mutex_);
			requests_.clear(); 
		}

		size_t size() { return requests_.size(); }

		bool WaitForRequests(int timeout_ms) {
			std::unique_lock<std::mutex> lk(request_mutex_);
			return request_cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [this]() {return requests_.size() > 0; });
		}
	private:
		// FHiCL-configurable variables. Note that the C++ variable names
		// are the FHiCL variable names with a "_" appended
		int request_port_;
		std::string request_addr_;
		bool running_;

		//Socket parameters
		int request_socket_;
		std::map<artdaq::Fragment::sequence_id_t, artdaq::Fragment::timestamp_t> requests_;
		std::map<artdaq::Fragment::sequence_id_t, std::chrono::steady_clock::time_point> request_timing_;
		std::atomic<bool> request_stop_requested_;
		std::chrono::steady_clock::time_point request_stop_timeout_;
		std::atomic<bool> request_received_;
		size_t end_of_run_timeout_ms_;
		std::atomic<bool> should_stop_;
		mutable std::mutex request_mutex_;
		std::condition_variable request_cv_;
		boost::thread requestThread_;
		
		artdaq::Fragment::sequence_id_t highest_seen_request_;
	};
}


#endif //ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH