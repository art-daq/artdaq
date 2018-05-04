#ifndef ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH
#define ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH

#include <boost/thread.hpp>
#include "artdaq-core/Data/Fragment.hh"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Atom.h"
#if MESSAGEFACILITY_HEX_VERSION >= 0x20103
#include "fhiclcpp/types/ConfigurationTable.h"
#endif

#include <mutex>
#include <condition_variable>

namespace artdaq
{
	class RequestReceiver
	{
	public:

		struct Config
		{
			fhicl::Atom<int> request_port{ fhicl::Name{"request_port"}, fhicl::Comment{"Port to listen for request messages on"}, 3001 };
			fhicl::Atom<std::string> request_addr{ fhicl::Name{"request_address"}, fhicl::Comment{"Multicast address to listen for request messages on"}, "227.128.12.26" };
			fhicl::Atom<std::string> output_address{ fhicl::Name{ "multicast_interface_ip" }, fhicl::Comment{ "Use this hostname for multicast (to assign to the proper NIC)" }, "0.0.0.0" };
			fhicl::Atom<size_t> end_of_run_timeout_ms{ fhicl::Name{"end_of_run_quiet_timeout_ms"}, fhicl::Comment{"Amount of time (in ms) to wait for no new requests when a Stop transition is pending"}, 1000 };
		};
#if MESSAGEFACILITY_HEX_VERSION >= 0x20103
		using Parameters = fhicl::WrappedTable<Config>;
#endif

		RequestReceiver();
		RequestReceiver(const fhicl::ParameterSet& ps);
		virtual ~RequestReceiver();

		/**
		* \brief Opens the socket used to listen for data requests
		*/
		void setupRequestListener();

		/**
		* \brief Stop the data request receiver thread (receiveRequestsLoop)
		*/
		void stopRequestReceiverThread();

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
			for (auto& in : requests_)
			{
				out[in.first] = in.second;
			}
			return out;
		}

		void RemoveRequest(artdaq::Fragment::sequence_id_t reqID);

		bool isRunning() { return running_; }

		void ClearRequests()
		{
			std::unique_lock<std::mutex> lk(request_mutex_);
			requests_.clear();
		}

		size_t size() { return requests_.size(); }

		bool WaitForRequests(int timeout_ms)
		{
			std::unique_lock<std::mutex> lk(request_mutex_);
			return request_cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [this]() { return requests_.size() > 0; });
		}
	private:
		// FHiCL-configurable variables. Note that the C++ variable names
		// are the FHiCL variable names with a "_" appended
		int request_port_;
		std::string request_addr_;
		std::string multicast_out_addr_;
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
		mutable std::mutex state_mutex_;
		std::condition_variable request_cv_;
		boost::thread requestThread_;

		std::atomic<artdaq::Fragment::sequence_id_t> highest_seen_request_;
	};
}


#endif //ARTDAQ_DAQRATE_REQUEST_RECEVIER_HH
