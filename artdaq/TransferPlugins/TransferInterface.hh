#ifndef artdaq_ArtModules_TransferInterface_hh
#define artdaq_ArtModules_TransferInterface_hh

#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "fhiclcpp/ParameterSet.h"
#include "cetlib/compiler_macros.h"

#include <limits>
#include <iostream>
#include <sstream>

namespace artdaq
{
	/**
	 * \brief This interface defines the functions used to transfer data between artdaq applications.
	 */
	class TransferInterface
	{
	public:
		struct Config
		{
			fhicl::Atom<int> source_rank{ fhicl::Name{"source_rank"}, fhicl::Comment{"The rank that data is coming from"}, my_rank };
			fhicl::Atom<int> destination_rank{ fhicl::Name{ "destination_rank"}, fhicl::Comment{"The rank that data is going to"}, my_rank };
			fhicl::Atom<std::string> unique_label{ fhicl::Name{"unique_label"}, fhicl::Comment{"A label that uniquely identifies the TransferInterface instance"},"transfer_between_[source_rank]_and_[destination_rank]" };
			fhicl::Atom<size_t>	buffer_count{ fhicl::Name{"buffer_count"}, fhicl::Comment{"How many Fragments can the TransferInterface handle simultaneously"},10 };
			fhicl::Atom<size_t> max_fragment_size{ fhicl::Name{"max_fragment_size_words" }, fhicl::Comment{ "The maximum Fragment size expected.May be used for static memory allocation, and will cause errors if larger Fragments are sent." }, 1024 };
			fhicl::Atom<short> partition_number{ fhicl::Name{"partition_number"},fhicl::Comment{"Partition that this TransferInterface is a part of"}, 0 };

		};
#if MESSAGEFACILITY_HEX_VERSION >= 0x20103
		using Parameters = fhicl::WrappedTable<Config>;
#endif

		enum : int
		{
			DATA_END = -2222,///< Value that is to be returned when a Transfer plugin determines that no more data will be arriving.
			RECV_TIMEOUT = -1111, ///< Value to be returned upon receive timeout.
			RECV_SUCCESS = 0 ///< For code clarity, things checking for successful receive should check retval >= RECV_SUCCESS
		};

		/**
		 * \brief Used to determine if a TransferInterface is a Sender or Receiver
		 */
		enum class Role
		{
			kSend, ///< This TransferInterface is a Sender
			kReceive ///< This TransferInterface is a Receiver
		};

		/**
		 * \brief Returned from the send functions, this enumeration describes the possible return codes.
		 * If an exception occurs, it will be thrown and should be handled normally.
		 */
		enum class CopyStatus
		{
			kSuccess, ///< The send operation completed successfully
			kTimeout, ///< The send operation timed out
			kErrorNotRequiringException ///< Some error occurred, but no exception was thrown
		};

		static std::string CopyStatusToString(CopyStatus in)
		{
			switch (in)
			{
			case CopyStatus::kSuccess: return "Success";
			case CopyStatus::kTimeout: return "Timeout";
			case CopyStatus::kErrorNotRequiringException: return "Error";
			default: return "UNKNOWN";
			}
			return "SWITCHERROR";
		}

		/**
		 * \brief TransferInterface Constructor
		 * \param ps ParameterSet used for configuring the TransferInterface
		 * \param role Role of the TransferInterface (See TransferInterface::Role)
		 *
		 * \verbatim
		 * TransferInterface accepts the following Parameters:
		 * "source_rank" (Default: my_rank): The rank that data is coming from
		 * "destination_rank" (Default: my_rank): The rank that data is going to
		 * "unique_label" (Default: "transfer_between_[source_rank]_and_[destination_rank]"): A label that uniquely identifies the TransferInterface instance
		 * "buffer_count" (Default: 10): How many Fragments can the TransferInterface handle simultaneously
		 * "max_fragment_size_words" (Default: 1024): The maximum Fragment size expected. May be used for static memory allocation, and will cause errors
		 * if larger Fragments are sent.
		 * "partition_number" (Default: 0): Partition that this TransferInterface is a part of
		 * \endverbatim
		 */
		TransferInterface(const fhicl::ParameterSet& ps, Role role);

		/**
		 * \brief Copy Constructor is deleted
		 */
		TransferInterface(const TransferInterface&) = delete;

		/**
		 * \brief Copy Assignment operator is deleted
		 * \return TransferInterface Copy
		 */
		TransferInterface& operator=(const TransferInterface&) = delete;

		/**
		 * \brief Default virtual Destructor
		 */
		virtual ~TransferInterface() = default;

		/**
		 * \brief Receive a Fragment from the transport mechanism
		 * \param[out] fragment Received Fragment
		 * \param receiveTimeout Timeout for receive
		 * \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
		 */
		virtual int receiveFragment(artdaq::Fragment& fragment, size_t receiveTimeout);

		/**
		 * \brief Receive a Fragment Header from the transport mechanism
		 * \param[out] header Received Fragment Header
		 * \param receiveTimeout Timeout for receive
		 * \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
		 */
		virtual int receiveFragmentHeader(detail::RawFragmentHeader& header, size_t receiveTimeout) = 0;

		/**
		 * \brief Receive the body of a Fragment to the given destination pointer
		 * \param destination Pointer to memory region where Fragment data should be stored
		 * \param wordCount Number of words of Fragment data to receive
		 * \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
		 *
		 * The precondition for calling this function is that you have received a valid header, therefore it does
		 * not have a , as the Fragment data should immediately be available.
		 */
		virtual int receiveFragmentData(RawDataType* destination, size_t wordCount) = 0;

		/**
		* \brief Copy a Fragment to the destination. May not necessarily be reliable
		* \param fragment Fragment to copy
		* \param send_timeout_usec Timeout for send, in microseconds
		* \return CopyStatus detailing result of copy
		*/
		virtual CopyStatus copyFragment(artdaq::Fragment& fragment, size_t send_timeout_usec) = 0;

		// Move fragment (should be reliable)
		/**
		* \brief Move a Fragment to the destination. This should be reliable, if the underlying transport mechanism supports reliable sending
		* \param fragment Fragment to move
		* \return CopyStatus detailing result of copy
		*/
		virtual CopyStatus moveFragment(artdaq::Fragment&& fragment) = 0;

		/**
		 * \brief Get the unique label of this TransferInterface instance
		 * \return The unique label of this TransferInterface instance
		 */
		std::string uniqueLabel() const { return unique_label_; }

		/**
		 * \brief Get the source rank for this TransferInterface instance
		 * \return The source rank for this Transferinterface instance
		 */
		virtual int source_rank() const { return source_rank_; }
		/**
		 * \brief Get the destination rank for this TransferInterface instance
		 * \return The destination rank for this TransferInterface instance
		 */
		virtual int destination_rank() const { return destination_rank_; }

		/**
		 * \brief Determine whether the TransferInterface plugin is able to send/receive data
		 * \return True if the TransferInterface plugin is currently able to send/receive data
		 */
		virtual bool isRunning() { return false; }



		/**
		 * \brief Constructs a name suitable for TRACE messages
		 * \return The unique_label and a SEND/RECV identifier
		 */
		#define GetTraceName() unique_label_ << (role_ == Role::kSend ? "_SEND" : "_RECV")

	protected:
		const Role role_;

		const int source_rank_;
		const int destination_rank_;
		const std::string unique_label_;

		size_t buffer_count_; ///< The number of Fragment transfers the TransferInterface can handle simultaneously
		const size_t max_fragment_size_words_; ///< The maximum size of the transferred Fragment objects, in artdaq::Fragment::RawDataType words
		const short partition_number_; ///< The partition number of the DAQ

	protected:
		/**
		 * \brief Get the TransferInterface::Role of this TransferInterface
		 * \return The Role of this TransferInterface
		 */
		Role role() const { return role_; }
	};
}

#ifndef EXTERN_C_FUNC_DECLARE_START
#define EXTERN_C_FUNC_DECLARE_START extern "C" {
#endif

#define DEFINE_ARTDAQ_TRANSFER(klass)                                \
  EXTERN_C_FUNC_DECLARE_START                                      \
std::unique_ptr<artdaq::TransferInterface> make(fhicl::ParameterSet const & ps, \
								 artdaq::TransferInterface::Role role) { \
	return std::unique_ptr<artdaq::TransferInterface>(new klass(ps, role)); \
}}


#endif /* artdaq_ArtModules_TransferInterface.hh */

// Local Variables:
// mode: c++
// End:
