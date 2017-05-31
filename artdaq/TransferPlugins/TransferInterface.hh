#ifndef artdaq_ArtModules_TransferInterface_hh
#define artdaq_ArtModules_TransferInterface_hh

#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "fhiclcpp/ParameterSet.h"

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
		/**
		 * \brief Value to be returned upon receive timeout. Because receivers otherwise return rank,
		 * this is also the limit on the number of ranks that artdaq currently supports.
		 */
		static const int RECV_TIMEOUT = 0xfedcba98;

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
		 * 
		 * This is a pure virtual function, derived classes should override it
		 */
		virtual int receiveFragment(artdaq::Fragment& fragment, size_t receiveTimeout) = 0;

		/**
		* \brief Copy a Fragment to the destination. May not necessarily be reliable
		* \param fragment Fragment to copy
		* \param send_timeout_usec Timeout for send, in microseconds
		* \return CopyStatus detailing result of copy
		*/
		virtual CopyStatus copyFragment(artdaq::Fragment& fragment, size_t send_timeout_usec = std::numeric_limits<size_t>::max()) = 0;

		// Move fragment (should be reliable)
		/**
		* \brief Move a Fragment to the destination. This should be reliable, if the underlying transport mechanism supports reliable sending
		* \param fragment Fragment to move
		* \param send_timeout_usec Timeout for send, in microseconds
		* \return CopyStatus detailing result of copy
		*/
		virtual CopyStatus moveFragment(artdaq::Fragment&& fragment, size_t send_timeout_usec = std::numeric_limits<size_t>::max()) = 0;

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
	private:
		const Role role_;

		const int source_rank_;
		const int destination_rank_;
		const std::string unique_label_;

	protected:
		size_t buffer_count_; ///< The number of Fragment transfers the TransferInterface can handle simultaneously
		const size_t max_fragment_size_words_; ///< The maximum size of the transferred Fragment objects, in artdaq::Fragment::RawDataType words
	protected:
		/**
		 * \brief Get the TransferInterface::Role of this TransferInterface
		 * \return The Role of this TransferInterface
		 */
		Role role() const { return role_; }
	};
}

#define DEFINE_ARTDAQ_TRANSFER(klass)                                \
  extern "C" std::unique_ptr<artdaq::TransferInterface> make(fhicl::ParameterSet const & ps, \
								 artdaq::TransferInterface::Role role) { \
	return std::unique_ptr<artdaq::TransferInterface>(new klass(ps, role)); \
}


#endif /* artdaq_ArtModules_TransferInterface.hh */

// Local Variables:
// mode: c++
// End:
