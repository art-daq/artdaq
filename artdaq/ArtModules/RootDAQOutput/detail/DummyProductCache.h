#ifndef art_Framework_IO_Root_detail_DummyProductCache_h
#define art_Framework_IO_Root_detail_DummyProductCache_h

#include "canvas/Persistency/Common/EDProduct.h"

#include <map>
#include <memory>
#include <string>

namespace art {
  namespace detail {

	  /**
	   * \brief A lightweight prodcut cache for when the full thing is not appropriate (TBB and ROOT don't fully get along)
	   */
    class DummyProductCache {
    public:
		/**
		 * \brief Retrieve a product handle from the cache
		 * \param wrappedName Name of the product handle
		 * \return EDProduct instance corresponding to the given name
		 */
      EDProduct const* product(std::string const& wrappedName);
    private:
      std::map<std::string,std::unique_ptr<EDProduct>> dummies_;
    };

  }
}

#endif

// Local variables:
// mode: c++
// End:
