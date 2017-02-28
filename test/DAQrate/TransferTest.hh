#ifndef ARTDAQ_TEST_DAQRATE_TRANSFERTEST_HH
#define ARTDAQ_TEST_DAQRATE_TRANSFERTEST_HH

#include <vector>
#include <string>
#include <chrono>
#include <cmath>

#include <fhiclcpp/ParameterSet.h>
#include "artdaq-utilities/Plugins/MetricManager.hh"

namespace artdaq {
	class TransferTest {
	public:
		TransferTest(fhicl::ParameterSet psi);
		int runTest();
	private:
        std::pair<size_t,double> do_sending();
        std::pair<size_t,double> do_receiving();

		//Helper functions
		const std::vector<std::string> suffixes{ " B", " KB", " MB", " GB", " TB" };
		std::string formatBytes(double bytes, size_t suffixIndex = 0);

		int senders_;
		int receivers_;
		int sends_each_sender_;
		int receives_each_receiver_; // Should be sends_each_sender * sending_ranks / receiving_ranks
		int buffer_count_;
		size_t max_payload_size_;
		std::chrono::steady_clock::time_point start_time_;
		fhicl::ParameterSet ps_;
		artdaq::MetricManager metricMan_;
	};

	inline std::string TransferTest::formatBytes(double bytes, size_t suffixIndex) {
		auto b = fabs(bytes);

		if (b > 1024.0 && suffixIndex < suffixes.size()) {
			return formatBytes(bytes / 1024.0, suffixIndex + 1);
		}

		return std::to_string(bytes) + suffixes[suffixIndex];
	}
}
#endif //ARTDAQ_TEST_DAQRATE_TRANSFERTEST_HH
