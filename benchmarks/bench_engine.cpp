#include <chrono>
#include <vector>
#include <string>
#include <numeric>
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>

// Mocking the titan-raft-engine interface for standalone compilation and demonstration.
// In a real-world scenario, this would include "titan_raft_engine.h" and link against the library.
namespace titan {
namespace raft {
    enum class Status { Ok, Error };
    
    class RaftEngine {
    public:
        Status Open(const std::string& path) { return Status::Ok; }
        Status Write(const std::string& key, const std::string& value) {
            // Simulate low-latency persistent log append (e.g., ~5-15 microseconds)
            // Using a tight spin/pause to mimic memory/cache operations of a fast engine
            for(volatile int i = 0; i < 150; ++i) {}
            return Status::Ok;
        }
        Status Read(const std::string& key, std::string& value) {
            value = "mock_value";
            return Status::Ok;
        }
    };
}
}

class LatencyRecorder {
public:
    explicit LatencyRecorder(size_t capacity) {
        latencies_.reserve(capacity);
    }

    void Record(uint64_t nanos) {
        latencies_.push_back(nanos);
    }

    void Sort() {
        std::sort(latencies_.begin(), latencies_.end());
    }

    double GetPercentile(double p) const {
        if (latencies_.empty()) return 0.0;
        size_t index = static_cast<size_t>(p / 100.0 * (latencies_.size() - 1));
        return static_cast<double>(latencies_[index]);
    }

    size_t Size() const { return latencies_.size(); }

private:
    std::vector<uint64_t> latencies_;
};

int main() {
    const size_t kWarmupIterations = 50000;
    const size_t kBenchmarkIterations = 500000;
    const std::string kTestDbPath = "./titan_raft_bench_dir";

    titan::raft::RaftEngine engine;
    if (engine.Open(kTestDbPath) != titan::raft::Status::Ok) {
        std::cerr << "Failed to open titan-raft-engine instance." << std::endl;
        return 1;
    }

    std::string key_prefix = "bench_key_";
    std::string val_payload = "bench_value_payload_1024_bytes_................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................";

    // --- Warmup Phase ---
    for (size_t i = 0; i < kWarmupIterations; ++i) {
        std::string key = key_prefix + std::to_string(i);
        engine.Write(key, val_payload);
    }

    // --- Benchmark Phase (Write/Append Throughput & Latency) ---
    LatencyRecorder recorder(kBenchmarkIterations);
    
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < kBenchmarkIterations; ++i) {
        std::string key = key_prefix + std::to_string(kWarmupIterations + i);
        
        auto op_start = std::chrono::high_resolution_clock::now();
        auto status = engine.Write(key, val_payload);
        auto op_end = std::chrono::high_resolution_clock::now();

        if (status != titan::raft::Status::Ok) {
            std::cerr << "Write operation failed at iteration " << i << std::endl;
            continue;
        }

        uint64_t nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
        recorder.Record(nanos);
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

    recorder.Sort();

    double total_seconds = static_cast<double>(total_duration) / 1e9;
    double ops_per_sec = static_cast<double>(kBenchmarkIterations) / total_seconds;
    
    double p50_us = recorder.GetPercentile(50.0) / 1000.0;
    double p99_us = recorder.GetPercentile(99.0) / 1000.0;
    double p999_us = recorder.GetPercentile(99.9) / 1000.0;

    // --- Formatted Tabular Output ---
    std::cout << "\n";
    std::cout << "===================================================================\n";
    std::cout << "         titan-raft-engine Microbenchmark Results                  \n";
    std::cout << "===================================================================\n";
    std::cout << "| Metric                  | Value          | Unit                 |\n";
    std::cout << "|-------------------------|----------------|----------------------|\n";
    std::cout << "| Total Operations        | " << std::setw(14) << kBenchmarkIterations << " | ops                  |\n";
    std::cout << "| Total Time              | " << std::setw(14) << std::fixed << std::setprecision(4) << total_seconds << " | s                    |\n";
    std::cout << "| Throughput              | " << std::setw(14) << std::fixed << std::setprecision(2) << ops_per_sec << " | ops/sec              |\n";
    std::cout << "| Latency P50             | " << std::setw(14) << std::fixed << std::setprecision(3) << p50_us << " | us                   |\n";
    std::cout << "| Latency P99             | " << std::setw(14) << std::fixed << std::setprecision(3) << p99_us << " | us                   |\n";
    std::cout << "| Latency P99.9           | " << std::setw(14) << std::fixed << std::setprecision(3) << p999_us << " | us                   |\n";
    std::cout << "===================================================================\n";
    std::cout << "\n";

    return 0;
}