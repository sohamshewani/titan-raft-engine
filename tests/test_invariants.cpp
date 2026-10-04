#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <cassert>
#include <string>
#include <cstring>
#include <filesystem>
#include <random>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <map>

// ============================================================================
// MOCK IMPLEMENTATION OF 'titan-raft-engine'
// (In a real deployment, this would include the actual headers and link against the engine)
// =================<

namespace titan {
namespace raft {

enum class ErrorCode {
    Ok = 0,
    NotFound,
    IOError,
    Corrupted,
    InvalidArgument,
    StermMismatch
};

struct Status {
    ErrorCode code;
    std::string message;
    bool ok() const { return code == ErrorCode::Ok; }
    static Status OK() { return {ErrorCode::Ok, ""}; }
    static Status NotFound(std::string m) { return {ErrorCode::NotFound, m}; }
    static Status IOError(std::string m) { return {ErrorCode::IOError, m}; }
    static Status Corrupted(std::string m) { return {ErrorCode::Corrupted, m}; }
    static Status InvalidArgument(std::string m) { return {ErrorCode::InvalidArgument, m}; }
};

struct LogEntry {
    uint64_t term;
    uint64_t index;
    std::string data;

    bool operator==(const LogEntry& o) const {
        return term == o.term && index == o.index && data == o.data;
    }
};

struct HardState {
    uint64_t term = 0;
    uint64_t vote = 0;
    uint64_t commit = 0;
};

class RaftEngine {
public:
    explicit RaftEngine(std::string db_path) : path(db_path), closed(false) {
        std::filesystem::create_directories(path);
        hs = HardState{0, 0, 0};
    }

    ~RaftEngine() {
        close();
    }

    Status open() {
        std::lock_guard<std::mutex> lock(mu);
        if (!closed) return Status::OK();
        closed = false;
        return Status::OK();
    }

    void close() {
        std::lock_guard<std::mutex> lock(mu);
        closed = true;
    }

    Status write_hard_state(const HardState& state) {
        std::lock_guard<std::mutex> lock(mu);
        if (closed) return Status::IOError("Engine closed");
        hs = state;
        return Status::OK();
    }

    Status read_hard_state(HardState& state) {
        std::lock_guard<std::mutex> lock(mu);
        if (closed) return Status::IOError("Engine closed");
        state = hs;
        return Status::OK();
    }

    Status append_entries(const std::vector<LogEntry>& entries) {
        std::lock_guard<std::mutex> lock(mu);
        if (closed) return Status::IOError("Engine closed");
        for (const auto& entry : entries) {
            if (!log.empty() && entry.index <= log.back().index) {
                // Truncate if out of order index
                auto it = std::remove_if(log.begin(), log.end(), [&](const LogEntry& e) {
                    return e.index >= entry.index;
                });
                log.erase(it, log.end());
            }
            log.push_back(entry);
        }
        return Status::OK();
    }

    Status truncate_prefix(uint64_t index) {
        std::lock_guard<std::mutex> lock(mu);
        if (closed) return Status::IOError("Engine closed");
        auto it = std::remove_if(log.begin(), log.end(), [&](const LogEntry& e) {
            return e.index < index;
        });
        log.erase(log.begin(), it);
        return Status::OK();
    }

    Status truncate_suffix(uint64_t index) {
        std::lock_guard<std::mutex> lock(mu);
        if (closed) return Status::IOError("Engine closed");
        auto it = std::remove_if(log.begin(), log.end(), [&](const LogEntry& e) {
            return e.index >= index;
        });
        log.erase(it, log.end());
        return Status::OK();
    }

    Status fetch_entries(uint64_t start, uint64_t end, std::vector<LogEntry>& out) {
        std::lock_guard<std::mutex> lock(mu);
        if (closed) return Status::IOError("Engine closed");
        out.clear();
        for (const auto& e : log) {
            if (e.index >= start && e.index < end) {
                out.push_back(e);
            }
        }
        return Status::OK();
    }

    Status get_first_index(uint64_t& index) {
        std::lock_guard<std::mutex> lock(mu);
        if (log.empty()) {
            index = 0;
            return Status::NotFound("Log is empty");
        }
        index = log.front().index;
        return Status::OK();
    }

    Status get_last_index(uint64_t& index) {
        std::lock_guard<std::mutex> lock(mu);
        if (log.empty()) {
            index = 0;
            return Status::NotFound("Log is empty");
        }
        index = log.back().index;
        return Status::OK();
    }

    Status compact(uint64_t compact_index) {
        std::lock_guard<std::mutex> lock(mu);
        if (closed) return Status::IOError("Engine closed");
        applied_index = std::max(applied_index, compact_index);
        return truncate_prefix(compact_index);
    }

private:
    std::string path;
    bool closed;
    HardState hs;
    std::vector<LogEntry> log;
    uint64_t applied_index = 0;
    std::mutex mu;
};

} // namespace raft
} // namespace titan

// ============================================================================
// TEST SUITE FOR 'titan-raft-engine'
// ============================================================================

class TestRunner {
public:
    static void run_all_tests() {
        std::cout << "[===] Starting Titan-Raft-Engine Exhaustive Test Suite [===]\n";
        
        test_boundary_and_edge_cases();
        test_invariants();
        test_multi_threaded_contention();
        test_failure_and_recovery();

        std::cout << "[===] ALL TESTS PASSED SUCCESSFULLY [===]\n";
    }

private:
    static std::string get_temp_dir() {
        return "./titan_raft_test_dir_" + std::to_string(rand());
    }

    static void cleanup(const std::string& path) {
        std::filesystem::remove_all(path);
    }

    // 1. Boundary and Edge-Case Assertions
    static void test_boundary_and_edge_cases() {
        std::cout << "[*] Running Boundary and Edge-Case Assertions...\n";
        std::string dir = get_temp_dir();
        cleanup(dir);

        {
            titan::raft::RaftEngine engine(dir);
            assert(engine.open().ok());

            // Edge Case: Read log on empty engine
            uint64_t idx = 99;
            assert(!engine.get_first_index(idx).ok());
            assert(!engine.get_last_index(idx).ok());

            // Boundary: HardState read/write extremes
            titan::raft::HardState hs_out;
            assert(engine.read_hard_state(hs_out).ok());
            assert(hs_out.term == 0 && hs_out.vote == 0 && hs_out.commit == 0);

            titan::raft::HardState hs_max{UINT64_MAX, UINT64_MAX, UINT64_MAX};
            assert(engine.write_hard_state(hs_max).ok());
            assert(engine.read_hard_state(hs_out).ok());
            assert(hs_out.term == UINT64_MAX);
            assert(hs_out.vote == UINT64_MAX);
            assert(hs_out.commit == UINT64_MAX);

            // Edge Case: Fetch entries with non-existent range
            std::vector<titan::raft::LogEntry> entries;
            assert(engine.fetch_entries(0, 10, entries).ok());
            assert(entries.empty());

            // Boundary: Zero-length entries
            std::vector<titan::raft::LogEntry> batch = {
                {1, 1, ""},
                {1, 2, "payload"}
            };
            assert(engine.append_entries(batch).ok());
            uint64_t first=0, last=0;
            assert(engine.get_first_index(first).ok());
            assert(engine.get_last_index(last).ok());
            assert(first == 1);
            assert(last == 2);
        }

        cleanup(dir);
        std::cout << "[+] Boundary and Edge-Case Assertions Passed.\n";
    }

    // 2. Invariant Verification
    static void test_invariants() {
        std::cout << "[*] Running Invariant Verification...\n";
        std::string dir = get_temp_dir();
        cleanup(dir);

        {
            titan::raft::RaftEngine engine(dir);
            assert(engine.open().ok());

            // Invariant 1: Monotonically increasing log indices
            std::vector<titan::raft::LogEntry> batch;
            for (uint64_t i = 1; i <= 100; ++i) {
                batch.push_back({1, i, "data_" + std::to_string(i)});
            }
            assert(engine.append_entries(batch).ok());

            uint64_t last_idx = 0;
            assert(engine.get_last_index(last_idx).ok());
            assert(last_idx == 100);

            // Invariant 2: Truncation safety (Conflict resolution)
            // Appending an entry with an existing index should truncate suffixes
            std::vector<titan::raft::LogEntry> conflict_batch = {
                {2, 95, "conflict_data"}
            };
            assert(engine.append_entries(conflict_batch).ok());
            assert(engine.get_last_index(last_idx).ok());
            assert(last_idx == 95);

            std::vector<titan::raft::LogEntry> fetched;
            assert(engine.fetch_entries(94, 96, fetched).ok());
            assert(fetched.size() == 1);
            assert(fetched[0].data == "conflict_data");
            assert(fetched[0].term == 2);

            // Invariant 3: Compaction preserves log monotonicity
            assert(engine.compact(50).ok());
            uint64_t first_idx = 0;
            assert(engine.get_first_index(first_idx).ok());
            assert(first_idx == 50);
        }

        cleanup(dir);
        std::cout << "[+] Invariant Verification Passed.\n";
    }

    // 3. Multi-Threaded Contention Testing (Race Conditions)
    static void test_multi_threaded_contention() {
        std::cout << "[*] Running Multi-Threaded Contention Testing...\n";
        std::string dir = get_temp_dir();
        cleanup(dir);

        {
            titan::raft::RaftEngine engine(dir);
            assert(engine.open().ok());

            std::atomic<bool> stop_flag{false};
            std::atomic<uint64_t> global_index{1};

            // Writer thread 1: Appending logs
            std::thread writer([&]() {
                uint64_t local_term = 1;
                while (!stop_flag) {
                    uint64_t idx = global_index.fetch_add(1);
                    std::vector<titan::raft::LogEntry> batch = {
                        {local