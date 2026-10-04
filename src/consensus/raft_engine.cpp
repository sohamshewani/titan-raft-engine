#include "consensus/raft_engine.hpp"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <system_error>

namespace titan {
namespace consensus {

// CRC32C (Castagnoli) Implementation using standard software fallback
// In a true high-performance environment, this would utilize __builtin_ia32_crc32si if available.
uint32_t RaftEngine::ComputeCRC32C(const uint8_t* data, size_t length) noexcept {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; ++i) {
        crc ^= data[i];
        for (int j = 0; j < 8; ++j) {
            uint32_t mask = -(crc & 1);
            crc = (crc >> 1) ^ (0x82F63B78 & mask);
        }
    }
    return ~crc;
}

void RaftEngine::WriteLittleEndianU64(uint8_t* dest, uint64_t val) noexcept {
    for (int i = 0; i < 8; ++i) {
        dest[i] = static_cast<uint8_t>(val >> (8 * i));
    }
}

uint64_t RaftEngine::ReadLittleEndianU64(const uint8_t* src) noexcept {
    uint64_t val = 0;
    for (int i = 0; i < 8; ++i) {
        val |= static_cast<uint64_t>(src[i]) << (8 * i);
    }
    return val;
}

void RaftEngine::WriteLittleEndianU32(uint8_t* dest, uint32_t val) noexcept {
    for (int i = 0; i < 4; ++i) {
        dest[i] = static_cast<uint8_t>(val >> (8 * i));
    }
}

uint32_t RaftEngine::ReadLittleEndianU32(const uint8_t* src) noexcept {
    uint32_t val = 0;
    for (int i = 0; i < 4; ++i) {
        val |= static_cast<uint32_t>(src[i]) << (8 * i);
    }
    return val;
}

RaftEngine::RaftEngine(NodeConfig config)
    : config_(std::move(config)),
      current_term_(0),
      voted_for_(0),
      commit_index_(0),
      last_applied_(0),
      leader_id_(0),
      state_(RaftState::Follower),
      running_(false),
      snapshot_index_(0),
      snapshot_term_(0),
      ring_head_(0),
      ring_tail_(0) {
    
    // Initialize ring buffer memory slab
    std::memset(ring_slab_, 0, sizeof(ring_slab_));
    
    // Seed random generator for randomized election timeouts
    std::random_device rd;
    rng_.seed(rd());
}

RaftEngine::~RaftEngine() {
    Stop();
}

void RaftEngine::Initialize() {
    std::filesystem::create_directories(config_.wal_directory);
    std::filesystem::create_directories(config_.snapshot_directory);

    RecoverWAL();
    
    // If WAL is completely empty, initialize default persistent state at term 0, index 0
    if (log_.empty()) {
        LogEntry sentinel;
        sentinel.term = 0;
        sentinel.index = 0;
        sentinel.type = LogEntryType::Command;
        log_.push_back(sentinel);
    }

    running_.store(true, std::memory_order_release);
    consensus_thread_ = std::thread(&RaftEngine::RunConsensusLoop, this);
}

void RaftEngine::Stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    
    // Wake up consensus loop if waiting
    state_cond_.notify_all();
    
    if (consensus_thread_.joinable()) {
        consensus_thread_.join();
    }
    
    CloseWALFiles();
}

void RaftEngine::RecoverWAL() {
    std::filesystem::path wal_path = std::filesystem::path(config_.wal_directory) / "raft.wal";
    if (!std::filesystem::exists(wal_path)) {
        wal_file_out_.open(wal_path, std::ios::out | std::ios::binary | std::ios::app);
        return;
    }

    wal_file_in_.open(wal_path, std::ios::in | std::ios::binary);
    if (!wal_file_in_.is_open()) {
        throw std::runtime_error("Failed to open WAL file for recovery: " + wal_path.string());
    }

    uint64_t valid_bytes = 0;
    while (wal_file_in_) {
        // Read fixed header prefix: Term (8) + Index (8) + Type (1) + PayloadLen (8) = 25 bytes
        uint8_t header_buf[25];
        wal_file_in_.read(reinterpret_cast<char*>(header_buf), sizeof(header_buf));
        if (wal_file_in_.gcount() < static_cast<std::streamsize>(sizeof(header_buf))) {
            break;
        }

        uint64_t term = ReadLittleEndianU64(header_buf);
        uint64_t index = ReadLittleEndianU64(header_buf + 8);
        uint8_t type_val = header_buf[16];
        uint64_t payload_len = ReadLittleEndianU64(header_buf + 17);

        if (payload_len > 1024 * 1024 * 64) { // Sanity check: 64MB max payload per entry
            // Corrupted record length, truncate here
            break;
        }

        std::vector<uint8_t> payload(payload_len);
        wal_file_in_.read(reinterpret_cast<char*>(payload.data()), payload_len);
        if (wal_file_in_.gcount() < static_cast<std::streamsize>(payload_len)) {
            // Unexpected EOF, truncate tail corruption
            break;
        }

        uint8_t crc_buf[4];
        wal_file_in_.read(reinterpret_cast<char*>(crc_buf), sizeof(crc_buf));
        if (wal_file_in_.gcount() < static_cast<std::streamsize>(sizeof(crc_buf))) {
            break;
        }
        uint32_t stored_crc = ReadLittleEndianU32(crc_buf);

        // Compute CRC over header and payload
        uint32_t computed_crc = ComputeCRC32C(header_buf, sizeof(header_buf));
        computed_crc = UpdateCRC32C(computed_crc, payload.data(), payload.size());

        if (computed_crc != stored_crc) {
            // Checksum mismatch -> corruption detected, truncate tail
            break;
        }

        LogEntry entry;
        entry.term = term;
        entry.index = index;
        entry.type = static_cast<LogEntryType>(type_val);
        entry.command = std::string(reinterpret_cast<char*>(payload.data()), payload.size());

        log_.push_back(std::move(entry));
        valid_bytes = wal_file_in_.tellg();
    }

    wal_file_in_.close();

    // Truncate file to valid bytes
    std::resize_file(wal_path, valid_bytes);

    // Re-open for append
    wal_file_out_.open(wal_path, std::ios::out | std::ios::binary | std::ios::app);
    if (!wal_file_out_.is_open()) {
        throw std::runtime_error("Failed to re-open WAL file for appending after recovery.");
    }
}

void RaftEngine::CloseWALFiles() {
    if (wal_file_out_.is_open()) {
        wal_file_out_.flush();
        wal_file_out_.close();
    }
    if (wal_file_in_.is_open()) {
        wal_file_in_.close();
    }
}

uint32_t RaftEngine::UpdateCRC32C(uint32_t crc, const uint8_t* data, size_t length) noexcept {
    for (size_t i = 0; i < length; ++i) {
        crc ^= data[i];
        for (int j = 0; j < 8; ++j) {
            uint32_t mask = -(crc & 1);
            crc = (crc >> 1) ^ (0x82F63B78 & mask);
        }
    }
    return crc;
}

void RaftEngine::AppendLogToWAL(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(wal_mutex_);
    
    uint64_t payload_len = entry.command.size();
    size_t total_size = 8 + 8 + 1 + 8 + payload_len + 4;
    std::vector<uint8_t> buffer(total_size);

    WriteLittleEndianU64(buffer.data(), entry.term);
    WriteLittleEndianU64(buffer.data() + 8, entry.index);
    buffer[16] = static_cast<uint8_t>(entry.type);
    WriteLittleEndianU64(buffer.data() + 17, payload_len);
    std::memcpy(buffer.data() + 25, entry.command.data(), payload_len);

    uint32_t crc = ComputeCRC32C(buffer.data(), 25 + payload_len);
    WriteLittleEndianU32(buffer.data() + 25 + payload_len, crc);

    wal_file_out_.write(reinterpret_cast<char*>(buffer.data()), buffer.size());
    wal_file_out_.flush();
}

bool RaftEngine::SubmitClientRequest(std::string command, ClientCallback callback) {
    ClientRequest req{std::move(command), std::move(callback)};
    return client_request_queue_.enqueue(std::move(req));
}

bool RaftEngine::HandleAppendEntries(const AppendEntriesArgs& args, AppendEntriesReply& reply) {
    std::lock_guard<std::mutex> lock(consensus_mutex_);
    
    reply.term = current_term_;
    reply.success = false;

    // 1. Reply false if term < currentTerm
    if (args.term < current_term_) {
        return true;
    }

    // If term > currentTerm, step down and update term
    if (args.term > current_term_) {
        current_term_ = args.term;
        voted_for_ = 0;
        TransitionToState(RaftState::Follower);
    }

    leader_id_ = args.leader_id;
    ResetElectionTimeout();

    // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
    if (args.prev_log_index > 0) {
        if (args.prev_log_index >= log_.size() || log_[args.prev_log_index].term != args.prev_log_term) {
            return true;
        }
    }

    // 3. If an existing entry conflicts with a new one (same index but different terms),
    // delete the existing entry and all that follow it
    size_t insert_idx = args.prev_log_index + 1;
    size_t arg_entry_idx = 0;

    while (insert_idx < log_.size() && arg_entry_idx < args.entries.size()) {
        if (log_[insert_idx].term != args.entries[arg_entry_idx].term) {
            // Conflict found, truncate from here
            log_.erase(log_.begin() + insert_idx, log_.end());
            break;
        }
        insert_idx++;
        arg_entry_idx++;
    }

    // 4. Append any new entries not already in the log
    while (arg_entry_idx < args.entries.size()) {
        LogEntry entry = args.entries[arg_entry_idx];
        AppendLogToWAL(entry);
        log_.push_back(std::move(entry));
        arg_entry_idx++;
    }

    // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if (args.leader_commit > commit_index_) {
        uint64_t last_new_index = log_.empty() ? 0 : log_.back().index;
        commit_index_ = std::min(args.leader_commit, last_new_index);
        ApplyEntriesToStateMachine();
    }

    reply.success = true;
    reply.term = current_term_;
    return true;
}

bool RaftEngine::HandleRequestVote(const RequestVoteArgs& args, RequestVoteReply& reply) {
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    reply.term = current_term_;
    reply.vote_granted = false;

    if (args.term < current_term_) {
        return true;
    }

    if (args.term > current_term_) {
        current_term_ = args.term;
        voted_for_ = 0;
        TransitionToState(RaftState::Follower);
    }

    // Check voting constraints: votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log
    bool term_ok = (voted_for_ == 0 || voted_for_ == args.candidate_id);
    
    uint64_t last_log_index = log_.empty() ? 0 : log_.back().index;
    uint64_t last_log_term = log_.empty() ? 0 : log_.back().term;

    bool log_ok = (args.last_log_term > last_log_term) ||
                  (args.last_log_term == last_log_term && args.last_log_index >= last_log_index);

    if (term_ok && log_ok) {
        voted_for_ = args.candidate_id;
        reply.vote_granted = true;
        ResetElectionTimeout();
    }

    reply.term = current_term_;
    return true;
}

bool RaftEngine::HandleInstallSnapshot(const InstallSnapshotArgs& args, InstallSnapshotReply& reply) {
    std::lock_guard<std::mutex> lock(consensus_mutex_);
    
    reply.term = current_term_;

    if (args.term < current_term_) {
        return true;
    }

    if (args.term > current_term_) {
        current_term_ = args.term;
        voted_for_ = 0;
        TransitionToState(RaftState::Follower);
    }

    leader_id_ = args.leader_id;
    ResetElectionTimeout();

    if (args.last_included_index <= snapshot_index_) {
        return true;
    }

    // Discard entire log or prefix up to last_included_index
    if (args.last_included_index >= (log_.empty() ? 0 : log_.back().index)) {
        log_.clear();
        LogEntry sentinel;
        sentinel.term = args.last_included_term;
        sentinel.index = args.last_included_index;
        sentinel.type = LogEntryType::Command;
        log_.push_back(sentinel);
    } else {
        auto it = std::find_if(log_.begin(), log_.end(), [idx = args.last_included_index](const LogEntry& e) {
            return e.index == idx;
        });
        if (it != log_.end()) {
            log_.erase(log_.begin(), it + 1);
            LogEntry sentinel;
            sentinel.term = args.last_included_term;
            sentinel.index = args.last_included_index;
            sentinel.type = LogEntryType::Command;
            log_.insert(log_.begin(), sentinel);
        }
    }

    snapshot_index_ = args.last