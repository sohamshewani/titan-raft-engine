#ifndef TITAN_TYPES_HPP
#define TITAN_TYPES_HPP

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <array>
#include <string_view>
#include <system_error>
#include <atomic>
#include <new>
#include <type_traits>

namespace titan {

// Hardware alignment constant for cache-line isolation (mitigates false sharing)
constexpr std::size_t kCacheLineSize = 64;

//------------------------------------------------------------------------------
// Error Handling & Status Hierarchies
//------------------------------------------------------------------------------
enum class ErrorCode : uint32_t {
    Success = 0,
    CorruptionDetected = 1,
    OutOfBounds = 2,
    BufferOverflow = 3,
    BufferUnderflow = 4,
    InvalidState = 5,
    ChecksumMismatch = 6,
    IOError = 7,
    TermMismatch = 8
};

class TitanErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "titan_raft_engine";
    }

    std::string message(int ev) const override {
        switch (static_cast<ErrorCode>(ev)) {
            case ErrorCode::Success: return "Success";
            case ErrorCode::CorruptionDetected: return "Data corruption detected";
            case ErrorCode::OutOfBounds: return "Index out of bounds";
            case ErrorCode::BufferOverflow: return "Buffer overflow prevented";
            case ErrorCode::BufferUnderflow: return "Buffer underflow prevented";
            case ErrorCode::InvalidState: return "Invalid state transition or configuration";
            case ErrorCode::ChecksumMismatch: return "CRC32C checksum verification failed";
            case ErrorCode::IOError: return "Underlying I/O operation failed";
            case ErrorCode::TermMismatch: return "Raft term validation failed";
            default: return "Unknown Titan error";
        }
    }
};

inline const std::error_category& titan_error_category() noexcept {
    static TitanErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(ErrorCode e) noexcept {
    return std::error_code(static_cast<int>(e), titan_error_category());
}

} // namespace titan

namespace std {
template <>
struct is_error_code_enum<titan::ErrorCode> : std::true_type {};
}

namespace titan {

//------------------------------------------------------------------------------
// Cryptographic / Checksum Utilities (CRC32C Software Fallback/Intrinsic)
//------------------------------------------------------------------------------
class CRC32C {
public:
    static uint32_t Calculate(const uint8_t* data, std::size_t length) noexcept {
        // Standard Castagnoli polynomial CRC32C implementation
        uint32_t crc = 0xFFFFFFFF;
        for (std::size_t i = 0; i < length; ++i) {
            crc ^= data[i];
            for (int j = 0; j < 8; ++j) {
                uint32_t mask = -(crc & 1);
                crc = (crc >> 1) ^ (0x8ED701AF & mask);
            }
        }
        return ~crc;
    }
};

//------------------------------------------------------------------------------
// Fixed-Width Little-Endian Serialization Primitives
//------------------------------------------------------------------------------
class BinaryCodec {
public:
    static void WriteUInt64LE(uint8_t* dst, uint64_t val) noexcept {
        for (std::size_t i = 0; i < 8; ++i) {
            dst[i] = static_cast<uint8_t>(val >> (8 * i));
        }
    }

    static uint64_t ReadUInt64LE(const uint8_t* src) noexcept {
        uint64_t val = 0;
        for (std::size_t i = 0; i < 8; ++i) {
            val |= static_cast<uint64_t>(src[i]) << (8 * i);
        }
        return val;
    }

    static void WriteUInt32LE(uint8_t* dst, uint32_t val) noexcept {
        for (std::size_t i = 0; i < 4; ++i) {
            dst[i] = static_cast<uint8_t>(val >> (8 * i));
        }
    }

    static uint32_t ReadUInt32LE(const uint8_t* src) noexcept {
        uint32_t val = 0;
        for (std::size_t i = 0; i < 4; ++i) {
            val |= static_cast<uint32_t>(src[i]) << (8 * i);
        }
        return val;
    }
};

//------------------------------------------------------------------------------
// Small String Optimization (SSO) Command Payload Container
//------------------------------------------------------------------------------
class alignas(8) SmallPayload {
private:
    static constexpr std::size_t kCapacity = 31;
    
    union Storage {
        char sso[kCapacity + 1];
        struct {
            char* ptr;
            std::size_t capacity;
        } heap;
    } storage_;
    
    uint8_t size_and_flag_; // High bit set if heap-allocated, lower bits store size (up to 31 for SSO)

    bool IsHeap() const noexcept {
        return (size_and_flag_ & 0x80) != 0;
    }

    void SetHeap(std::size_t size) noexcept {
        size_and_flag_ = static_cast<uint8_t>(size | 0x80);
    }

    void SetSSO(std::size_t size) noexcept {
        size_and_flag_ = static_cast<uint8_t>(size & 0x7F);
    }

public:
    SmallPayload() noexcept {
        std::memset(storage_.sso, 0, sizeof(storage_.sso));
        SetSSO(0);
    }

    SmallPayload(const char* data, std::size_t size) {
        if (size > kCapacity) {
            storage_.heap.ptr = new char[size];
            std::memcpy(storage_.heap.ptr, data, size);
            storage_.heap.capacity = size;
            SetHeap(size);
        } else {
            std::memcpy(storage_.sso, data, size);
            storage_.sso[size] = '\0';
            SetSSO(size);
        }
    }

    ~SmallPayload() noexcept {
        if (IsHeap()) {
            delete[] storage_.heap.ptr;
        }
    }

    SmallPayload(const SmallPayload& other) {
        std::size_t sz = other.Size();
        if (other.IsHeap()) {
            storage_.heap.ptr = new char[sz];
            std::memcpy(storage_.heap.ptr, other.storage_.heap.ptr, sz);
            storage_.heap.capacity = sz;
            SetHeap(sz);
        } else {
            std::memcpy(storage_.sso, other.storage_.sso, sz + 1);
            size_and_flag_ = other.size_and_flag_;
        }
    }

    SmallPayload& operator=(const SmallPayload& other) {
        if (this == &other) return *this;
        
        if (IsHeap()) {
            delete[] storage_.heap.ptr;
        }

        std::size_t sz = other.Size();
        if (other.IsHeap()) {
            storage_.heap.ptr = new char[sz];
            std::memcpy(storage_.heap.ptr, other.storage_.heap.ptr, sz);
            storage_.heap.capacity = sz;
            SetHeap(sz);
        } else {
            std::memcpy(storage_.sso, other.storage_.sso, sz + 1);
            size_and_flag_ = other.size_and_flag_;
        }
        return *this;
    }

    SmallPayload(SmallPayload&& other) noexcept {
        std::memcpy(&storage_, &other.storage_, sizeof(storage_));
        size_and_flag_ = other.size_and_flag_;
        other.SetSSO(0);
        other.storage_.sso[0] = '\0';
    }

    SmallPayload& operator=(SmallPayload&& other) noexcept {
        if (this == &other) return *this;
        
        if (IsHeap()) {
            delete[] storage_.heap.ptr;
        }

        std::memcpy(&storage_, &other.storage_, sizeof(storage_));
        size_and_flag_ = other.size_and_flag_;
        other.SetSSO(0);
        other.storage_.sso[0] = '\0';
        return *this;
    }

    std::size_t Size() const noexcept {
        return IsHeap() ? storage_.heap.capacity : (size_and_flag_ & 0x7F);
    }

    const char* Data() const noexcept {
        return IsHeap() ? storage_.heap.ptr : storage_.sso;
    }

    std::string_view ToStringView() const noexcept {
        return std::string_view(Data(), Size());
    }
};

//------------------------------------------------------------------------------
// Cache-Line Aligned LogEntry (Persistent State Record)
//------------------------------------------------------------------------------
struct alignas(kCacheLineSize) LogEntry {
    uint64_t term;
    uint64_t index;
    SmallPayload command;
    uint32_t crc32c;

    LogEntry() noexcept : term(0), index(0), command(), crc32c(0) {}

    LogEntry(uint64_t t, uint64_t idx, const char* cmd_data, std::size_t cmd_size)
        : term(t), index(idx), command(cmd_data, cmd_size), crc32c(0) {
        UpdateChecksum();
    }

    void UpdateChecksum() noexcept {
        // Compute CRC over term (8 bytes), index (8 bytes), and command payload
        // Temporary buffer assembly for deterministic checksum calculation
        constexpr std::size_t kHeaderSize = 16;
        std::size_t cmd_sz = command.Size();
        
        // Use a stack buffer if possible, or dynamic buffer if command exceeds stack frame limits
        const std::size_t total_size = kHeaderSize + cmd_sz;
        if (total_size <= 512) {
            uint8_t buf[512];
            BinaryCodec::WriteUInt64LE(buf, term);
            BinaryCodec::WriteUInt64LE(buf + 8, index);
            std::memcpy(buf + 16, command.Data(), cmd_sz);
            crc32c = CRC32C::Calculate(buf, total_size);
        } else {
            // Fallback for large payloads exceeding local stack allocations
            auto* buf = new uint8_t[total_size];
            BinaryCodec::WriteUInt64LE(buf, term);
            BinaryCodec::WriteUInt64LE(buf + 8, index);
            std::memcpy(buf + 16, command.Data(), cmd_sz);
            crc32c = CRC32C::Calculate(buf, total_size);
            delete[] buf;
        }
    }

    bool VerifyChecksum() const noexcept {
        uint32_t current_crc = crc32c;
        // Re-evaluate
        constexpr std::size_t kHeaderSize = 16;
        std::size_t cmd_sz = command.Size();
        const std::size_t total_size = kHeaderSize + cmd_sz;
        
        uint32_t calculated = 0;
        if (total_size <= 512) {
            uint8_t buf[512];
            BinaryCodec::WriteUInt64LE(buf, term);
            BinaryCodec::WriteUInt64LE(buf + 8, index);
            std::memcpy(buf + 16, command.Data(), cmd_sz);
            calculated = CRC32C::Calculate(buf, total_size);
        } else {
            auto* buf = new uint8_t[total_size];
            BinaryCodec::WriteUInt64LE(buf, term);
            BinaryCodec::WriteUInt64LE(buf + 8, index);
            std::memcpy(buf + 16, command.Data(), cmd_sz);
            calculated = CRC32C::Calculate(buf, total_size);
            delete[] buf;
        }
        return current_crc == calculated;
    }
};

// Ensure strict cache-line sizing
static_assert(sizeof(LogEntry) % kCacheLineSize == 0 || sizeof(LogEntry) >= kCacheLineSize, 
              "LogEntry must align correctly with system cache architecture.");

//------------------------------------------------------------------------------
// Persistent Node State Struct
//------------------------------------------------------------------------------
struct alignas(kCacheLineSize) PersistentState {
    uint64_t current_term;
    uint64_t voted_for; // Node ID representation
    uint64_t commit_index;
    uint32_t crc32c;

    PersistentState() noexcept : current_term(0), voted_for(0), commit_index(0), crc32c(0) {
        UpdateChecksum();
    }

    void UpdateChecksum() noexcept {
        uint8_t buf[24];
        BinaryCodec::WriteUInt64LE(buf, current_term);
        BinaryCodec::WriteUInt64LE(buf + 8, voted_for);
        BinaryCodec::WriteUInt64LE(buf + 16, commit_index);
        crc32c = CRC32C::Calculate(buf, 24);
    }

    bool VerifyChecksum() const noexcept {
        uint8_t buf[24];
        BinaryCodec::WriteUInt64LE(buf, current_term);
        BinaryCodec::WriteUInt64LE(buf + 8, voted_for);
        BinaryCodec::WriteUInt64LE(buf + 16, commit_index);
        return crc32c == CRC32C::Calculate(buf, 24);
    }
};

//------------------------------------------------------------------------------
// Lock-Free MPSC Bounded Queue for Reactor-to-Consensus Messaging
//------------------------------------------------------------------------------
template <typename T, std::size_t Capacity>
class alignas(kCacheLineSize) LockFreeMPSCQueue {
private:
    struct Node {
        T data;
        std::atomic<Node*> next;
        Node() noexcept : next(nullptr) {}
    };

    alignas(kCacheLineSize) Node* head_;
    alignas(kCacheLineSize) char pad1_[kCacheLineSize - sizeof(Node*)];
    alignas(kCacheLineSize) std::atomic<Node*> tail_;
    alignas(kCacheLineSize) char pad2_[kCacheLineSize - sizeof(std::atomic<Node*>)];
    alignas(kCacheLineSize) std::atomic<std::size_t> size_;
    alignas(kCacheLineSize) char pad3_[kCacheLineSize - sizeof(std::atomic<std::size_t>)];
    
    Node stub_;

public:
    LockFreeMPSCQueue() {
        stub_.next.store(nullptr, std::memory_order_relaxed);
        head_ = &stub_;
        tail_.store(&stub_, std::memory_order_relaxed);
        size_.store(0, std::memory_order_relaxed);
    }

    ~LockFreeMPSCQueue() {
        T dummy;
        while (Dequeue(dummy)) {
            // Drain queue
        }
    }

    bool Enqueue(const T& item) noexcept {
        Node* node = new (std::nothrow) Node();
        if (!node) return false;
        node->data = item;
        node->next.store(nullptr, std::memory_order_relaxed);

        Node* prev = tail_.exchange(node, std::atomic_memory_order_acq_rel);
        prev->next.store(node, std::memory_order_release);
        size_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    bool Dequeue(T& item) noexcept {
        Node* head = head_;
        Node* next = head->next.load(std::memory_order_acquire);
        if (!next) {
            return false;
        }

        item = std::move(next->data);
        head_ = next;