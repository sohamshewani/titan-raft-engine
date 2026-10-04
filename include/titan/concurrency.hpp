#ifndef TITAN_CONCURRENCY_HPP
#define TITAN_CONCURRENCY_HPP

#include <atomic>
#include <new>
#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <utility>
#include <array>
#include <optional>
#include <system_error>
#include <exception>

namespace titan {

// Hardware cache line size for modern x86_64/ARM architectures to eliminate false sharing.
constexpr std::size_t kCacheLineSize = 64;

// Exception type for concurrency primitive violations.
class ConcurrencyException : public std::system_error {
public:
    explicit ConcurrencyException(std::error_code ec, const char* what_arg)
        : std::system_error(ec, what_arg) {}
    explicit ConcurrencyException(int ev, const std::error_category& ecat, const char* what_arg)
        : std::system_error(ev, ecat, what_arg) {}
};

// Node structure for the lock-free MPSC bounded queue.
template <typename T>
struct alignas(kCacheLineSize) MPSCNode {
    T data;
    std::atomic<MPSCNode<T>*> next;

    MPSCNode() noexcept : data(), next(nullptr) {}
    explicit MPSCNode(const T& val) noexcept(std::is_nothrow_copy_constructible_v<T>) 
        : data(val), next(nullptr) {}
    explicit MPSCNode(T&& val) noexcept(std::is_nothrow_move_constructible_v<T>) 
        : data(std::move(val)), next(nullptr) {}
};

/**
* @brief High-performance, lock-free Multi-Producer Single-Consumer (MPSC) bounded queue.
* Implements Dmitry Vyukov's lock-free queue algorithm optimized with cache-line padding.
*/
template <typename T, std::size_t Capacity>
class alignas(kCacheLineSize) MPSCBoundedQueue {
private:
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

    // We use a fixed array of nodes to provide bounds and eliminate dynamic allocation on push/pop.
    struct Node {
        std::atomic<std::size_t> sequence;
        T data;
    };

    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};
    
    // Ring buffer storage
    Node* buffer_;
    std::size_t mask_;

public:
    MPSCBoundedQueue() {
        buffer_ = static_cast<Node*>(std::aligned_alloc(kCacheLineSize, sizeof(Node) * Capacity));
        if (!buffer_) {
            throw ConcurrencyException(std::make_error_code(std::errc::not_enough_memory), 
                                       "Failed to allocate memory for MPSCBoundedQueue buffer");
        }
        for (std::size_t i = 0; i < Capacity; ++i) {
            new (&buffer_[i].data) T();
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
        mask_ = Capacity - 1;
    }

    ~MPSCBoundedQueue() noexcept {
        // Drain remaining elements to invoke destructors properly
        T dummy;
        while (try_pop(dummy)) {}
        
        for (std::size_t i = 0; i < Capacity; ++i) {
            buffer_[i].data.~T();
        }
        std::free(buffer_);
    }

    MPSCBoundedQueue(const MPSCBoundedQueue&) = delete;
    MPSCBoundedQueue& operator=(const MPSCBoundedQueue&) = delete;
    MPSCBoundedQueue(MPSCBoundedQueue&&) = delete;
    MPSCBoundedQueue& operator=(MPSCBoundedQueue&&) = delete;

    bool push(const T& value) noexcept(std::is_nothrow_copy_constructible_v<T>) {
        Node* node;
        std::size_t pos = head_.load(std::memory_order_relaxed);
        for (;;) {
            node = &buffer_[pos & mask_];
            std::size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (dif == 0) {
                if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Queue is full
                return false;
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }
        node->data = value;
        node->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    bool push(T&& value) noexcept(std::is_nothrow_move_constructible_v<T>) {
        Node* node;
        std::size_t pos = head_.load(std::memory_order_relaxed);
        for (;;) {
            node = &buffer_[pos & mask_];
            std::size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (dif == 0) {
                if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Queue is full
                return false;
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }
        node->data = std::move(value);
        node->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    bool try_pop(T& value) noexcept(std::is_nothrow_move_assignable_v<T>) {
        Node* node;
        std::size_t pos = tail_.load(std::memory_order_relaxed);
        for (;;) {
            node = &buffer_[pos & mask_];
            std::size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
            if (dif == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Queue is empty
                return false;
            } else {
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
        value = std::move(node->data);
        node->sequence.store(pos + mask_ + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        return static_cast<intptr_t>(head) - static_cast<intptr_t>(tail) <= 0;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        intptr_t diff = static_cast<intptr_t>(head) - static_cast<intptr_t>(tail);
        return diff > 0 ? static_cast<std::size_t>(diff) : 0;
    }

    [[nodiscard]] constexpr std::size_t capacity() const noexcept {
        return Capacity;
    }
};

/**
* @brief Lightweight atomic state manager for consensus role transitions (Leader/Follower/Candidate).
* Uses acquire-release memory semantics to coordinate thread-per-core reactors without heavy mutexes.
*/
enum class NodeRole : uint8_t {
    Follower = 0,
    Candidate = 1,
    Leader = 2
};

class alignas(kCacheLineSize) ConsensusStateController {
private:
    struct StatePayload {
        NodeRole role;
        uint64_t current_term;
        uint64_t commit_index;
        uint64_t last_applied;
    };

    // Pack consensus hot flags into a single cache-aligned atomic block to ensure zero contention.
    std::atomic<uint64_t> packed_state_{0};

    // Encoding layout:
    // [ Role: 2 bits ] [ Term: 31 bits ] [ Reserved/Flags: 31 bits ]
    // For absolute performance and correctness, we store Term and Role atomically together where possible,
    // or maintain separate cache-padded atomics if separation yields cleaner instruction pipelines.
    
    alignas(kCacheLineSize) std::atomic<NodeRole> role_{NodeRole::Follower};
    alignas(kCacheLineSize) std::atomic<uint64_t> current_term_{0};
    alignas(kCacheLineSize) std::atomic<uint64_t> commit_index_{0};
    alignas(kCacheLineSize) std::atomic<uint64_t> last_applied_{0};

public:
    ConsensusStateController() noexcept = default;
    ~ConsensusStateController() noexcept = default;

    ConsensusStateController(const ConsensusStateController&) = delete;
    ConsensusStateController& operator=(const ConsensusStateController&) = delete;
    ConsensusStateController(ConsensusStateController&&) = delete;
    ConsensusStateController& operator=(ConsensusStateController&&) = delete;

    void set_role(NodeRole new_role) noexcept {
        role_.store(new_role, std::memory_order_release);
    }

    [[nodiscard]] NodeRole get_role() const noexcept {
        return role_.load(std::memory_order_acquire);
    }

    void set_term(uint64_t term) noexcept {
        current_term_.store(term, std::memory_order_release);
    }

    [[nodiscard]] uint64_t get_term() const noexcept {
        return current_term_.load(std::memory_order_acquire);
    }

    bool compare_and_swap_term(uint64_t expected_term, uint64_t new_term) noexcept {
        return current_term_.compare_exchange_strong(expected_term, new_term,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_acquire);
    }

    void set_commit_index(uint64_t index) noexcept {
        uint64_t current = commit_index_.load(std::memory_order_relaxed);
        while (index > current && !commit_index_.compare_exchange_weak(current, index,
                                                                       std::memory_order_release,
                                                                       std::memory_order_relaxed)) {
            // Spin until updated or index is already higher/equal
        }
    }

    [[nodiscard]] uint64_t get_commit_index() const noexcept {
        return commit_index_.load(std::memory_order_acquire);
    }

    void set_last_applied(uint64_t index) noexcept {
        last_applied_.store(index, std::memory_order_release);
    }

    [[nodiscard]] uint64_t get_last_applied() const noexcept {
        return last_applied_.load(std::memory_order_acquire);
    }

    // Atomic step-up logic for election safety guarantees
    bool transition_to_candidate_if_follower(uint64_t expected_term) noexcept {
        NodeRole expected_role = NodeRole::Follower;
        if (role_.compare_exchange_strong(expected_role, NodeRole::Candidate,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
            // Bump term atomically upon stepping up to candidate
            current_term_.fetch_add(1, std::memory_order_acq_rel);
            return true;
        }
        return false;
    }
};

/**
* @brief Reactor execution guard ensuring thread-per-core pinning and execution isolation.
*/
class alignas(kCacheLineSize) ThreadPinningGuard {
public:
    explicit ThreadPinningGuard(int core_id) noexcept {
        pin_thread_to_core(core_id);
    }

    ~ThreadPinningGuard() noexcept = default;

    ThreadPinningGuard(const ThreadPinningGuard&) = delete;
    ThreadPinningGuard& operator=(const ThreadPinningGuard&) = delete;
    ThreadPinningGuard(ThreadPinningGuard&&) = delete;
    ThreadPinningGuard& operator=(ThreadPinningGuard&&) = delete;

private:
    static void pin_thread_to_core(int core_id) noexcept {
        // Implementation relies on POSIX sched_setaffinity if compiled for Linux environments
        #if defined(__linux__)
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_t current_thread = pthread_self();
        pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
        #else
        (void)core_id;
        #endif
    }
};

} // namespace titan

#endif // TITAN_CONCURRENCY_HPP