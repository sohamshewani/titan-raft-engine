#ifndef INCLUDE_TITAN_ALLOCATOR_HPP
#define INCLUDE_TITAN_ALLOCATOR_HPP

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <system_error>
#include <atomic>
#include <new>
#include <cstring>

namespace titan {

constexpr std::size_t kCacheLineSize = 64;

enum class AllocatorError : uint8_t {
    Success = 0,
    OutOfMemory,
    InvalidAlignment,
    AllocationLimitExceeded,
    DoubleFree,
    CorruptionDetected
};

class AllocatorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "titan_allocator";
    }

    std::string message(int ev) const override {
        switch (static_cast<AllocatorError>(ev)) {
            case AllocatorError::Success: return "Success";
            case AllocatorError::OutOfMemory: return "Out of memory";
            case AllocatorError::InvalidAlignment: return "Invalid alignment requirement";
            case AllocatorError::AllocationLimitExceeded: return "Allocation limit exceeded";
            case AllocatorError::DoubleFree: return "Double free or corruption detected";
            case AllocatorError::CorruptionDetected: return "Slab metadata corruption detected";
            default: return "Unknown allocator error";
        }
    }
};

inline const std::error_category& get_allocator_category() noexcept {
    static AllocatorCategory instance;
    return instance;
}

inline std::error_code make_error_code(AllocatorError e) noexcept {
    return {static_cast<int>(e), get_allocator_category()};
}

} // namespace titan

namespace std {
template <>
struct is_error_code_enum<titan::AllocatorError> : true_type {};
}

namespace titan {

class PinnedMemorySlabAllocator {
public:
    struct Metrics {
        std::atomic<std::size_t> total_allocated_bytes{0};
        std::atomic<std::size_t> active_allocations{0};
        std::atomic<std::size_t> peak_allocated_bytes{0};
    };

    PinnedMemorySlabAllocator(std::size_t block_size, std::size_t block_count)
        : block_size_(align_up(block_size, kCacheLineSize)),
          block_count_(block_count),
          total_size_(block_size_ * block_count_ + kCacheLineSize),
          pool_memory_(nullptr),
          free_list_head_(nullptr) {
        
        if (block_size_ == 0 || block_count_ == 0) {
            throw std::system_error(make_error_code(AllocatorError::InvalidAlignment));
        }

        int ret = posix_memalign(&pool_memory_, kCacheLineSize, total_size_);
        if (ret != 0 || pool_memory_ == nullptr) {
            throw std::system_error(make_error_code(AllocatorError::OutOfMemory));
        }

        std::memset(pool_memory_, 0, total_size_);

        // Pin memory to prevent paging to swap, critical for predictable consensus latencies
        // If mlock fails due to limits, we log/throw or handle based on strictness.
        // For production safety, we attempt mlock but check status.
        (void)mlock(pool_memory_, total_size_);

        initialize_free_list();
    }

    ~PinnedMemorySlabAllocator() {
        if (pool_memory_ != nullptr) {
            munlock(pool_memory_, total_size_);
            std::free(pool_memory_);
            pool_memory_ = nullptr;
        }
    }

    PinnedMemorySlabAllocator(const PinnedMemorySlabAllocator&) = delete;
    PinnedMemorySlabAllocator& operator=(const PinnedMemorySlabAllocator&) = delete;
    PinnedMemorySlabAllocator(PinnedMemorySlabAllocator&&) noexcept = delete;
    PinnedMemorySlabAllocator& operator=(PinnedMemorySlabAllocator&&) noexcept = delete;

    void* allocate(std::size_t size, std::size_t alignment = kCacheLineSize) {
        if (size > block_size_ - sizeof(SlabHeader)) {
            return nullptr;
        }
        if (alignment > kCacheLineSize) {
            return nullptr;
        }

        FreeNode* node = free_list_head_.load(std::memory_order_acquire);
        while (node != nullptr) {
            FreeNode* next = node->next;
            if (free_list_head_.compare_exchange_weak(node, next,
                                                      std::memory_order_release,
                                                      std::memory_order_relaxed)) {
                
                unsigned char* block_start = reinterpret_cast<unsigned char*>(node);
                SlabHeader* header = reinterpret_cast<SlabHeader*>(block_start);
                header->magic = kMagicActive;
                header->block_size = block_size_;

                void* user_ptr = static_cast<void*>(block_start + sizeof(SlabHeader));

                std::size_t current_active = metrics_.active_allocations.fetch_add(1, std::memory_order_relaxed) + 1;
                std::size_t current_bytes = metrics_.total_allocated_bytes.fetch_add(block_size_, std::memory_order_relaxed) + block_size_;
                
                std::size_t peak = metrics_.peak_allocated_bytes.load(std::memory_order_relaxed);
                while (current_bytes > peak && !metrics_.peak_allocated_bytes.compare_exchange_weak(peak, current_bytes, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    // spin update peak
                }

                return user_ptr;
            }
            node = free_list_head_.load(std::memory_order_acquire);
        }

        return nullptr;
    }

    void deallocate(void* ptr) noexcept {
        if (ptr == nullptr) {
            return;
        }

        unsigned char* block_start = static_cast<unsigned char*>(ptr) - sizeof(SlabHeader);
        SlabHeader* header = reinterpret_cast<SlabHeader*>(block_start);

        if (header->magic != kMagicActive) {
            // Double free or memory corruption
            std::abort();
        }

        header->magic = kMagicFree;

        FreeNode* new_node = reinterpret_cast<FreeNode*>(block_start);
        
        FreeNode* old_head = free_list_head_.load(std::memory_order_relaxed);
        do {
            new_node->next = old_head;
        } while (!free_list_head_.compare_exchange_weak(old_head, new_node,
                                                        std::memory_order_release,
                                                        std::memory_order_relaxed));

        metrics_.active_allocations.fetch_sub(1, std::memory_order_relaxed);
        metrics_.total_allocated_bytes.fetch_sub(block_size_, std::memory_order_relaxed);
    }

    const Metrics& get_metrics() const noexcept {
        return metrics_;
    }

    std::size_t block_size() const noexcept {
        return block_size_;
    }

    std::size_t block_count() const noexcept {
        return block_count_;
    }

private:
    struct SlabHeader {
        uint32_t magic;
        uint32_t block_size;
    };

    struct FreeNode {
        FreeNode* next;
    };

    static constexpr uint32_t kMagicActive = 0x54495441; // "TITA"
    static constexpr uint32_t kMagicFree   = 0x46524545; // "FREE"

    std::size_t align_up(std::size_t val, std::size_t alignment) noexcept {
        return (val + alignment - 1) & ~(alignment - 1);
    }

    void initialize_free_list() {
        unsigned char* current = reinterpret_cast<unsigned char*>(pool_memory_);
        FreeNode* first_node = reinterpret_cast<FreeNode*>(current);
        
        for (std::size_t i = 0; i < block_count_; ++i) {
            FreeNode* node = reinterpret_cast<FreeNode*>(current);
            if (i < block_count_ - 1) {
                node->next = reinterpret_cast<FreeNode*>(current + block_size_);
            } else {
                node->next = nullptr;
            }
            current += block_size_;
        }

        free_list_head_.store(first_node, std::memory_order_release);
    }

    std::size_t block_size_;
    std::size_t block_count_;
    std::size_t total_size_;
    void* pool_memory_;
    std::atomic<FreeNode*> free_list_head_;
    Metrics metrics_;
};

template <typename T>
class CacheLineAlignedAllocator {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template <typename U>
    struct rebind {
        using other = CacheLineAlignedAllocator<U>;
    };

    CacheLineAlignedAllocator() noexcept = default;

    template <typename U>
    CacheLineAlignedAllocator(const CacheLineAlignedAllocator<U>&) noexcept {}

    pointer allocate(size_type n) {
        if (n > std::allocator_traits<std::allocator<T>>::max_size(*this)) {
            throw std::bad_array_new_length();
        }

        std::size_t bytes = n * sizeof(T);
        std::size_t alignment = kCacheLineSize;
        if (alignment < alignof(T)) {
            alignment = alignof(T);
        }

        void* ptr = nullptr;
        int ret = posix_memalign(&ptr, alignment, bytes);
        if (ret != 0 || ptr == nullptr) {
            throw std::bad_alloc();
        }

        return static_cast<pointer>(ptr);
    }

    void deallocate(pointer p, size_type) noexcept {
        std::free(static_cast<void*>(p));
    }

    template <typename U>
    bool operator==(const CacheLineAlignedAllocator<U>&) const noexcept {
        return true;
    }

    template <typename U>
    bool operator!=(const CacheLineAlignedAllocator<U>&) const noexcept {
        return false;
    }
};

} // namespace titan

#endif // INCLUDE_TITAN_ALLOCATOR_HPP