#include "network/rpc_transport.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <system_error>
#include <cstring>
#include <new>

namespace titan {
namespace network {

// Helper to set non-blocking flag on a file descriptor
static int MakeNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        return -1;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

RpcTransport::RpcTransport(uint16_t port, 
                           RingBuffer<RpcMessage>& inbound_queue, 
                           size_t worker_threads)
    : port_(port),
      inbound_queue_(inbound_queue),
      worker_count_(worker_threads == 0 ? 1 : worker_threads),
      server_fd_(-1),
      epoll_fd_(-1),
      running_(false) {
    
    // Allocate cache-line aligned workers
    workers_.reserve(worker_count_);
    for (size_t i = 0; i < worker_count_; ++i) {
        void* ptr = ::operator new(sizeof(WorkerContext), std::align_val_t{64});
        WorkerContext* worker = new (ptr) WorkerContext();
        worker->worker_id = i;
        worker->epoll_fd = -1;
        workers_.push_back(worker);
    }
}

RpcTransport::~RpcTransport() {
    Stop();
    for (auto* worker : workers_) {
        if (worker) {
            worker->~WorkerContext();
            ::operator delete(worker, std::align_val_t{64});
        }
    }
    workers_.clear();
}

bool RpcTransport::Start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return false;
    }

    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == -1) {
        running_.store(false, std::memory_order_release);
        return false;
    }

    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(server_fd_);
        server_fd_ = -1;
        running_.store(false, std::memory_order_release);
        return false;
    }

    if (setsockopt(server_fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
        close(server_fd_);
        server_fd_ = -1;
        running_.store(false, std::memory_order_release);
        return false;
    }

    if (MakeNonBlocking(server_fd_) < 0) {
        close(server_fd_);
        server_fd_ = -1;
        running_.store(false, std::memory_order_release);
        return false;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);

    if (bind(server_fd_, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) < 0) {
        close(server_fd_);
        server_fd_ = -1;
        running_.store(false, std::memory_order_release);
        return false;
    }

    if (listen(server_fd_, SOMAXCONN) < 0) {
        close(server_fd_);
        server_fd_ = -1;
        running_.store(false, std::memory_order_release);
        return false;
    }

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == -1) {
        close(server_fd_);
        server_fd_ = -1;
        running_.store(false, std::memory_order_release);
        return false;
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = server_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev) < 0) {
        close(epoll_fd_);
        close(server_fd_);
        epoll_fd_ = -1;
        server_fd_ = -1;
        running_.store(false, std::memory_order_release);
        return false;
    }

    // Initialize worker epoll instances
    for (size_t i = 0; i < worker_count_; ++i) {
        workers_[i]->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
        if (workers_[i]->epoll_fd == -1) {
            Stop();
            return false;
        }
        workers_[i]->running = true;
        workers_[i]->thread = std::thread(&RpcTransport::WorkerLoop, this, workers_[i]);
    }

    // Start main accept thread
    accept_thread_ = std::thread(&RpcTransport::AcceptLoop, this);

    return true;
}

void RpcTransport::Stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        return;
    }

    if (server_fd_ != -1) {
        shutdown(server_fd_, SHUT_RDWR);
        close(server_fd_);
        server_fd_ = -1;
    }

    if (epoll_fd_ != -1) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    for (auto* worker : workers_) {
        if (worker) {
            worker->running = false;
            if (worker->epoll_fd != -1) {
                close(worker->epoll_fd);
                worker->epoll_fd = -1;
            }
            if (worker->thread.joinable()) {
                worker->thread.join();
            }
        }
    }
}

void RpcTransport::AcceptLoop() {
    constexpr int kMaxEvents = 64;
    epoll_event events[kMaxEvents];

    while (running_.load(std::memory_order_acquire)) {
        int n = epoll_wait(epoll_fd_, events, kMaxEvents, 100);
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == server_fd_) {
                while (true) {
                    sockaddr_in client_addr{};
                    socklen_t client_len = sizeof(client_addr);
                    int client_fd = accept4(server_fd_, 
                                            reinterpret_cast<struct sockaddr*>(&client_addr), 
                                            &client_len, 
                                            SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        }
                        break;
                    }

                    int opt = 1;
                    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

                    // Round-robin dispatch to workers
                    size_t target_worker = next_worker_.fetch_add(1, std::memory_order_relaxed) % worker_count_;
                    WorkerContext* worker = workers_[target_worker];

                    {
                        std::lock_guard<std::mutex> lock(worker->mutex);
                        epoll_event ev{};
                        ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
                        ev.data.fd = client_fd;
                        if (epoll_ctl(worker->epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
                            close(client_fd);
                        } else {
                            worker->client_fds.insert(client_fd);
                        }
                    }
                }
            }
        }
    }
}

void RpcTransport::WorkerLoop(WorkerContext* worker) {
    constexpr int kMaxEvents = 64;
    epoll_event events[kMaxEvents];
    
    // Allocate connection states mapping
    std::unordered_map<int, ConnectionState> connections;

    while (worker->running.load(std::memory_order_acquire)) {
        int n = epoll_wait(worker->epoll_fd, events, kMaxEvents, 100);
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            uint32_t flags = events[i].events;

            if ((flags & EPOLLERR) || (flags & EPOLLHUP) || !(flags & EPOLLIN)) {
                CloseConnection(worker, fd, connections);
                continue;
            }

            auto& conn = connections[fd];
            bool closed = false;

            while (true) {
                // If we need header space
                if (conn.bytes_read < sizeof(RpcHeader)) {
                    size_t to_read = sizeof(RpcHeader) - conn.bytes_read;
                    ssize_t nread = read(fd, reinterpret_cast<char*>(&conn.header) + conn.bytes_read, to_read);
                    if (nread < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        }
                        closed = true;
                        break;
                    } else if (nread == 0) {
                        closed = true;
                        break;
                    }
                    conn.bytes_read += nread;
                }

                // If header is fully read, allocate or check payload
                if (conn.bytes_read >= sizeof(RpcHeader)) {
                    if (conn.header.payload_length > kMaxPayloadSize) {
                        closed = true;
                        break;
                    }

                    if (!conn.payload_buffer) {
                        conn.payload_buffer = std::make_unique<uint8_t[]>(conn.header.payload_length);
                        conn.payload_read = 0;
                    }

                    size_t payload_target = conn.header.payload_length;
                    if (conn.payload_read < payload_target) {
                        size_t to_read = payload_target - conn.payload_read;
                        ssize_t nread = read(fd, conn.payload_buffer.get() + conn.payload_read, to_read);
                        if (nread < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            closed = true;
                            break;
                        } else if (nread == 0) {
                            closed = true;
                            break;
                        }
                        conn.payload_read += nread;
                    }

                    // If full message is read, push to MPSC ring buffer
                    if (conn.payload_read == payload_target) {
                        RpcMessage msg;
                        msg.type = conn.header.type;
                        msg.term = conn.header.term;
                        msg.sender_id = conn.header.sender_id;
                        msg.payload.assign(
                            reinterpret_cast<const char*>(conn.payload_buffer.get()), 
                            conn.header.payload_length
                        );

                        // Push to lock-free MPSC inbound queue
                        while (!inbound_queue_.Push(std::move(msg))) {
                            // Spin if bounded queue is full, backpressure mechanism
                            if (!worker->running.load(std::memory_order_acquire)) {
                                break;
                            }
                            sched_yield();
                        }

                        // Reset connection state for next pipelined message
                        conn.bytes_read = 0;
                        conn.payload_read = 0;
                        conn.payload_buffer.reset();
                    }
                }
            }

            if (closed) {
                CloseConnection(worker, fd, connections);
            } else {
                // Re-arm epoll oneshot
                std::lock_guard<std::mutex> lock(worker->mutex);
                epoll_event ev{};
                ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
                ev.data.fd = fd;
                epoll_ctl(worker->epoll_fd, EPOLL_CTL_MOD, fd, &ev);
            }
        }
    }

    // Cleanup remaining connections on thread exit
    for (auto& pair : connections) {
        close(pair.first);
    }
    connections.clear();
}

void RpcTransport::CloseConnection(WorkerContext* worker, int fd, std::unordered_map<int, ConnectionState>& connections) {
    std::lock_guard<std::mutex> lock(worker->mutex);
    epoll_ctl(worker->epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    worker->client_fds.erase(fd);
    connections.erase(fd);
}

} // namespace network
} // namespace titan