#include <arpa/inet.h>
#include <assert.h>
#include <string.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <array>
#include <iostream>
#include <thread>

#include "task_info.h"

using TimePoint = std::chrono::steady_clock::time_point;

constexpr int BUFFER_SIZE = 1024;

struct TaskInfo {
    bool done = false;
    bool assigned = false;
    TimePoint lastAssigned;
};

// -------------------------------------------------------------------------------------------------
// ----- Broadcast discovery -----
// -------------------------------------------------------------------------------------------------

void do_broadcast() {
    int udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_sock < 0) {
        perror("Broadcast discovery: socket error");
        return;
    }

    int bcast_enable = 1;
    if (setsockopt(udp_sock, SOL_SOCKET, SO_BROADCAST, &bcast_enable, sizeof(bcast_enable)) < 0) {
        perror("Broadcast discovery: setsockopt error");
        return;
    }

    const char* message = "DISCOVER";
    sockaddr_in bcast_addr{
        .sin_family = AF_INET,
        .sin_port = htons(BROADCAST_PORT),
        .sin_addr = in_addr{.s_addr = htonl(INADDR_BROADCAST)}
    };
    if (sendto(udp_sock, message, strlen(message), 0, (sockaddr*)&bcast_addr, sizeof(bcast_addr)) < 0) {
        perror("Broadcast discovery: sendto error");
        return;
    }

    close(udp_sock);
}

void broadcast_discovery() {
    while (true) {
        do_broadcast();
        std::this_thread::sleep_for(std::chrono::seconds{1});
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Handle connections with workers -----
// -------------------------------------------------------------------------------------------------

void assign_task(int worker_sock, std::array<TaskInfo, SEG_COUNT>& tasks_info) {
    TimePoint now = std::chrono::steady_clock::now();
    size_t seg_id = 0;
    for (; seg_id < SEG_COUNT; ++seg_id) {
        TaskInfo& info = tasks_info[seg_id];

        if (!info.done && (!info.assigned || 
            std::chrono::duration_cast<std::chrono::seconds>(now - info.lastAssigned).count() > 4))
        {
            // Found task to assign
            break;
        }
    }
    if (seg_id == SEG_COUNT) {
        return;
    }

    if (write(worker_sock, &seg_id, sizeof(seg_id)) < 0) {
        perror("Failed to send task to worker");
        close(worker_sock);
        return;
    }
    tasks_info[seg_id].assigned = true;
    tasks_info[seg_id].lastAssigned = now;
    // std::cerr << "Assigned work: segment " << seg_id << "\n";
}

void solve() {
    // Setup TCP socket
    int tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_sock < 0) {
        perror("socket error");
        exit(1);
    }
    int reuse = 1;
    if (setsockopt(tcp_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt error");
        exit(1);
    }
    sockaddr_in server_addr{
        .sin_family = AF_INET,
        .sin_port = htons(TCP_PORT),
        .sin_addr = in_addr{.s_addr = INADDR_ANY}
    };
    if (bind(tcp_sock, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind error");
        exit(1);
    }
    if (listen(tcp_sock, SOMAXCONN) < 0) {
        perror("listen error");
        exit(1);
    }

    // Setup epoll
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1 error");
        exit(1);
    }
    epoll_event server_event{
        .events = EPOLLIN,
        .data = {.fd = tcp_sock}
    };
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, tcp_sock, &server_event) < 0) {
        perror("epoll_ctl error");
        exit(1);
    }
    epoll_event events[BUFFER_SIZE];

    // Solve task
    std::array<TaskInfo, SEG_COUNT> tasks_info;
    size_t segments_done = 0;
    double result = 0.0;
    while (segments_done < SEG_COUNT) {
        // std::cerr << "Waiting for epoll events\n";
        int event_count = epoll_wait(epoll_fd, events, BUFFER_SIZE, -1);
        if (event_count < 0) {
            perror("epoll_wait error");
            exit(1);
        }

        for (int i = 0; i < event_count; ++i) {
            if (events[i].data.fd == tcp_sock) {
                // std::cerr << "New worker!\n";

                int worker_sock = accept(tcp_sock, nullptr, nullptr);
                if (worker_sock < 0) {
                    perror("Failed to accept connection");
                    continue;
                }

                // Add worker's socket to epoll
                epoll_event client_event{
                    .events = EPOLLIN,
                    .data = {.fd = worker_sock}
                };
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, worker_sock, &client_event) < 0) {
                    perror("Failed to add worker socket to epoll");
                    close(worker_sock);
                    continue;
                }

                // Find not completed task for worker
                assign_task(worker_sock, tasks_info);
            } else {
                // Worker sent result
                int worker_sock = events[i].data.fd;
                SegResult worker_msg;
                ssize_t bytes_read = read(worker_sock, &worker_msg, sizeof(worker_msg));
                if (bytes_read <= 0) {
                    if (bytes_read < 0) {
                        perror("read error");
                    }
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, worker_sock, nullptr);
                    close(worker_sock);
                    continue;
                }
                size_t seg_num = worker_msg.seg_num;
                const double seg_result = worker_msg.result;

                if (!tasks_info[seg_num].done) {
                    std::cerr << "Got result for segment " << seg_num << ": " << seg_result << "\n";
                    result += seg_result;
                    tasks_info[seg_num].done = true;
                    ++segments_done;
                } else {
                    // std::cerr << "Got duplicate result for segment\n";
                }

                assign_task(worker_sock, tasks_info);
            }
        }
    }

    std::cerr << "Calculated integral: " << result << "\n";
    close(tcp_sock);
    close(epoll_fd);
}

int main() {
    // std::cerr << "Sleep for small amount of time...\n";
    // std::this_thread::sleep_for(std::chrono::seconds{5});
    std::cerr << "Get to work!\n";

    std::thread discovery_thread(broadcast_discovery);
    discovery_thread.detach();  // will finish at the end of the program
    std::thread connections_thread(solve);
    connections_thread.join();
    return 0;
}
