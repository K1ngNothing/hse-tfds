#include <netinet/in.h>
#include <unistd.h>

#include <iostream>
#include <thread>

#include "task_info.h"

constexpr int BUFFER_SIZE = 1024;

// -------------------------------------------------------------------------------------------------
// ----- Integral calculation -----
// -------------------------------------------------------------------------------------------------

// Function to integrate
double f(const double x) {
    return (2*x*x - 3*x + 11) / (x*x + 1);
}

double integrate_seg(const size_t seg_num) {
    double lb = SEG_LEN * seg_num;
    double rb = SEG_LEN * (seg_num + 1);
    std::cerr << "Integrating segment " << seg_num << ": [" << lb << ", " << rb << "]\n";
    std::this_thread::sleep_for(std::chrono::seconds{3});

    double result = 0.0;
    double x = lb;
    for (size_t i = 0; i < STEPS_PER_SEG; ++i) {
        result += f(x) * STEP_LEN;
        x += STEP_LEN;
    }
    std::cerr << "Result: " << result << "\n";
    return result;
}

// -------------------------------------------------------------------------------------------------
// ----- Communication with master -----
// -------------------------------------------------------------------------------------------------

// Wait for discover message from master
sockaddr_in receive_broadcast() {
    std::cerr << "Waiting for broadcast\n";

    const int udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_sock < 0) {
        perror("socket error");
        exit(1);
    }

    int reuse = 1;
    if (setsockopt(udp_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt error");
        exit(1);
    }

    sockaddr_in broadcast_addr{
        .sin_family = AF_INET,
        .sin_port = htons(BROADCAST_PORT),
        .sin_addr = in_addr{.s_addr = htonl(INADDR_BROADCAST)}
    };
    if (bind(udp_sock, (sockaddr*)&broadcast_addr, sizeof(broadcast_addr)) < 0) {
        perror("bind error");
        exit(1);
    }

    char buffer[BUFFER_SIZE];
    sockaddr_in master_addr{};
    socklen_t master_len = sizeof(master_addr);
    while (true) {
        ssize_t bytes_received = recvfrom(udp_sock, buffer, BUFFER_SIZE, 0, (sockaddr*)&master_addr, &master_len);
        if (bytes_received < 0) {
            perror("recvfrom error");
            exit(1);
        }
        if (bytes_received >= 0) {
            buffer[bytes_received] = '\0';
            if (std::string(buffer) == DISCOVER_MSG) {
                std::cerr << "Received broadcast from master!\n";
                close(udp_sock);
                return master_addr;
            }
        }
    }
}

// Communicate with master through TCP and do assigned work
void communicate_with_master(sockaddr_in master_addr) {
    // Change port in master's address
    master_addr.sin_port = htons(TCP_PORT);

    auto on_connection_error = [](int tcp_sock) {
        close(tcp_sock);
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    };

    while (true) {
        const int tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (tcp_sock < 0) {
            perror("socket error");
            exit(1);
        }

        std::cerr << "Trying to connect to master\n";
        if (connect(tcp_sock, (sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
            if (errno == ECONNREFUSED) {
                std::cerr << "All master's tasks done!\n";
                exit(0);
            }
            perror("Failed to connect to master.");
            on_connection_error(tcp_sock);
            continue;
        }

        // Receive tasks from master
        while (true) {
            size_t seg_num;
            int read_result = read(tcp_sock, &seg_num, sizeof(seg_num));
            if (read_result == 0) {
                std::cerr << "All master's tasks done!\n";
                exit(0);
            }
            if (read_result < 0 || read_result != sizeof(seg_num)) {
                perror("Failed to read task.");
                on_connection_error(tcp_sock);
                break;
            }
            // std::cerr << "Got response from master!\n";
            const SegResult result{
                .seg_num = seg_num,
                .result = integrate_seg(seg_num)
            };
            if (write(tcp_sock, &result, sizeof(result)) < 0) {
                perror("Failed to send result to master.");
                on_connection_error(tcp_sock);
            }
        }

    }
}

int main() {
    std::cerr << "Worker started\n";
    const sockaddr_in master_addr = receive_broadcast();
    communicate_with_master(master_addr);
}
