// thrower_rr.cpp - Basic Round Robin Job Thrower
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <random>
#include <condition_variable>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include "json.hpp"

using json = nlohmann::json;

#define TOTAL_PEERS 3

enum NetError {
    NET_OK = 0,
    NET_ERR_SYS = -1,
    NET_ERR_PEER_CLOSED = -2,
    NET_ERR_PROTOCOL = -3
};

ssize_t recv_all(int descriptor, void* buffer, size_t expected_length) {
    size_t collected = 0;
    char* buf = static_cast<char*>(buffer);
    while (collected < expected_length) {
        ssize_t n = recv(descriptor, buf + collected, expected_length - collected, 0);
        if (n == 0) return NET_ERR_PEER_CLOSED;
        if (n < 0) {
            if (errno == EINTR) continue;
            return NET_ERR_SYS;
        }
        collected += n;
    }
    return static_cast<ssize_t>(collected);
}

ssize_t send_all(int descriptor, const void* buffer, size_t to_send) {
    size_t sent = 0;
    const char* buf = static_cast<const char*>(buffer);
    while (sent < to_send) {
        ssize_t n = send(descriptor, buf + sent, to_send - sent, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return NET_ERR_SYS;
        }
        sent += n;
    }
    return static_cast<ssize_t>(sent);
}

std::pair<bool, int> send_string(int descriptor, const std::string& msg) {
    uint32_t len = msg.size();
    uint32_t len_net = htonl(len);
    ssize_t n = send_all(descriptor, &len_net, sizeof(len_net));
    if (n < 0) return {false, static_cast<int>(n)};
    n = send_all(descriptor, msg.data(), len);
    if (n < 0) return {false, static_cast<int>(n)};
    return {true, NET_OK};
}

std::pair<bool, int> recv_string(int descriptor, std::string& msg) {
    uint32_t len_net;
    ssize_t n = recv_all(descriptor, &len_net, sizeof(len_net));
    if (n == NET_ERR_PEER_CLOSED) return {false, NET_ERR_PEER_CLOSED};
    if (n < 0) return {false, static_cast<int>(n)};
    uint32_t len = ntohl(len_net);
    if (len > 10 * 1024 * 1024) return {false, NET_ERR_PROTOCOL};
    msg.resize(len);
    n = recv_all(descriptor, msg.data(), len);
    if (n == NET_ERR_PEER_CLOSED) return {false, NET_ERR_PEER_CLOSED};
    if (n < 0) return {false, static_cast<int>(n)};
    return {true, NET_OK};
}

struct PeerInfo {
    int peer_id;
    int socket_fd;
    bool connected;
    bool ready;
    std::mutex socket_mutex;
    
    PeerInfo() : peer_id(-1), socket_fd(-1), connected(false), ready(false) {}
};

class RoundRobinThrower {
private:
    int server_port;
    int server_socket;
    std::vector<PeerInfo*> peers;  // index 0 = peer 1, index 1 = peer 2, etc.
    std::mutex peers_mutex;
    std::atomic<bool> running;
    std::atomic<uint64_t> job_counter;
    std::atomic<int> peers_ready;
    std::atomic<int> current_peer_index;
    std::condition_variable cv_all_ready;
    std::mutex ready_mutex;
    


    std::mt19937 rng;
    std::uniform_int_distribution<int> cpu_dist;
    std::uniform_int_distribution<int> mem_dist;
    std::uniform_int_distribution<int> duration_dist;
    
public:
    RoundRobinThrower(int port) 
        : server_port(port), server_socket(-1), running(false), job_counter(0), 
          peers_ready(0), current_peer_index(0),
          rng(std::random_device{}()),
          cpu_dist(50, 150),
          mem_dist(200, 800),
          duration_dist(3, 7)
    {
        // Initialize peer slots
        for (int i = 0; i < TOTAL_PEERS; i++) {
            peers.push_back(new PeerInfo());
        }
    }
    
    ~RoundRobinThrower() {
        shutdown();
        for (auto peer : peers) {
            delete peer;
        }
    }
    
    bool start() {
        server_socket = socket(AF_INET, SOCK_STREAM, 0);
        if (server_socket < 0) {
            std::cerr << "Failed to create socket" << std::endl;
            return false;
        }
        
        int opt = 1;
        setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        
        sockaddr_in server_addr;
        std::memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(server_port);
        
        if (bind(server_socket, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            std::cerr << "Bind failed" << std::endl;
            close(server_socket);
            return false;
        }
        
        if (listen(server_socket, 10) < 0) {
            std::cerr << "Listen failed" << std::endl;
            close(server_socket);
            return false;
        }
        
        running = true;
        std::cout << "=== ROUND ROBIN Job Thrower Started on port " << server_port << " ===" << std::endl;
        return true;
    }
    
    void accept_peers() {
        while (running and peers_ready<TOTAL_PEERS) {
            sockaddr_in peer_addr;
            socklen_t addr_len = sizeof(peer_addr);
            int peer_socket = accept(server_socket, (sockaddr*)&peer_addr, &addr_len);
            
            if (peer_socket < 0) {
                if (running) std::cerr << "Accept failed" << std::endl;
                continue;
            }
            
            std::thread handler(&RoundRobinThrower::handle_peer_registration, this, peer_socket);
            handler.detach();
        }
        
    }
    
    void handle_peer_registration(int socket_fd) {
        std::string msg;
        auto [success, err] = recv_string(socket_fd, msg);
        
        if (!success) {
            close(socket_fd);
            return;
        }
        
        try {
            json reg_msg = json::parse(msg);
            
            if (reg_msg["type"] == "register") {
                int peer_id = reg_msg["peer_id"];
                
                if (peer_id < 1 || peer_id > TOTAL_PEERS) {
                    close(socket_fd);
                    return;
                }
                
                std::lock_guard<std::mutex> lock(peers_mutex);
                int idx = peer_id - 1;
                peers[idx]->peer_id = peer_id;
                peers[idx]->socket_fd = socket_fd;
                peers[idx]->connected = true;
                
                std::cout << "Peer " << peer_id << " connected" << std::endl;
                
                std::thread response_handler(&RoundRobinThrower::handle_peer_responses, this, idx);
                response_handler.detach();
            }
        } catch (const std::exception& e) {
            close(socket_fd);
        }
    }
    
    void handle_peer_responses(int peer_idx) {
        PeerInfo* peer = peers[peer_idx];
        
        while (running && peer->connected) {
            std::string msg;
            auto [success, err] = recv_string(peer->socket_fd, msg);
            
            if (!success) {
                std::cout << "Peer " << peer->peer_id << " disconnected" << std::endl;
                peer->connected = false;
                close(peer->socket_fd);
                break;
            }
            
            try {
                json response = json::parse(msg);
                
                if (response["type"] == "jobs_allowed") {
                    peer->ready = true;
                    {
                        std::lock_guard<std::mutex> lock(ready_mutex);
                        peers_ready++;
                    }
                    cv_all_ready.notify_all();
                    std::cout << "Peer " << peer->peer_id << " ready" << std::endl;
                } else if (response["type"] == "job_completed") {
                    std::cout << "Job " << response["job_id"] << " completed by Peer " << peer->peer_id << std::endl;
                } else if (response["type"] == "job_rejected") {
                    std::cout << "Job " << response["job_id"] << " REJECTED by Peer " << peer->peer_id 
                              << " (Reason: " << response["reason"] << ")" << std::endl;
                }
            } catch (const std::exception& e) {
            }
        }
    }
    
    void generate_and_send_jobs() {
        std::cout << "\n=== Starting ROUND ROBIN job distribution ===" << std::endl;
        
        while (running) {
            json job = create_job();
            
            // ROUND ROBIN: Get next peer in sequence
            int target_peer_idx = get_random_peer();
            
            if (target_peer_idx != -1) {
                send_job_to_peer(target_peer_idx, job);
            } else {
                std::cerr << "No peers available!" << std::endl;
            }
            
            // Wait before next job
            std::uniform_int_distribution<int> wait_dist(800, 1500);
            std::this_thread::sleep_for(std::chrono::milliseconds(wait_dist(rng)));
        }
    }
    
    json create_job() {
        json job;
        job["job_id"] = ++job_counter;
        job["type"] = "job";
        job["cpu_required"] = cpu_dist(rng);
        job["memory_required"] = mem_dist(rng);
        job["estimated_duration"] = duration_dist(rng);
        job["timestamp"] = std::chrono::system_clock::now().time_since_epoch().count();
        return job;
    }
    
    int get_next_peer_round_robin() {
        std::lock_guard<std::mutex> lock(peers_mutex);
        
        int start_idx = current_peer_index;
        
        // Find next connected and ready peer
        do {
            if (peers[current_peer_index]->connected && peers[current_peer_index]->ready) {
                int selected = current_peer_index;
                current_peer_index = (current_peer_index + 1) % TOTAL_PEERS;
                return selected;
            }
            current_peer_index = (current_peer_index + 1) % TOTAL_PEERS;
        } while (current_peer_index != start_idx);
        
        return -1;  // No peers available
    }
    

    int get_random_peer() {
        std::lock_guard<std::mutex> lock(peers_mutex);
        
        // Collect all connected and ready peers
        std::vector<int> available_peers;
        for (int i = 0; i < TOTAL_PEERS; i++) {
            if (peers[i]->connected && peers[i]->ready) {
                available_peers.push_back(i);
            }
        }
        
        // If no peers available
        if (available_peers.empty()) {
            return -1;
        }
        
        // Select random peer from available ones
        std::uniform_int_distribution<size_t> dist(0, available_peers.size() - 1);
        return available_peers[dist(rng)];
    }

    bool send_job_to_peer(int peer_idx, const json& job) {
        std::lock_guard<std::mutex> lock(peers_mutex);
        
        PeerInfo* peer = peers[peer_idx];
        if (!peer->connected) return false;
        
        std::lock_guard<std::mutex> socket_lock(peer->socket_mutex);
        
        std::string job_str = job.dump();
        auto [success, err] = send_string(peer->socket_fd, job_str);
        
        if (success) {
            std::cout << "[RR] Job " << job["job_id"] << " → Peer " << peer->peer_id << std::endl;
            return true;
        }
        return false;
    }
    
    void shutdown() {
        if (!running) return;
        running = false;
        
        if (server_socket >= 0) {
            close(server_socket);
        }
        
        std::lock_guard<std::mutex> lock(peers_mutex);
        for (auto peer : peers) {
            if (peer->connected && peer->socket_fd >= 0) {
                close(peer->socket_fd);
                peer->connected = false;
            }
        }
    }
    
    void print_statistics() {
        std::lock_guard<std::mutex> lock(peers_mutex);
        std::cout << "\n=== ROUND ROBIN Statistics ===" << std::endl;
        std::cout << "Total Jobs Sent: " << job_counter << std::endl;
        std::cout << "Connected Peers: ";
        for (auto peer : peers) {
            if (peer->connected) {
                std::cout << peer->peer_id << " ";
            }
        }
        std::cout << std::endl;
    }
    
    void run() {
        if (!start()) return;
        
        std::thread accept_thread(&RoundRobinThrower::accept_peers, this);
        accept_thread.detach();
        
        std::unique_lock<std::mutex> lck(ready_mutex);
        cv_all_ready.wait(lck, [this]{ return peers_ready == TOTAL_PEERS; });
        
        std::cout << "\nAll " << TOTAL_PEERS << " peers ready!" << std::endl;
        
        std::thread job_thread(&RoundRobinThrower::generate_and_send_jobs, this);
        job_thread.detach();
        
        std::cout << "\nPress Enter for statistics (type 'quit' to exit)..." << std::endl;
        std::string input;
        while (std::getline(std::cin, input)) {
            if (input == "quit") break;
            print_statistics();
        }
    }
};

int main(int argc, char* argv[]) {
    int port = 9000;
    if (argc > 1) port = std::atoi(argv[1]);
    
    RoundRobinThrower server(port);
    server.run();
    
    return 0;
}