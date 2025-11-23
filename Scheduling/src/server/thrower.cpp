// thrower.cpp - Job Thrower Server
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
#include "../../include/json.hpp"
#include<fstream>
using json = nlohmann::json;

#define TOTAL_PEERS 3

// Network utility functions
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

// Peer information structure
struct PeerInfo {
    int peer_id;
    std::string ip;
    int port;
    int socket_fd;
    bool connected;
    bool ready;
    std::mutex socket_mutex;
    
    PeerInfo(int id, std::string ip_addr, int p) 
        : peer_id(id), ip(ip_addr), port(p), socket_fd(-1), connected(false), ready(false) {}
};

// Job Thrower Server Class
class JobThrowerServer {
private:
    std::ofstream file;
    int server_port;
    int server_socket;
    std::map<int, PeerInfo*> peers;
    std::mutex peers_mutex;
    std::atomic<bool> running;
    std::atomic<uint64_t> job_counter;
    std::atomic<int> peers_ready;
    std::condition_variable cv_all_ready;
    std::mutex ready_mutex;
    
    std::mt19937 rng;
    std::uniform_int_distribution<int> cpu_dist;
    std::uniform_int_distribution<int> mem_dist;
    std::uniform_int_distribution<int> duration_dist;
    
public:
    JobThrowerServer(int port) 
        : server_port(port), server_socket(-1), running(false), job_counter(0), peers_ready(0),
          rng(std::random_device{}()),
        cpu_dist(100, 250),          // CPU: 100-250 units (VERY HIGH)
        mem_dist(400, 1200),         // Memory: 400-1200 MB (VERY HIGH)
        duration_dist(4, 8)          // Duration: 4-8 seconds (LONGER)

    {
        file.open("logs.txt");
        initialize_peers();
    }
    
    ~JobThrowerServer() {
        shutdown();
        for (auto& pair : peers) {
            delete pair.second;
        }
    }
    
    void initialize_peers() {
        peers[1] = new PeerInfo(1, "127.0.0.1", 8001);
        peers[2] = new PeerInfo(2, "127.0.0.1", 8002);
        peers[3] = new PeerInfo(3, "127.0.0.1", 8003);
    }
    
    bool start() {
        server_socket = socket(AF_INET, SOCK_STREAM, 0);
        if (server_socket < 0) {
            std::cerr << "Failed to create socket" << std::endl;
            return false;
        }
        
        int opt = 1;
        if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            std::cerr << "setsockopt failed" << std::endl;
            close(server_socket);
            return false;
        }
        
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
        std::cout << "Job Thrower Server started on port " << server_port << std::endl;
        return true;
    }
    
    void accept_peers() {
        while (running) {
            sockaddr_in peer_addr;
            socklen_t addr_len = sizeof(peer_addr);
            int peer_socket = accept(server_socket, (sockaddr*)&peer_addr, &addr_len);
            
            if (peer_socket < 0) {
                if (running) {
                    std::cerr << "Accept failed" << std::endl;
                }
                continue;
            }
            
            std::cout << "Peer connected from " 
                      << inet_ntoa(peer_addr.sin_addr) << ":" 
                      << ntohs(peer_addr.sin_port) << std::endl;
            
            std::thread handler(&JobThrowerServer::handle_peer_registration, this, peer_socket);
            handler.detach();
        }
    }
    
    void handle_peer_registration(int socket_fd) {
        std::string msg;
        auto [success, err] = recv_string(socket_fd, msg);
        
        if (!success) {
            std::cerr << "Failed to receive registration from peer" << std::endl;
            close(socket_fd);
            return;
        }
        
        try {
            json reg_msg = json::parse(msg);
            
            if (reg_msg["type"] == "register") {
                int peer_id = reg_msg["peer_id"];
                
                std::lock_guard<std::mutex> lock(peers_mutex);
                if (peers.find(peer_id) != peers.end()) {
                    peers[peer_id]->socket_fd = socket_fd;
                    peers[peer_id]->connected = true;
                    
                    std::cout << "Peer " << peer_id << " registered successfully" << std::endl;
                    
                    std::thread response_handler(&JobThrowerServer::handle_peer_responses, this, peer_id);
                    response_handler.detach();
                } else {
                    std::cerr << "Unknown peer_id: " << peer_id << std::endl;
                    close(socket_fd);
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Error parsing registration: " << e.what() << std::endl;
            close(socket_fd);
        }
    }
    
    void handle_peer_responses(int peer_id) {
        peers_mutex.lock();
        PeerInfo* peer = peers[peer_id];
        peers_mutex.unlock();
        
        while (running && peer->connected) {
            std::string msg;
            auto [success, err] = recv_string(peer->socket_fd, msg);
            
            if (!success) {
                std::cout << "Peer " << peer_id << " disconnected" << std::endl;
                peer->connected = false;
                close(peer->socket_fd);
                peer->socket_fd = -1;
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
                    std::cout << "Peer " << peer_id << " is ready to accept jobs" << std::endl;
                } else if (response["type"] == "job_completed") {
                    file<< "Job " << response["job_id"] << " completed by peer " << peer_id << std::endl;
                }
            } catch (const std::exception& e) {
                std::cerr << "Error parsing response from peer " << peer_id << ": " << e.what() << std::endl;
            }
        }
    }
    
    void generate_and_send_jobs() {
        std::cout << "Job generation thread started" << std::endl;
        
        while (running) {
            json job = create_job();
            int target_peer = select_random_peer();
            
            if (target_peer != -1) {
                send_job_to_peer(target_peer, job);
            } else {
                std::cerr << "No connected peers available" << std::endl;
            }
            
            std::uniform_int_distribution<int> wait_dist(500, 1000)    ;     // 0.5-1 second (VERY FAST!)
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
    
    int select_random_peer() {
        std::lock_guard<std::mutex> lock(peers_mutex);
        
        std::vector<int> ready_peers;
        for (auto& pair : peers) {
            if (pair.second->connected && pair.second->ready) {
                ready_peers.push_back(pair.first);
            }
        }
        
        if (ready_peers.empty()) return -1;
        
        std::uniform_int_distribution<size_t> dist(0, ready_peers.size() - 1);
        return ready_peers[dist(rng)];
    }
    
    bool send_job_to_peer(int peer_id, const json& job) {
        std::lock_guard<std::mutex> lock(peers_mutex);
        
        if (peers.find(peer_id) == peers.end() || !peers[peer_id]->connected) {
            return false;
        }
        
        PeerInfo* peer = peers[peer_id];
        std::lock_guard<std::mutex> socket_lock(peer->socket_mutex);
        
        std::string job_str = job.dump();
        auto [success, err] = send_string(peer->socket_fd, job_str);
        
        if (success) {
            file << "Job " << job["job_id"] << " sent to peer " << peer_id << std::endl;
            return true;
        } else {
            peer->connected = false;
            return false;
        }
    }
    
    void shutdown() {
        if (!running) return;
        running = false;
        
        if (server_socket >= 0) {
            close(server_socket);
        }
        
        std::lock_guard<std::mutex> lock(peers_mutex);
        for (auto& pair : peers) {
            if (pair.second->connected && pair.second->socket_fd >= 0) {
                close(pair.second->socket_fd);
                pair.second->connected = false;
            }
        }
        
        std::cout << "Server shutdown complete" << std::endl;
    }
    
    void print_statistics() {
        std::lock_guard<std::mutex> lock(peers_mutex);
        std::cout << "\n=== Job Thrower Statistics ===" << std::endl;
        std::cout << "Total jobs generated: " << job_counter << std::endl;
        std::cout << "Connected peers: ";
        for (auto& pair : peers) {
            if (pair.second->connected) {
                std::cout << pair.first << " ";
            }
        }
        std::cout << std::endl;
    }
    
    void run() {
        if (!start()) {
            std::cerr << "Failed to start server" << std::endl;
            return;
        }
        
        std::thread accept_thread(&JobThrowerServer::accept_peers, this);
        accept_thread.detach();
        
        std::unique_lock<std::mutex> lck(ready_mutex);
        cv_all_ready.wait(lck, [this]{ return peers_ready == TOTAL_PEERS; });
        
        std::cout << "\nAll peers ready! Starting job generation..." << std::endl;
        
        std::thread job_thread(&JobThrowerServer::generate_and_send_jobs, this);
        job_thread.detach();
        
        std::cout << "Press Enter to view statistics (type 'quit' to exit)..." << std::endl;
        std::string input;
        while (std::getline(std::cin, input)) {
            if (input == "quit") {
                break;
            }
            print_statistics();
        }
    }
};

int main(int argc, char* argv[]) {
    int port = 9000;
    
    if (argc > 1) {
        port = std::atoi(argv[1]);
    }
    
    JobThrowerServer server(port);
    server.run();
    
    return 0;
}