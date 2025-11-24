// peer.cpp - Peer Node Implementation
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <queue>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <random>
#include <algorithm>
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
#define JOB_THROWER_IP "127.0.0.1"
#define JOB_THROWER_PORT 9000

#define ACCEPT_THRESHOLD 0.65
#define GOSSIP_INTERVAL_MS 1500
#define GOSSIP_FANOUT 2
#define STALE_THRESHOLD_MS 12000

#define CPU_CAPACITY 1000
#define MEMORY_CAPACITY 4096

#define ALPHA 0.6
#define BETA 0.4

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

std::pair<double, double> mean_and_stddev(const std::vector<double>& a) {
    if (a.empty()) {
        return {0.0, 0.0};  // or throw exception if preferred
    }

    double sum = 0.0;
    for (double x : a) sum += x;

    double mean = sum / a.size();

    double sq_sum = 0.0;
    for (double x : a) {
        sq_sum += (x - mean) * (x - mean);
    }

    double stddev = std::sqrt(sq_sum / a.size());  // population stddev

    return {mean, stddev};
}

int connect_to_server(const std::string &ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        return -1;
    }
    
    sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }
    
    if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(sock);
        return -1;
    }
    
    return sock;
}

struct PeerNetworkInfo {
    int peer_id;
    std::string ip;
    int port;
};

PeerNetworkInfo all_peers[TOTAL_PEERS] = {
    {1, "127.0.0.1", 8001},
    {2, "127.0.0.1", 8002},
    {3, "127.0.0.1", 8003},
    // {4, "127.0.0.1", 8004},
    // {5,"127.0.0.1",8005}
};

struct LoadInfo {
    double load_score;
    int cpu_used;
    int memory_used;
    long long timestamp;
    
    LoadInfo() : load_score(0.0), cpu_used(0), memory_used(0), timestamp(0) {}
};

struct Job {
    uint64_t job_id;
    int cpu_required;
    int memory_required;
    int estimated_duration;
    long long timestamp;
    int hop_count;
    
    Job() : job_id(0), cpu_required(0), memory_required(0), 
            estimated_duration(0), timestamp(0), hop_count(0) {}
};

struct PeerConnection {
    int peer_id;
    int socket_fd;
    bool connected;
    std::mutex socket_mutex;
    
    PeerConnection() : peer_id(-1), socket_fd(-1), connected(false) {}
};

class Peer {
private:
    std::ofstream file;
    int my_peer_id;
    int my_port;
    int server_socket;
    std::atomic<bool> running;
    
    int job_thrower_socket;
    std::mutex job_thrower_mutex;
    
    std::map<int, PeerConnection*> peer_connections;
    std::mutex peer_connections_mutex;
    
    std::atomic<int>jobs_forwarded; // your forwarded jobs
    std::atomic<int>jobs_executed; // number of jobs u executed
    std::atomic<int>forward_receives; // recevied from peers
    std::atomic<int>direct_receives; // received from thrower
    std::vector<int>forwarded_jobs; 
    std::vector<int>executed_jobs;
    std::vector<double> load_tracker;
    std::mutex load_track_mutex;

    std::atomic<int> current_cpu_used;
    std::atomic<int> current_memory_used;
    std::map<int, LoadInfo> load_table;
    std::mutex load_table_mutex;
    
    std::queue<Job> job_queue;
    std::mutex job_queue_mutex;
    std::condition_variable job_queue_cv;
    
    std::vector<Job> running_jobs;
    std::mutex running_jobs_mutex;
    
    std::mt19937 rng;
    
public:
    Peer(int peer_id) 
        : my_peer_id(peer_id), 
          my_port(all_peers[peer_id-1].port),
          server_socket(-1),
          running(false),
          job_thrower_socket(-1),
          current_cpu_used(0),
          current_memory_used(0),
          rng(std::random_device{}())
    {
        file.open(std::string("logs_")+std::to_string(peer_id)+".txt");
        for (int i = 0; i < TOTAL_PEERS; i++) {
            if (all_peers[i].peer_id != my_peer_id) {
                peer_connections[all_peers[i].peer_id] = new PeerConnection();
                peer_connections[all_peers[i].peer_id]->peer_id = all_peers[i].peer_id;
            }
        }
    }
    
    ~Peer() {
        shutdown();
        for (auto& pair : peer_connections) {
            delete pair.second;
        }
    }
    
    double calculate_load_score() {
        int cpu = current_cpu_used.load();
        int mem = current_memory_used.load();
        double cpu_ratio = static_cast<double>(cpu) / CPU_CAPACITY;
        double mem_ratio = static_cast<double>(mem) / MEMORY_CAPACITY;
        return ALPHA * cpu_ratio + BETA * mem_ratio;
    }
    
    bool can_accept_job(const Job& job) {
        int cpu = current_cpu_used.load();
        int mem = current_memory_used.load();
        
        if (cpu + job.cpu_required > CPU_CAPACITY) return false;
        if (mem + job.memory_required > MEMORY_CAPACITY) return false;
        
        double load = calculate_load_score();
        return load < ACCEPT_THRESHOLD;
    }
    
    bool start_server() {
        server_socket = socket(AF_INET, SOCK_STREAM, 0);
        if (server_socket < 0) {
            std::cerr << "Failed to create server socket" << std::endl;
            return false;
        }
        
        int opt = 1;
        setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        
        sockaddr_in server_addr;
        std::memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(my_port);
        
        if (bind(server_socket, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            std::cerr << "Bind failed for peer " << my_peer_id << std::endl;
            close(server_socket);
            return false;
        }
        
        if (listen(server_socket, 10) < 0) {
            std::cerr << "Listen failed" << std::endl;
            close(server_socket);
            return false;
        }
        
        running = true;
        std::cout << "Peer " << my_peer_id << " server started on port " << my_port << std::endl;
        return true;
    }
    
    void setup_p2p_connections() {
        std::cout << "Peer " << my_peer_id << " ready. Press ENTER when all peers are started: ";
        std::cin.get();
        
        std::thread accept_thread(&Peer::accept_peer_connections, this);
        accept_thread.detach();
        
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        for (int i = 0; i < TOTAL_PEERS; i++) {
            int peer_id = all_peers[i].peer_id;
            if (peer_id < my_peer_id) {
                connect_to_peer(peer_id);
            }
        }
        
        std::cout << "Waiting for all P2P connections..." << std::endl;
        while (true) {
            int connected_count = 0;
            {
                std::lock_guard<std::mutex> lock(peer_connections_mutex);
                for (auto& pair : peer_connections) {
                    if (pair.second->connected) connected_count++;
                }
            }
            if (connected_count == TOTAL_PEERS - 1) {
                std::cout << "All P2P connections established!" << std::endl;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }
    
    void connect_to_peer(int peer_id) {
        std::string ip = all_peers[peer_id-1].ip;
        int port = all_peers[peer_id-1].port;
        
        int sock = connect_to_server(ip, port);
        if (sock < 0) {
            std::cerr << "Failed to connect to peer " << peer_id << std::endl;
            return;
        }
        
        json handshake;
        handshake["type"] = "peer_handshake";
        handshake["peer_id"] = my_peer_id;
        handshake["port"] = my_port;
        
        std::string handshake_str = handshake.dump();
        auto [success, err] = send_string(sock, handshake_str);
        
        if (!success) {
            std::cerr << "Failed to send handshake to peer " << peer_id << std::endl;
            close(sock);
            return;
        }
        
        std::string ack_msg;
        auto [ack_success, ack_err] = recv_string(sock, ack_msg);
        
        if (ack_success) {
            std::lock_guard<std::mutex> lock(peer_connections_mutex);
            peer_connections[peer_id]->socket_fd = sock;
            peer_connections[peer_id]->connected = true;
            std::cout << "Connected to peer " << peer_id << std::endl;
            
            std::thread handler(&Peer::handle_peer_messages, this, peer_id);
            handler.detach();
        }
    }
    
    void accept_peer_connections() {
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
            
            std::string msg;
            auto [success, err] = recv_string(peer_socket, msg);
            
            if (!success) {
                close(peer_socket);
                continue;
            }
            
            try {
                json handshake = json::parse(msg);
                if (handshake["type"] == "peer_handshake") {
                    int peer_id = handshake["peer_id"];
                    
                    json ack;
                    ack["type"] = "peer_handshake_ack";
                    ack["peer_id"] = my_peer_id;
                    std::string ack_str = ack.dump();
                    send_string(peer_socket, ack_str);
                    
                    std::lock_guard<std::mutex> lock(peer_connections_mutex);
                    peer_connections[peer_id]->socket_fd = peer_socket;
                    peer_connections[peer_id]->connected = true;
                    std::cout << "Accepted connection from peer " << peer_id << std::endl;
                    
                    std::thread handler(&Peer::handle_peer_messages, this, peer_id);
                    handler.detach();
                }
            } catch (const std::exception& e) {
                std::cerr << "Error in handshake: " << e.what() << std::endl;
                close(peer_socket);
            }
        }
    }
    
    bool connect_to_job_thrower() {
        job_thrower_socket = connect_to_server(JOB_THROWER_IP, JOB_THROWER_PORT);
        if (job_thrower_socket < 0) {
            std::cerr << "Failed to connect to job thrower" << std::endl;
            return false;
        }
        
        json reg;
        reg["type"] = "register";
        reg["peer_id"] = my_peer_id;
        
        std::string reg_str = reg.dump();
        auto [success, err] = send_string(job_thrower_socket, reg_str);
        
        if (!success) {
            std::cerr << "Failed to register with job thrower" << std::endl;
            return false;
        }
        
        std::cout << "Registered with job thrower" << std::endl;
        
        json allowed;
        allowed["type"] = "jobs_allowed";
        allowed["peer_id"] = my_peer_id;
        
        std::string allowed_str = allowed.dump();
        send_string(job_thrower_socket, allowed_str);
        
        std::cout << "Sent jobs_allowed to job thrower" << std::endl;
        
        std::thread job_listener(&Peer::listen_job_thrower, this);
        job_listener.detach();
        
        return true;
    }
    
    void listen_job_thrower() {
        while (running) {
            std::string msg;
            auto [success, err] = recv_string(job_thrower_socket, msg);
            
            if (!success) {
                std::cerr << "Job thrower disconnected" << std::endl;
                break;
            }
            
            try {
                json job_json = json::parse(msg);
                if (job_json["type"] == "job") {
                    direct_receives++;
                    Job job;
                    job.job_id = job_json["job_id"];
                    job.cpu_required = job_json["cpu_required"];
                    job.memory_required = job_json["memory_required"];
                    job.estimated_duration = job_json["estimated_duration"];
                    job.timestamp = job_json["timestamp"];
                    job.hop_count = 0;
                    
                   file << "\n[RECEIVED] Job " << job.job_id << " from job thrower" << std::endl;
                    handle_incoming_job(job, -1);
                }else if(job_json["type"]=="request_stats"){
                    load_track_mutex.lock();
                    auto [m,s]=mean_and_stddev(load_tracker);
                    load_track_mutex.unlock();
                    json j ;
                    j["load"]=m;
                    j["type"]="my_avg_load";
                    std::string str =j.dump();
                    send_string(job_thrower_socket,str);
                }
                msg="";
            } catch (const std::exception& e) {
                std::cerr << "Error parsing job from job thrower: " << e.what() << std::endl;
            }
        }
    }
    
    void handle_peer_messages(int peer_id) {
        peer_connections_mutex.lock();
        PeerConnection* peer = peer_connections[peer_id];
        peer_connections_mutex.unlock();
        
        while (running && peer->connected) {
            std::string msg;
            auto [success, err] = recv_string(peer->socket_fd, msg);
            
            if (!success) {
                std::cout << "Peer " << peer_id << " disconnected" << std::endl;
                peer->connected = false;
                close(peer->socket_fd);
                break;
            }
            
            try {
                json message = json::parse(msg);
                
                if (message["type"] == "gossip_load") {
                    handle_gossip_message(message);
                } else if (message["type"] == "job_forward") {
                    forward_receives++;
                    Job job;
                    job.job_id = message["job_id"];
                    job.cpu_required = message["cpu_required"];
                    job.memory_required = message["memory_required"];
                    job.estimated_duration = message["estimated_duration"];
                    job.timestamp = message["timestamp"];
                    job.hop_count = message["hop_count"];
                    
                   file << "\n[FORWARDED] Job " << job.job_id << " from peer " << peer_id << std::endl;
                    handle_incoming_job(job, peer_id);
                }
            } catch (const std::exception& e) {
                std::cerr << "Error handling message from peer " << peer_id << ": " << e.what() << std::endl;
            }
        }
    }
    
    void handle_incoming_job(const Job& job, int source_peer) {
        if (can_accept_job(job)) {
            current_cpu_used += job.cpu_required;
            current_memory_used += job.memory_required;
        
            {
                std::lock_guard<std::mutex> lock(running_jobs_mutex);
                running_jobs.push_back(job);
            }
            
            file << " (Load: " << calculate_load_score() 
                      << ", CPU: " << current_cpu_used << "/" << CPU_CAPACITY 
                      << ", MEM: " << current_memory_used << "/" << MEMORY_CAPACITY << ")" << std::endl;
            
            std::thread executor(&Peer::execute_job, this, job);
            executor.detach();
            
        } else {
           file<< "[OVERLOADED] Cannot accept job " << job.job_id 
                      << " (Load: " << calculate_load_score() << ")" << std::endl;
            
            if (job.hop_count >= 3) {
               file<< "[QUEUED] Job " << job.job_id << " exceeded max hops" << std::endl;
                std::lock_guard<std::mutex> lock(job_queue_mutex);
                job_queue.push(job);
                job_queue_cv.notify_one();
            } else {
                int target_peer = select_offload_peer(source_peer);
                if (target_peer != -1) {
                    jobs_forwarded++;
                    forward_job_to_peer(target_peer, job);
                    forwarded_jobs.push_back(job.job_id);
                } else {
                   file << "[QUEUED] No suitable peer, queueing job " << job.job_id << std::endl;
                    std::lock_guard<std::mutex> lock(job_queue_mutex);
                    job_queue.push(job);
                    job_queue_cv.notify_one();
                }
            }
        }
    }
    
    int select_offload_peer(int exclude_peer) {
        std::lock_guard<std::mutex> lock(load_table_mutex);
        
        std::vector<std::pair<int, double>> candidates;
        
        for (auto& pair : load_table) {
            int peer_id = pair.first;
            LoadInfo& info = pair.second;
            
            if (peer_id == exclude_peer) continue;
            
            if (info.load_score < ACCEPT_THRESHOLD) {
                candidates.push_back({peer_id, info.load_score});
            }
        }
        
        if (candidates.empty()) {
            std::vector<int> all_peer_ids;
            for (auto& pair : peer_connections) {
                if (pair.first != exclude_peer && pair.second->connected) {
                    all_peer_ids.push_back(pair.first);
                }
            }
            
            if (all_peer_ids.empty()) return -1;
            
            std::uniform_int_distribution<size_t> dist(0, all_peer_ids.size() - 1);
            return all_peer_ids[dist(rng)];
        }
        
        std::sort(candidates.begin(), candidates.end(), 
                  [](const auto& a, const auto& b) { return a.second < b.second; });
        
        return candidates[0].first;
    }
    
    void forward_job_to_peer(int peer_id, Job job) {
        std::lock_guard<std::mutex> lock(peer_connections_mutex);
        
        if (peer_connections.find(peer_id) == peer_connections.end() || 
            !peer_connections[peer_id]->connected) {
            return;
        }
        
        PeerConnection* peer = peer_connections[peer_id];
        std::lock_guard<std::mutex> socket_lock(peer->socket_mutex);
        
        job.hop_count++;
        
        json forward_msg;
        forward_msg["type"] = "job_forward";
        forward_msg["job_id"] = job.job_id;
        forward_msg["cpu_required"] = job.cpu_required;
        forward_msg["memory_required"] = job.memory_required;
        forward_msg["estimated_duration"] = job.estimated_duration;
        forward_msg["timestamp"] = job.timestamp;
        forward_msg["hop_count"] = job.hop_count;
        
        std::string msg_str = forward_msg.dump();
        auto [success, err] = send_string(peer->socket_fd, msg_str);
        
        if (success) {
          file << "[OFFLOADING] Job " << job.job_id << " to peer " << peer_id << std::endl;
        }
    }
    
    void execute_job(Job job) {
       file<< "[EXECUTING] Job " << job.job_id << " for " << job.estimated_duration << " seconds" << std::endl;
        
        std::this_thread::sleep_for(std::chrono::seconds(job.estimated_duration));
        
        current_cpu_used -= job.cpu_required;
        current_memory_used -= job.memory_required;
        jobs_executed++; // i.e completely job is completed
        executed_jobs.push_back(job.job_id);
        {
            std::lock_guard<std::mutex> lock(running_jobs_mutex);
            running_jobs.erase(
                std::remove_if(running_jobs.begin(), running_jobs.end(),
                               [&job](const Job& j) { return j.job_id == job.job_id; }),
                running_jobs.end()
            );
        }
        
        file<< "[COMPLETED] Job " << job.job_id 
                  << " (Load now: " << calculate_load_score() << ")" << std::endl;
        
        json completion;
        completion["type"] = "job_completed";
        completion["job_id"] = job.job_id;
        completion["peer_id"] = my_peer_id;
        completion["execution_time"] = job.estimated_duration;
        
        std::string completion_str = completion.dump();
        std::lock_guard<std::mutex> lock(job_thrower_mutex);
        send_string(job_thrower_socket, completion_str);
    }
    
    void process_job_queue() {
        while (running) {
            std::unique_lock<std::mutex> lock(job_queue_mutex);
            job_queue_cv.wait(lock, [this]{ return !job_queue.empty() || !running; });
            
            if (!running) break;
            
            if (!job_queue.empty()) {
                Job job = job_queue.front();
                
                if (can_accept_job(job)) {
                    job_queue.pop();
                    lock.unlock();
                    
                    current_cpu_used += job.cpu_required;
                    current_memory_used += job.memory_required;
                    
                    {
                        std::lock_guard<std::mutex> rj_lock(running_jobs_mutex);
                        running_jobs.push_back(job);
                    }
                    
                    file << "[QUEUE->EXEC] Job " << job.job_id << std::endl;
                    std::thread executor(&Peer::execute_job, this, job);
                    executor.detach();
                } else {
                    lock.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
            }
        }
    }
    
    void gossip_loop() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(GOSSIP_INTERVAL_MS));
            
            std::vector<int> peer_ids;
            {
                std::lock_guard<std::mutex> lock(peer_connections_mutex);
                for (auto& pair : peer_connections) {
                    if (pair.second->connected) {
                        peer_ids.push_back(pair.first);
                    }
                }
            }
            
            if (peer_ids.empty()) continue;
            
            std::shuffle(peer_ids.begin(), peer_ids.end(), rng);
            int fanout = std::min(GOSSIP_FANOUT, static_cast<int>(peer_ids.size()));
            
            for (int i = 0; i < fanout; i++) {
                send_gossip_message(peer_ids[i]);
            }
            
            clean_stale_load_info();
        }
    }
    
    void send_gossip_message(int peer_id) {
        json gossip;
        gossip["type"] = "gossip_load";
        gossip["peer_id"] = my_peer_id;
        gossip["load_score"] = calculate_load_score();
        gossip["cpu_used"] = current_cpu_used.load();
        gossip["memory_used"] = current_memory_used.load();
        gossip["cpu_capacity"] = CPU_CAPACITY;
        gossip["memory_capacity"] = MEMORY_CAPACITY;
        gossip["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        
        json known_loads = json::object();
        {
            std::lock_guard<std::mutex> lock(load_table_mutex);
            for (auto& pair : load_table) {
                json load_info;
                load_info["load"] = pair.second.load_score;
                load_info["timestamp"] = pair.second.timestamp;
                known_loads[std::to_string(pair.first)] = load_info;
            }
        }
        gossip["known_loads"] = known_loads;
        
        std::string gossip_str = gossip.dump();
        
        std::lock_guard<std::mutex> lock(peer_connections_mutex);
        if (peer_connections[peer_id]->connected) {
            std::lock_guard<std::mutex> socket_lock(peer_connections[peer_id]->socket_mutex);
            send_string(peer_connections[peer_id]->socket_fd, gossip_str);
        }
    }
    
    void handle_gossip_message(const json& gossip) {
        int peer_id = gossip["peer_id"];
        
        LoadInfo info;
        info.load_score = gossip["load_score"];
        info.cpu_used = gossip["cpu_used"];
        info.memory_used = gossip["memory_used"];
        info.timestamp = gossip["timestamp"];
        
        {
            std::lock_guard<std::mutex> lock(load_table_mutex);
            load_table[peer_id] = info;
            
            if (gossip.contains("known_loads")) {
                for (auto& item : gossip["known_loads"].items()) {
                    int other_peer_id = std::stoi(item.key());
                    if (other_peer_id != my_peer_id) {
                        long long their_timestamp = item.value()["timestamp"];
                        
                        if (load_table.find(other_peer_id) == load_table.end() ||
                            load_table[other_peer_id].timestamp < their_timestamp) {
                            LoadInfo other_info;
                            other_info.load_score = item.value()["load"];
                            other_info.timestamp = their_timestamp;
                            load_table[other_peer_id] = other_info;
                        }
                    }
                }
            }
        }
    }
    
    void clean_stale_load_info() {
        long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        
        std::lock_guard<std::mutex> lock(load_table_mutex);
        auto it = load_table.begin();
        while (it != load_table.end()) {
            if (now - it->second.timestamp > STALE_THRESHOLD_MS) {
                it = load_table.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    void print_statistics() {
        std::cout << "\n=== Peer " << my_peer_id << " Statistics ===" << std::endl;
        std::cout << "Current Load: " << calculate_load_score() << std::endl;
        std::cout << "CPU Used: " << current_cpu_used << "/" << CPU_CAPACITY << std::endl;
        std::cout << "Memory Used: " << current_memory_used << "/" << MEMORY_CAPACITY << std::endl;

        std::cout<<"Jobs-received from thrower: "<<direct_receives<<"\n";
        std::cout<<"jobs-received from forwarding "<<forward_receives<<"\n";
        std::cout<<"jobs  executed "<<jobs_executed<<"\n";
        std::cout<<"Jobs off-loaded : "<<jobs_forwarded<<"\n";

        {
            std::lock_guard<std::mutex> lock(running_jobs_mutex);
            std::cout << "Running Jobs: " << running_jobs.size() << std::endl;
        }
        
        {
            std::lock_guard<std::mutex> lock(job_queue_mutex);
            std::cout << "Queued Jobs: " << job_queue.size() << std::endl;
        }
        
        std::cout << "\nKnown Peer Loads:" << std::endl;
        {
            std::lock_guard<std::mutex> lock(load_table_mutex);
            for (auto& pair : load_table) {
                std::cout << "  Peer " << pair.first << ": " << pair.second.load_score << std::endl;
            }
        }

        {
            std::lock_guard<std::mutex>lock(load_track_mutex);
            auto [mean,stddev]=mean_and_stddev(load_tracker);
            std::cout<<"average load: "<<mean<<"\n";
        }
    }


    // this is there to exeucte this periodically so to account how load is 
    // i.e is puts values into load tracker
    void track_loads(){
        
        while(running){
            auto load_value = calculate_load_score();
            load_track_mutex.lock();
            load_tracker.push_back(load_value);
            load_track_mutex.unlock();
            std::this_thread::sleep_for(std::chrono::seconds(30));
        }
    }
    
    void run() {
        if (!start_server()) {
            return;
        }
        
        setup_p2p_connections();
        
        if (!connect_to_job_thrower()) {
            return;
        }
        
        std::thread gossip_thread(&Peer::gossip_loop, this);
        gossip_thread.detach();
        
        std::thread queue_processor(&Peer::process_job_queue, this);
        queue_processor.detach();
        
        std::thread load_caclulation_thread(&Peer::track_loads,this);
        load_caclulation_thread.detach();
        std::cout << "\nPeer " << my_peer_id << " fully operational!" << std::endl;
        std::cout << "Press Enter to view statistics (type 'quit' to exit)..." << std::endl;
        
        std::string input;
        while (std::getline(std::cin, input)) {
            if (input == "quit") {
                break;
            }
            print_statistics();
        }
    }
    
    void shutdown() {
        if (!running) return;
        running = false;
        
        if (job_thrower_socket >= 0) {
            close(job_thrower_socket);
        }
        
        if (server_socket >= 0) {
            close(server_socket);
        }
        
        std::lock_guard<std::mutex> lock(peer_connections_mutex);
        for (auto& pair : peer_connections) {
            if (pair.second->connected && pair.second->socket_fd >= 0) {
                close(pair.second->socket_fd);
                pair.second->connected = false;
            }
        }
        
        job_queue_cv.notify_all();
        std::cout << "Peer " << my_peer_id << " shutdown complete" << std::endl;
    }
};




int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <peer_id>" << std::endl;
        std::cerr << "Example: " << argv[0] << " 1" << std::endl;
        return 1;
    }
    
    int peer_id = std::atoi(argv[1]);
    
    if (peer_id < 1 || peer_id > TOTAL_PEERS) {
        std::cerr << "Invalid peer_id. Must be between 1 and " << TOTAL_PEERS << std::endl;
        return 1;
    }
    
    std::cout << "Starting Peer " << peer_id << "..." << std::endl;
    
    Peer peer(peer_id);
    peer.run();
    
    return 0;
}