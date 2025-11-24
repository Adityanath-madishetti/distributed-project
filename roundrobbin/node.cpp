// peer_rr.cpp - Basic Round Robin Peer (NO P2P, NO Gossip, NO Offloading)
#include <iostream>
#include <string>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include "json.hpp"

using json = nlohmann::json;

#define JOB_THROWER_IP "127.0.0.1"
#define JOB_THROWER_PORT 9000

#define CPU_CAPACITY 1000
#define MEMORY_CAPACITY 4096

#define MAX_JOBS 100

#define ALPHA 0.6
#define BETA 0.4

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

int connect_to_server(const std::string &ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    
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

struct Job {
    uint64_t job_id;
    int cpu_required;
    int memory_required;
    int estimated_duration;
    long long timestamp;
    
    Job() : job_id(0), cpu_required(0), memory_required(0), 
            estimated_duration(0), timestamp(0) {}
};

class RoundRobinPeer {
private:
    int my_peer_id;
    std::atomic<bool> running;
    
    int job_thrower_socket;
    std::mutex job_thrower_mutex;
    
    std::atomic<int> current_cpu_used;
    std::atomic<int> current_memory_used;
    
    std::queue<Job> job_queue;
    std::mutex job_queue_mutex;
    std::condition_variable job_queue_cv;
    
    std::vector<Job> running_jobs;
    std::mutex running_jobs_mutex;
    
    std::atomic<int> jobs_accepted;
    std::atomic<int> jobs_rejected;
    std::atomic<int> jobs_completed;
    
public:
    RoundRobinPeer(int peer_id) 
        : my_peer_id(peer_id),
          running(false),
          job_thrower_socket(-1),
          current_cpu_used(0),
          current_memory_used(0),
          jobs_accepted(0),
          jobs_rejected(0),
          jobs_completed(0)
    {}
    
    ~RoundRobinPeer() {
        shutdown();
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
        
        // Check if adding this job would exceed capacity
        if (cpu + job.cpu_required > CPU_CAPACITY) return false;
        if (mem + job.memory_required > MEMORY_CAPACITY) return false;
        
        return true;
    }
    
    bool connect_to_job_thrower() {
        job_thrower_socket = connect_to_server(JOB_THROWER_IP, JOB_THROWER_PORT);
        if (job_thrower_socket < 0) {
            std::cerr << "Failed to connect to job thrower" << std::endl;
            return false;
        }
        
        // Send registration
        json reg;
        reg["type"] = "register";
        reg["peer_id"] = my_peer_id;
        
        std::string reg_str = reg.dump();
        auto [success, err] = send_string(job_thrower_socket, reg_str);
        
        if (!success) {
            std::cerr << "Failed to register" << std::endl;
            return false;
        }
        
        std::cout << "=== Peer " << my_peer_id << " (Round Robin) ===" << std::endl;
        std::cout << "Registered with job thrower" << std::endl;
        
        // Send jobs_allowed
        json allowed;
        allowed["type"] = "jobs_allowed";
        allowed["peer_id"] = my_peer_id;
        
        std::string allowed_str = allowed.dump();
        send_string(job_thrower_socket, allowed_str);
        
        std::cout << "Ready to accept jobs" << std::endl;
        
        running = true;
        
        // Start listening for jobs
        std::thread job_listener(&RoundRobinPeer::listen_job_thrower, this);
        job_listener.detach();
        
        // Start queue processor
        std::thread queue_processor(&RoundRobinPeer::process_job_queue, this);
        queue_processor.detach();
        
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
                    Job job;
                    job.job_id = job_json["job_id"];
                    job.cpu_required = job_json["cpu_required"];
                    job.memory_required = job_json["memory_required"];
                    job.estimated_duration = job_json["estimated_duration"];
                    job.timestamp = job_json["timestamp"];
                    
                    std::cout << "\n[RECEIVED] Job " << job.job_id << std::endl;
                    handle_incoming_job(job);
                }
            } catch (const std::exception& e) {
                std::cerr << "Error parsing job: " << e.what() << std::endl;
            }
        }
    }
    
    void handle_incoming_job(const Job& job) {
        if (can_accept_job(job)) {
            // ACCEPT - Execute immediately
            jobs_accepted++;
            
            current_cpu_used += job.cpu_required;
            current_memory_used += job.memory_required;
            
            {
                std::lock_guard<std::mutex> lock(running_jobs_mutex);
                running_jobs.push_back(job);
            }
            
            std::cout << "[ACCEPTED] Job " << job.job_id 
                      << " (Load: " << calculate_load_score() 
                      << ", CPU: " << current_cpu_used << "/" << CPU_CAPACITY 
                      << ", MEM: " << current_memory_used << "/" << MEMORY_CAPACITY << ")" << std::endl;
            
            std::thread executor(&RoundRobinPeer::execute_job, this, job);
            executor.detach();
            
        } else {
            // REJECT - Cannot accept (NO OFFLOADING in Round Robin!)
            jobs_rejected++;
            
            std::cout << "[REJECTED] Job " << job.job_id 
                      << " (Load: " << calculate_load_score() 
                      << ", CPU: " << current_cpu_used << "/" << CPU_CAPACITY 
                      << ", MEM: " << current_memory_used << "/" << MEMORY_CAPACITY 
                      << ") - CAPACITY EXCEEDED!" << std::endl;
            
            // Notify thrower about rejection
            json rejection;
            rejection["type"] = "job_rejected";
            rejection["job_id"] = job.job_id;
            rejection["peer_id"] = my_peer_id;
            rejection["reason"] = "capacity_exceeded";
            
            std::string rejection_str = rejection.dump();
            std::lock_guard<std::mutex> lock(job_thrower_mutex);
            send_string(job_thrower_socket, rejection_str);
        }
    }
    
    void execute_job(Job job) {
        std::cout << "[EXECUTING] Job " << job.job_id 
                  << " for " << job.estimated_duration << " seconds" << std::endl;
        
        // Simulate job execution
        std::this_thread::sleep_for(std::chrono::seconds(job.estimated_duration));
        
        // Job completed - free resources
        current_cpu_used -= job.cpu_required;
        current_memory_used -= job.memory_required;
        
        {
            std::lock_guard<std::mutex> lock(running_jobs_mutex);
            running_jobs.erase(
                std::remove_if(running_jobs.begin(), running_jobs.end(),
                               [&job](const Job& j) { return j.job_id == job.job_id; }),
                running_jobs.end()
            );
        }
        
        jobs_completed++;
        
        std::cout << "[COMPLETED] Job " << job.job_id 
                  << " (Load now: " << calculate_load_score() << ")" << std::endl;
        
        // Notify thrower
        json completion;
        completion["type"] = "job_completed";
        completion["job_id"] = job.job_id;
        completion["peer_id"] = my_peer_id;
        completion["execution_time"] = job.estimated_duration;
        
        std::string completion_str = completion.dump();
        std::lock_guard<std::mutex> lock(job_thrower_mutex);
        send_string(job_thrower_socket, completion_str);
        
        // Check if there are queued jobs
        job_queue_cv.notify_one();
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
                    
                    std::cout << "[QUEUE->EXEC] Job " << job.job_id << std::endl;
                    std::thread executor(&RoundRobinPeer::execute_job, this, job);
                    executor.detach();
                } else {
                    lock.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
            }
        }
    }
    
    void print_statistics() {
        std::cout << "\n=== Peer " << my_peer_id << " Statistics ===" << std::endl;
        std::cout << "Jobs Accepted: " << jobs_accepted << std::endl;
        std::cout << "Jobs Rejected: " << jobs_rejected << std::endl;
        std::cout << "Jobs Completed: " << jobs_completed << std::endl;
        std::cout << "Current Load: " << calculate_load_score() << std::endl;
        std::cout << "CPU: " << current_cpu_used << "/" << CPU_CAPACITY << std::endl;
        std::cout << "Memory: " << current_memory_used << "/" << MEMORY_CAPACITY << std::endl;
        
        {
            std::lock_guard<std::mutex> lock(running_jobs_mutex);
            std::cout << "Running Jobs: " << running_jobs.size() << std::endl;
        }
        
        {
            std::lock_guard<std::mutex> lock(job_queue_mutex);
            std::cout << "Queued Jobs: " << job_queue.size() << std::endl;
        }
    }
    
    void run() {
        if (!connect_to_job_thrower()) {
            return;
        }
        
        std::cout << "\nPeer " << my_peer_id << " operational!" << std::endl;
        std::cout << "Press Enter for statistics (type 'quit' to exit)..." << std::endl;
        
        std::string input;
        while (std::getline(std::cin, input)) {
            if (input == "quit") break;
            print_statistics();
        }
    }
    
    void shutdown() {
        if (!running) return;
        running = false;
        
        if (job_thrower_socket >= 0) {
            close(job_thrower_socket);
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
    
    if (peer_id < 1 || peer_id > 10) {
        std::cerr << "Invalid peer_id. Must be between 1 and 10" << std::endl;
        return 1;
    }
    
    RoundRobinPeer peer(peer_id);
    peer.run();
    
    return 0;
}