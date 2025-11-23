// peer.cpp
// Simple P2P peer: listens for incoming connections and can also connect to another peer.
// Compile: g++ -std=c++17 -pthread peer.cpp -o peer
// Works on Linux/macOS (POSIX). For Windows, Winsock initialization/cleanup and API names differ.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

static const int BACKLOG = 10;
std::atomic<bool> running{true};
std::mutex cout_mtx;

// helper: print thread-safe
void ts_print(const std::string &s) {
    std::lock_guard<std::mutex> lk(cout_mtx);
    std::cout << s << std::endl;
}

// thread: handle a connected socket (recv and echo/display)
void connection_handler(int connfd, sockaddr_in peer_addr) {
    char peer_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &peer_addr.sin_addr, peer_ip, sizeof(peer_ip));
    int peer_port = ntohs(peer_addr.sin_port);
    {
        std::string info = "Connected to " + std::string(peer_ip) + ":" + std::to_string(peer_port)
                         + " (fd=" + std::to_string(connfd) + ")";
        ts_print(info);
    }

    // simple loop: read lines from socket and print
    constexpr size_t BUF_SZ = 1024;
    char buf[BUF_SZ];
    while (running) {
        ssize_t n = recv(connfd, buf, BUF_SZ - 1, 0);
        if (n > 0) {
            buf[n] = '\0';
            std::string msg = std::string("[") + peer_ip + ":" + std::to_string(peer_port) + "] " + buf;
            ts_print(msg);
        } else if (n == 0) {
            ts_print("Peer disconnected (fd=" + std::to_string(connfd) + ")");
            break;
        } else {
            ts_print("recv error on fd " + std::to_string(connfd) + ": " + strerror(errno));
            break;
        }
    }

    close(connfd);
}

// thread: listening socket accept loop
void listener_thread(uint16_t listen_port) {
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) {
        ts_print(std::string("socket() failed: ") + strerror(errno));
        return;
    }

    // allow reuse
    int opt = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(listen_port);

    if (bind(listenfd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        ts_print(std::string("bind() failed: ") + strerror(errno));
        close(listenfd);
        return;
    }

    if (listen(listenfd, BACKLOG) < 0) {
        ts_print(std::string("listen() failed: ") + strerror(errno));
        close(listenfd);
        return;
    }

    ts_print("Listening on port " + std::to_string(listen_port) + "...");

    while (running) {
        sockaddr_in peer{};
        socklen_t plen = sizeof(peer);
        int connfd = accept(listenfd, (sockaddr*)&peer, &plen);
        if (connfd < 0) {
            if (errno == EINTR) continue;
            ts_print(std::string("accept() failed: ") + strerror(errno));
            break;
        }
        // spawn handler thread for this connection
        std::thread(connection_handler, connfd, peer).detach();
    }

    close(listenfd);
}

// function: connect to a remote peer and start a handler thread for that socket
int connect_to_peer(const std::string &ip, uint16_t port)  {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        ts_print(std::string("socket() failed: ") + strerror(errno));
        return -1;
    }

    sockaddr_in remote{};
    remote.sin_family = AF_INET;
    remote.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &remote.sin_addr) <= 0) {
        ts_print("inet_pton failed for " + ip);
        close(sock);
        return -1;
    }

    ts_print("Attempting connection to " + ip + ":" + std::to_string(port) + " ...");
    if (connect(sock, (sockaddr*)&remote, sizeof(remote)) < 0) {
        ts_print(std::string("connect() failed: ") + strerror(errno));
        close(sock);
        return -1;
    }

    // Spawn a handler so we receive messages from this connected peer.
    std::thread(connection_handler, sock, remote).detach();
    return sock; // return socket so caller can send messages on it
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        std::cout << "Usage: " << argv[0] << " --listen <port> [--connect <peer_ip:port>]\n";
        std::cout << "Example:\n";
        std::cout << "  ./peer --listen 5000 --connect 127.0.0.1:6000\n";
        return 1;
    }

    uint16_t listen_port = 0;
    std::string connect_to = "";

    // Very simple arg parsing
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--listen" && i + 1 < argc) {
            listen_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (a == "--connect" && i + 1 < argc) {
            connect_to = argv[++i];
        }
    }

    if (listen_port == 0) {
        std::cerr << "Must provide --listen <port>\n";
        return 1;
    }

    // start listener in separate thread
    std::thread(listener_thread, listen_port).detach();

    // optionally connect out to a peer
    int outgoing_sock = -1;
    if (!connect_to.empty()) {
        // parse ip:port
        auto pos = connect_to.find(':');
        if (pos == std::string::npos) {
            std::cerr << "connect argument must be ip:port\n";
        } else {
            std::string ip = connect_to.substr(0, pos);
            uint16_t port = static_cast<uint16_t>(std::stoi(connect_to.substr(pos+1)));
            outgoing_sock = connect_to_peer(ip, port);
        }
    }

    // main loop: read user input and send to all outgoing socket(s).
    // For simplicity, we only send on the explicit outgoing socket (if connected).
    // Incoming connections each have their own handler thread which reads and displays messages.
    ts_print("Type messages and press Enter to send (to the peer you connected to).");
    ts_print("Type /quit to exit.");

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "/quit") break;
        if (outgoing_sock >= 0) {
            ssize_t w = send(outgoing_sock, line.c_str(), line.size(), 0);
            if (w < 0) {
                ts_print(std::string("send() failed: ") + strerror(errno));
                // optionally try to reconnect or ignore
            }
        } else {
            ts_print("No outgoing connection established. Use --connect to initiate one.");
        }
    }

    running = false;
    // Give a moment for threads to exit gracefully
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ts_print("Shutting down.");
    return 0;
}
