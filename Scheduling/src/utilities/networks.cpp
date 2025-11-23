
/*
- this is network.cpp file 
- provides functionality to send string msgs and any msgs
- use it to send msg as whole 
i.e use recv_string send_string and connect_to_server to do those functions
*/

#include<macros.hpp> // conatins most fo cpp libraries needed 
#include <iostream>
#include <string>
#include <cstring>
#include <utility>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>



enum NetError {
    NET_OK = 0,
    NET_ERR_SYS = -1,      // System-level error (check errno)
    NET_ERR_PEER_CLOSED = -2,
    NET_ERR_PROTOCOL = -3
};

ssize_t recv_all(int descriptor, void* buffer, size_t expected_length) {
    size_t collected = 0;
    char* buf = static_cast<char*>(buffer);

    while (collected < expected_length) {
        ssize_t n = recv(descriptor, buf + collected, expected_length - collected, 0);
        if (n == 0) {
            // Peer closed connection
            return NET_ERR_PEER_CLOSED;
        }
        if (n < 0) {
            if (errno == EINTR) continue; // retry
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
            if (errno == EINTR) continue; // retry
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
    if (len > 10 * 1024 * 1024) { // 10MB sanity limit
        return {false, NET_ERR_PROTOCOL};
    }

    msg.resize(len);
    n = recv_all(descriptor, msg.data(), len);
    if (n == NET_ERR_PEER_CLOSED) return {false, NET_ERR_PEER_CLOSED};
    if (n < 0) return {false, static_cast<int>(n)};

    return {true, NET_OK};
}



// Function to connect to a TCP server
int connect_to_server(const std::string &ip, int port) {
    // 1. Create socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket() failed");
        return -1;
    }

    // 2. Setup server address struct
    sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;       // IPv4
    server_addr.sin_port = htons(port);     // Convert port to network byte order

    // 3. Convert IP string to binary
    if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) <= 0) {
        perror("inet_pton() failed - invalid IP address");
        close(sock);
        return -1;
    }

    // 4. Connect to the server
    if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect() failed");
        close(sock);
        return -1;
    }

    // 5. Connected successfully
    std::cout << "Connected to " << ip << ":" << port << std::endl;
    return sock;  // return the connected socket descriptor
}