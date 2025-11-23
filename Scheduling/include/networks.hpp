

#ifndef NETWORK_HPP
#define NETWORK_HPP

#include <iostream>
#include <string>
#include <cstring>
#include <utility>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>

ssize_t recv_all(int descriptor, void* buffer, size_t expected_length);
ssize_t send_all(int descriptor, const void* buffer, size_t to_send) ;
std::pair<bool, int> send_string(int descriptor, const std::string& msg);
std::pair<bool, int> recv_string(int descriptor, std::string& msg);
int connect_to_server(const std::string &ip, int port);
#endif