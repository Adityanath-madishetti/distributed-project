#include <zmq.hpp>
#include <iostream>
#include <thread>
#include <chrono>
#include <string>
#include<zmqpp/zmqpp.hpp>
void receiver(zmq::context_t& ctx, std::string bind_addr) {
    zmq::socket_t router(ctx, zmq::socket_type::router);
    router.bind(bind_addr);
    std::cout << "Listening on " << bind_addr << std::endl;

    while (true) {
        zmq::message_t sender;
        zmq::message_t empty;
        zmq::message_t content;

        router.recv(sender);
        router.recv(empty);
        router.recv(content);

        std::string msg = content.to_string();
        std::string sender_id = sender.to_string();

        std::cout << "[RECV from " << sender_id << "] " << msg << std::endl;

        // reply
        std::string reply = "Reply from " + bind_addr;
        router.send(sender, zmq::send_flags::sndmore);
        router.send(zmq::message_t(), zmq::send_flags::sndmore);
        router.send(zmq::buffer(reply), zmq::send_flags::none);
    }
}

void sender(zmq::context_t& ctx, std::string id, std::vector<std::string> peers) {
    zmq::socket_t dealer(ctx, zmq::socket_type::dealer);
    dealer.set(zmq::sockopt::routing_id, id);

    for (auto& p : peers) {
        dealer.connect(p);
        std::cout << "[" << id << "] connected to " << p << std::endl;
    }

    while (true) {
        std::string msg = "Hello from " + id;
        dealer.send(zmq::buffer(msg), zmq::send_flags::none);

        zmq::message_t reply;
        dealer.recv(reply);
        std::cout << "[" << id << "] Got reply: " << reply.to_string() << std::endl;

        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}

int main() {
    zmq::context_t ctx(1);

    // Example for peer A
    std::thread t1(receiver, std::ref(ctx), "tcp://*:5551");
    std::this_thread::sleep_for(std::chrono::seconds(1)); // wait for bind

    sender(ctx, "peer-A", {"tcp://localhost:5552", "tcp://localhost:5553"});

    t1.join();
}
