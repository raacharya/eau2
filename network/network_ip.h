#include <climits>

#pragma once

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <condition_variable>
#include <mutex>
#include "serial.h"

class NodeInfo : public Object {
    public:
        unsigned id;
        sockaddr_in address;
        int send = -1;
        int recv = -1;

        ~NodeInfo() {
            if (send != -1) {
                close(send);
            }
            if (recv != -1) {
                close(recv);
            }
        }
};

class NetworkIP : public Object {
    public:
        NodeInfo* nodes_;
        size_t this_node_;
        int sock_;
        sockaddr_in ip_;
//        std::mutex mtx;
//        std::condition_variable cv;
//        bool ready = false;

        int num_nodes = 5;

        ~NetworkIP() override {
            close(sock_);
            delete[] nodes_;
        }

        size_t index() { return this_node_; }

        void init_sock_(size_t port) {
            int yes=1;
            int rv;
            struct addrinfo hints, *ai, *p;
            memset(&hints, 0, sizeof hints);
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            std::string s = std::to_string(port);
            if ((rv = getaddrinfo(NULL, s.c_str(), &hints, &ai)) != 0) {
                fprintf(stderr, "error getting address information: %s\n", gai_strerror(rv));
                exit(1);
            }
            for(p = ai; p != NULL; p = p->ai_next) {
                sock_ = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
                if (sock_ < 0) {
                    continue;
                }
                setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
                if (::bind(sock_, p->ai_addr, p->ai_addrlen) < 0) {
                    close(sock_);
                    continue;
                }
                ip_ = *(struct sockaddr_in*)p->ai_addr;
                break;
            }
            assert(p != NULL);
            freeaddrinfo(ai);
            assert(listen(sock_, 10) != -1);
        }

        void server_init(unsigned idx, size_t port) {
//            std::unique_lock<std::mutex> lck(mtx);
            this_node_ = idx;
            init_sock_(port);
//            ready = true;
//            lck.unlock();
//            cv.notify_all();
            nodes_ = new NodeInfo[num_nodes];
            for (size_t i = 2; i <= num_nodes; i += 1) {
                auto* msg = dynamic_cast<Register*>(recv_first_msg(false));
                nodes_[msg->sender_].id = msg->sender_;
                nodes_[msg->sender_].address.sin_family = AF_INET;
                nodes_[msg->sender_].address.sin_port = htons(msg->port);
                nodes_[msg->sender_].send = -1;
                nodes_[msg->sender_].recv = -1;
                inet_aton(msg->client->c_str(), &(nodes_[msg->sender_].address.sin_addr));
                delete msg;
            }
            auto* ports = new size_t[num_nodes - 1];
            auto** addresses = new String*[num_nodes - 1];
            for (size_t i = 0; i < num_nodes - 1; i += 1) {
                ports[i] = ntohs(nodes_[i + 1].address.sin_port);
                addresses[i] = new String(inet_ntoa(nodes_[i + 1].address.sin_addr));
            }

            Directory ipd(num_nodes - 1, ports,addresses);
            for (size_t i = 1; i < num_nodes; i += 1) {
                ipd.target_ = i;
                send_msg(&ipd, false);
            }
        }

        void client_init(unsigned idx, size_t port, const char* server_adr, unsigned server_port) {
            this_node_ = idx;
            init_sock_(port);
            nodes_ = new NodeInfo[num_nodes];
            nodes_[0].id = 0;
            nodes_[0].address.sin_family = AF_INET;
            nodes_[0].address.sin_port = htons(server_port);
            if (inet_pton(AF_INET, server_adr, &nodes_[0].address.sin_addr) <= 0)
                assert(false && "Invalid server IP address format");
            Register msg(idx, port, inet_ntoa(ip_.sin_addr));
            send_msg(&msg, false);
            Message* rec = recv_first_msg(false);
            auto* ipd = dynamic_cast<Directory*>(rec);
            for (size_t i = 0; i < ipd->clients; i += 1) {
                nodes_[i + 1].id = i + 1;
                nodes_[i + 1].address.sin_family = AF_INET;
                nodes_[i + 1].address.sin_port = htons(ipd->ports[i]);
                nodes_[i + 1].send = -1;
                nodes_[i + 1].recv = -1;
                inet_aton(ipd->addresses[i]->c_str(), &nodes_[i + 1].address.sin_addr);
            }
            delete ipd;
        }

        void shutdown() {
            auto* kill = new Kill(this_node_, (this_node_ + 1) % num_nodes, 0);
            NodeInfo & tgt = nodes_[kill->target_];
            int conn = socket(AF_INET, SOCK_STREAM, 0);
            assert(conn >= 0 && "Unable to create client socket");
            if (connect(conn, (sockaddr *) &tgt.address, sizeof(tgt.address)) < 0)
                assert(false && "Unable to connect to remote node");
            send_msg_(kill, conn);
            close(conn);
            delete kill;
        }

        void send_msg(Message* msg, bool keepAlive) {
            NodeInfo & tgt = nodes_[msg->target_];
            if (tgt.send == -1) {
                tgt.send = socket(AF_INET, SOCK_STREAM, 0);
                assert(tgt.send >= 0 && "Unable to create client socket");
                if (connect(tgt.send, (sockaddr *) &tgt.address, sizeof(tgt.address)) < 0)
                    assert(false && "Unable to connect to remote node");
            }
            send_msg_(msg, tgt.send);
            if (!keepAlive) {
                close(tgt.send);
                tgt.send = -1;
            }
        }

        void send_reply(Message* msg, bool keepAlive) {
            NodeInfo & tgt = nodes_[msg->target_];
            send_msg_(msg, tgt.recv);
            if (!keepAlive) {
                close(tgt.recv);
                tgt.recv = -1;
            }
        }

        void send_msg_(Message* msg, int& sock) {
            assert(sock != -1);
            size_t size = 0;
            char* buf = Serializer::serialize(msg, size);
            send(sock, &size, sizeof(size_t), 0);
            send(sock, buf, size, 0);
            delete[] buf;
        }

        Message* recv_first_msg(bool keepAlive) {
            int req;
            accept_connection(req);
            Message* msg = recv_message_(req);
            if (!keepAlive) close(req);
            return msg;
        }

        Message* recv_msg(int& req, bool keepAlive) {
            Message* msg = recv_message_(req);
            NodeInfo & sender = nodes_[msg->sender_];
            if (sender.recv != -1) {
                close(sender.recv);
                sender.recv = -1;
            }
            if (keepAlive) {
                sender.recv = req;
            } else {
                close(req);
            }
            return msg;
        }

        Message* recv_msg(size_t index, bool keepAlive) {
            NodeInfo & sender = nodes_[index];
            Message* msg = recv_message_(sender.recv);
            if (!keepAlive) {
                close(sender.recv);
                sender.recv = -1;
            }
            return msg;
        }

        Message* recv_reply(size_t index, bool keepAlive) {
            NodeInfo & sender = nodes_[index];
            Message* msg = recv_message_(sender.send);
            if (!keepAlive) {
                close(sender.send);
                sender.send = -1;
            }
            return msg;
        }

        void accept_connection(int& req) {
            sockaddr_in sender{};
            socklen_t addrlen = sizeof(sender);
            req = accept(sock_, (sockaddr*)&sender, &addrlen);
        }

        Message* recv_message_(int& req) {
            if (req == -1) assert(false && "no established connection");
            size_t size = 0;
            if (read(req, &size, sizeof(size_t)) == 0) {
                close(req);
                req = -1;
                return nullptr;
            }
            char* buf = new char[size];
            int rd = 0;
            while (rd != size) rd += read(req, buf + rd, size - rd);
            Message* msg = Serializer::deserializeMessage(buf);
            delete[] buf;
            return msg;
        }
};