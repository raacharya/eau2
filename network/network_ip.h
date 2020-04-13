#pragma once

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "network_ifc.h"

class NodeInfo : public Object {
    public:
        unsigned id;
        sockaddr_in address;
};

class NetworkIP : public NetworkIfc {
    public:
        NodeInfo* nodes_;
        size_t this_node_;
        int sock_;
        sockaddr_in ip_;

        int num_nodes = 5;

        ~NetworkIP() override { close(sock_); }

        size_t index() override { return this_node_; }

        int init_sock_(size_t port) {
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

            if (p == NULL) {
                return -1;
            }

            freeaddrinfo(ai);

            if (listen(sock_, 10) == -1) {
                return -1;
            }

            return sock_;
        }

        void server_init(unsigned idx, size_t port) {
            this_node_ = idx;
            init_sock_(port);
            nodes_ = new NodeInfo[num_nodes];
            for (size_t i = 2; i <= num_nodes; i += 1) {
                auto* msg = dynamic_cast<Register*>(recv_m());
                nodes_[msg->sender_].id = msg->sender_;
                nodes_[msg->sender_].address.sin_family = AF_INET;
                nodes_[msg->sender_].address.sin_port = htons(msg->port);
                inet_aton(msg->client, &(nodes_[msg->sender_].address.sin_addr));
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
                send_m(&ipd);
            }
        }

        void client_init(unsigned idx, size_t port, char* server_adr, unsigned server_port) {
            this_node_ = idx;
            init_sock_(port);
            nodes_ = new NodeInfo[1];
            nodes_[0].id = 0;
            nodes_[0].address.sin_family = AF_INET;
            nodes_[0].address.sin_port = htons(server_port);
            if (inet_pton(AF_INET, server_adr, &nodes_[0].address.sin_addr) <= 0)
                assert(false && "Invalid server IP address format");
            Register msg(idx, port, inet_ntoa(ip_.sin_addr));
            send_m(&msg);
            auto* ipd = dynamic_cast<Directory*>(recv_m());
            auto* nodes = new NodeInfo[num_nodes];
            for (size_t i = 0; i < ipd->clients; i += 1) {
                nodes[i + 1].id = i + 1;
                nodes[i + 1].address.sin_family = AF_INET;
                nodes[i + 1].address.sin_port = htons(ipd->ports[i]);
                if (inet_pton(AF_INET, ipd->addresses[i]->c_str(), &nodes[i + 1].address.sin_addr) <= 0)
                    assert(false);
            }
            delete[] nodes_;
            nodes_ = nodes;
            delete ipd;
        }

        void send_m(Message* msg) override {
            NodeInfo & tgt = nodes_[msg->target_];
            int conn = socket(AF_INET, SOCK_STREAM, 0);
            assert(conn >= 0 && "Unable to create client socket");
            if (connect(conn, (sockaddr*)&tgt.address, sizeof(tgt.address)) < 0)
                assert(false && "Unable to connect to remote node");
            size_t size = 0;
            char* buf = Serializer::serialize(msg, size);
            send(conn, &size, sizeof(size_t), 0);
            send(conn, buf, size, 0);
            close(conn);
        }

        Message* recv_m() override {
            sockaddr_in sender{};
            socklen_t addrlen = sizeof(sender);
            int req = accept(sock_, (sockaddr*)&sender, &addrlen);
            size_t size = 0;
            if (read(req, &size, sizeof(size_t)) == 0) assert(false && "failed to read");
            char* buf = new char[size];
            int rd = 0;
            while (rd != size) rd += read(req, buf + rd, size - rd);
            Message* msg = Serializer::deserializeMessage(buf);
            close(req);
            return msg;
        }
};