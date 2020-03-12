#pragma once
#include "object.h"
#include "msgKind.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>

class Message : public Object {

    // the message kind

    public:

        MsgKind kind_;

        size_t sender_; // the index of the sender node

        size_t target_; // the index of the receiver node

        size_t id_;     // an id t unique within the node

        Message(MsgKind kind, size_t sender, size_t target, size_t id) {
            kind_ = kind;
            sender_ = sender;
            target_ = target;
            id_ = id;
        }

        char* as_char() {

        }

};

class Ack : public Message {

    public:


};


class Status : public Message {

    public:

        String* msg_; // owned



};


class Register : public Message {

    public:

        sockaddr_in client;
        size_t port;




};



class Directory : public Message {

    public:

        size_t client;

        size_t * ports;  // owned

        String ** addresses;  // owned; strings owned

};