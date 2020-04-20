#pragma once
#include "../util/object.h"
#include "msgKind.h"
#include "../array/array.h"

class Message : public Object {

    // the message kind

    public:

        MsgKind kind_;

        size_t sender_; // the index of the sender node

        size_t target_; // the index of the receiver node

        size_t id_;     // an id t unique within the node

        Message() {}

        Message(MsgKind kind, size_t sender, size_t target, size_t id) {
            kind_ = kind;
            sender_ = sender;
            target_ = target;
            id_ = id;
        }

        virtual ~Message() {}

};

class Register : public Message {
    public:
        char* client;
        size_t port;

        Register(unsigned idx_, size_t port_, char* client_) {
            client = client_;
            port = port_;
            sender_ = idx_;
            target_ = 0;
            kind_ = MsgKind::Register;
        }
};


class Directory : public Message {
    public:
        size_t clients;
        size_t * ports;  // owned
        String ** addresses;  // owned; strings owned

        Directory(size_t clients_, size_t* ports_, String** addresses_) {
            clients = clients_;
            ports = ports_;
            addresses = addresses_;
            kind_ = MsgKind::Directory;
        }

        ~Directory() {
            delete[] ports;
            for (size_t i = 0; i < clients; i += 1) {
                delete addresses[i];
            }
            delete[] addresses;
        }
};

class Get : public Message {
    public:
        char type; // one of 'I', 'F', 'B', 'S'
        char* key;

        Get(char type_, char* key_) {
            type = type_;
            key = key_;
            kind_ = MsgKind::Get;
        }
};

/**
 * @brief A union for each type of fixed array
 *
 */

union Chunk  {
    FixedIntArray* fi;
    FixedFloatArray* ff;
    FixedBoolArray* fb;
    FixedStrArray* fs;
};

class Send : public Message {
    public:
        Chunk* c;
        char type;
        char* key;

        Send(Chunk* c_, char type_, char* key_) {
            c = c_;
            type = type_;
            key = key_;
            kind_ = MsgKind::Send;
        }
};