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
        char type; // one of 'I', 'F', 'B', 'S', 'T'
        String* key;

        Get(char type_, char* key_) {
            type = type_;
            key = new String(key_);
            kind_ = MsgKind::Get;
        }

        ~Get() {
            delete key;
        }
};

/**
 * @brief A union for each type of fixed array
 *
 */

union Data {
    size_t st;
    FixedIntArray* fi;
    FixedFloatArray* ff;
    FixedBoolArray* fb;
    FixedStrArray* fs;
};

class Transfer : public Object {
    public:
        Data* data;
        char type;

        Transfer(size_t var) {
            type = 'T';
            data = new Data();
            data->st = var;
        }

        Transfer(FixedIntArray* var) {
            type = 'I';
            data = new Data();
            data->fi = var;
        }

        Transfer(FixedFloatArray* var) {
            type = 'F';
            data = new Data();
            data->ff = var;
        }

        Transfer(FixedBoolArray* var) {
            type = 'B';
            data = new Data();
            data->fb = var;
        }

        Transfer(FixedStrArray* var) {
            type = 'S';
            data = new Data();
            data->fs = var;
        }

        ~Transfer() {
            if (type == 'I') {
                delete data->fi;
            } else if (type == 'F') {
                delete data->ff;
            } else if (type == 'B') {
                delete data->fb;
            } else if (type == 'S') {
                delete data->fs;
            }
            delete data;
        }
};

class Send : public Message {
    public:
        Transfer* transfer;
        String* key;

        Send(size_t var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
        }

        Send(FixedIntArray* var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
        }

        Send(FixedBoolArray* var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
        }

        Send(FixedFloatArray* var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
        }

        Send(FixedStrArray* var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
        }

        void delete_transfer() {
            delete transfer;
        }

        ~Send() {
            delete key;
        }

        char type() {
            return transfer->type;
        }
};