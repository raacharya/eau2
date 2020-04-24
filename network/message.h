#pragma once
#include "../util/object.h"
#include "msgKind.h"
#include "../array/array.h"

class Message : public Object {
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

class Kill : public Message {
    public:

        Kill(size_t sender, size_t target, size_t id) :
        Message(MsgKind::Kill, sender, target, id) {}
};

class Ack : public Message {
    public:
        String* key_;

        Ack(size_t sender, size_t target, size_t id, const char* key) :
                Message(MsgKind::Ack, sender, target, id) {
            key_ = new String(key);
        }

        ~Ack() {
            delete key_;
        }
};

class Finished : public Message {
    public:
        String* key_;

        Finished(size_t sender, size_t target, size_t id, const char* key) :
        Message(MsgKind::Finished, sender, target, id) {
            key_ = new String(key);
        }

        ~Finished() {
            delete key_;
        }
};



class Register : public Message {
    public:
        String* client;
        size_t port;

        Register(unsigned idx_, size_t port_, const char* client_) {
            client = new String(client_);
            port = port_;
            sender_ = idx_;
            target_ = 0;
            id_ = 0;
            kind_ = MsgKind::Register;
        }

        ~Register() {
            delete client;
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
            id_ = 0;
            sender_ = 0;
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
        char type; // one of 'I', 'F', 'B', 'S', 'T', 'C'
        String* key;

        Get(char type_, const char* key_) {
            type = type_;
            key = new String(key_);
            kind_ = MsgKind::Get;
            id_ = 0;
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
    bool b;
    FixedIntArray* fi;
    FixedFloatArray* ff;
    FixedBoolArray* fb;
    FixedStrArray* fs;
    FixedCharArray* fc;
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

        Transfer(FixedCharArray* var) {
            type = 'C';
            data = new Data();
            data->fc = var;
        }

        Transfer(bool var) {
            type = 'U';
            data = new Data();
            data->b = var;
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
            } else if (type == 'C') {
                delete data->fc;
            }
            delete data;
        }

        size_t s_t() {
            assert(type == 'T');
            return data->st;
        }

        bool b() {
            assert(type == 'U');
            return data->b;
        }

        FixedIntArray* int_chunk() {
            assert(type == 'I');
            return data->fi;
        }

        FixedFloatArray* float_chunk() {
            assert(type == 'F');
            return data->ff;
        }

        FixedBoolArray* bool_chunk() {
            assert(type == 'B');
            return data->fb;
        }

        FixedStrArray* str_chunk() {
            assert(type == 'S');
            return data->fs;
        }

        FixedCharArray* char_chunk() {
            assert(type == 'C');
            return data->fc;
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
            id_ = 0;
        }

        Send(FixedIntArray* var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
            id_ = 0;
        }

        Send(FixedBoolArray* var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
            id_ = 0;
        }

        Send(FixedFloatArray* var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
            id_ = 0;
        }

        Send(FixedStrArray* var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
            id_ = 0;
        }

        Send(FixedCharArray* var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
            id_ = 0;
        }

        Send(bool var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = new Transfer(var);
            id_ = 0;
        }

        Send(Transfer* var, const char* key_) {
            kind_ = MsgKind::Send;
            key = new String(key_);
            transfer = var;
            id_ = 0;
        }

        ~Send() {
            delete key;
        }

        char type() {
            return transfer->type;
        }
};