//
// Created by Rahul Acharya on 2/25/20.
//
#pragma once
#include "object.h"
#include "efficientArray.h"
#include "message.h"
#include "msgKind.h"
#include <stdlib.h>
#include <sstream>

/**
 * Serializes different values of various data types. Makes the networking layer more efficient.
 */
class Serializer: public Object {

public:

    /**
     * Creates a serializer object
     */
    Serializer() {
    }


    /**
     * Destroys this serializer object
     */
    ~Serializer() {}

    /**
     * serialize the float
     * @param num - the float
     * @return - the serialized value
     */
    char* serialize(float num) {
        char* buffer = new char[sizeof(num)];
        memcpy(buffer, &num, sizeof(num));
        return buffer;
    }

    /**
     * serialize the size_t
     * @param num = the size_t
     * @return - the serialized value
     */
    char* serialize(size_t num) {
        char* buffer = new char[sizeof(num)];
        memcpy(buffer, &num, sizeof(num));
        return buffer;
    }

    /**
     * serialize the int
     * @param num = the int
     * @return - the serialized value
     */
    char* serialize(int num) {
        char* buffer = new char[sizeof(num)];
        memcpy(buffer, &num, sizeof(num));
        return buffer;
    }

    /**buffer[];
     * Serializes a fixed string array
     * @param arr - the array
     * @return
     */
    char* serializeFixedStrArray(FixedStrArray* arr) {
        size_t bufferSize = arr->numElements() + 1;

        for(size_t i = 0; i < arr->numElements(); i++) {
            bufferSize += arr->get(i)->size();
        }

        char* buffer = new char[bufferSize];
        buffer[0] = arr->size();

        size_t index = 1;
        size_t word = 0;

        while(index < bufferSize) {
            String* currString = arr->get(word);
            if(index != bufferSize - 1) {
                buffer[index] = currString->size();
                index += 1;
            }
            for(size_t i = 0; i < currString->size(); i++) {
                buffer[index] = currString->at(i);
                index += 1;
            }
            word += 1;
        }
        return buffer;
    }

    /**
     * Serialize this fixed int array
     * @param arr
     * @return
     */
    char* serializeFixedIntArray(FixedIntArray* arr) {
        size_t bufferSize = arr->used + 1;

        for(size_t i = 0; i < arr->used; i++) {
            bufferSize += sizeof(arr->get(i));
        }

        char* buffer = new char[bufferSize];
        buffer[0] = arr->used;

        size_t index = 1;
        size_t word = 0;

        while(index < bufferSize) {
            int currInt = arr->get(word);
            if(index != bufferSize - 1) {
                buffer[index] = sizeof(currInt);
                index += 1;
            }
            char* serialized = serialize(currInt);
            for(size_t i = 0; i < sizeof(currInt); i++) {
                buffer[index] = serialized[i];
                index += 1;
            }
            word += 1;
        }
        return buffer;
    }

    /**
     * Serialize this fixed float array
     * @param arr
     * @return
     */
    char* serializeFixedFloatArray(FixedFloatArray* arr) {
        size_t bufferSize = arr->used + 1;

        for(size_t i = 0; i < arr->used; i++) {
            bufferSize += sizeof(arr->get(i));
        }

        char* buffer = new char[bufferSize];
        buffer[0] = arr->used;

        size_t index = 1;
        size_t word = 0;

        while(index < bufferSize) {
            float currFloat = arr->get(word);
            if(index != bufferSize - 1) {
                buffer[index] = sizeof(currFloat);
                index += 1;
            }
            char* serialized = serialize(currFloat);
            for(size_t i = 0; i < sizeof(currFloat); i++) {
                buffer[index] = serialized[i];
                index += 1;
            }
            word += 1;
        }
        return buffer;
    }

    /**
     * Serialize this fixed boolean array
     * @param arr
     * @return
     */
    char* serializeFixedBoolArray(FixedBoolArray* arr) {
        size_t bufferSize = arr->used + 1;

        char* buffer = new char[bufferSize];

        buffer[0] = arr->used;

        for(size_t i = 1; i < arr->used; i++) {
            char next;
            if(arr->get(i)) {
                next = 1;
            }
            else {
                next = 0;
            }
            buffer[i] = next;
        }

        buffer[strlen(buffer) - 1] = '\0';

        return buffer;
    }

    /**
     * serialize the array of strings
     * @param arr - the array
     * @return - the serialized array
     */
    char* serialize(EffStrArr* arr) {

        size_t bufferSize = arr->numberOfElements;

        for(size_t i = 0; i < arr->numberOfElements; i++) {
            bufferSize += arr->get(i)->size();
        }

        char* buffer = new char[bufferSize];

        size_t index = 0;
        size_t word = 0;

        while(index < bufferSize) {
            String* currString = arr->get(word);
            buffer[index] = currString->size();
            index += 1;
            for(size_t i = 0; i < currString->size(); i++) {
                buffer[index] = currString->at(i);
                index += 1;
            }
            word += 1;
        }
        return buffer;
    }

    /**
     * deserialize the serialized string arrray
     * @param buffer - the seriaized form of the string array
     * @return - the string array in its original format
     */
    EffStrArr* deserializeStringArr(char* buffer) {
        size_t index = 0;
        char* curr;
        EffStrArr* arr = new EffStrArr();
        size_t total = strlen(buffer);

        while(index < total) {
            size_t iter = buffer[index];
            index += 1;
            curr = new char[iter];
            for(size_t i = 0; i < iter; i++) {
                curr[i] = buffer[index];
                index += 1;
            }
            arr->pushBack(new String(curr));
        }

        return arr;
    }


    FixedStrArray* deserializeFixedStringArr(char* buffer) {
        size_t index = 1;
        char* curr;
        FixedStrArray* arr = new FixedStrArray(buffer[0]);
        size_t total = strlen(buffer - 1);

        while(index < total) {
            size_t iter = buffer[index];
            index += 1;
            curr = new char[iter];
            for(size_t i = 0; i < iter; i++) {
                curr[i] = buffer[index];
                index += 1;
            }
            arr->pushBack(new String(curr));
        }

        return arr;
    }


    FixedIntArray* deserializeFixedIntArr(char* buffer) {
        size_t index = 1;
        char* curr;
        FixedIntArray* arr = new FixedIntArray(buffer[0]);
        size_t total = strlen(buffer-1);

        while(index < total) {
            size_t iter = buffer[index];
            index += 1;
            curr = new char[iter];
            for(size_t i = 0; i < iter; i++) {
                curr[i] = buffer[index];
                index += 1;
            }
            arr->pushBack(*reinterpret_cast<int *> (curr));
        }

        return arr;
    }

    FixedIntArray* deserializeFixedBoolArr(char* buffer) {
        FixedBoolArray* arr = new FixedBoolArray(buffer[0]);

        size_t total = strlen(buffer-1);

        for(size_t i = 1; i < total; i++) {
            if(buffer[i] == '0') {
                arr->pushBack(false);
            } else {
                arr->pushBack(true);
            }
        }
    }

    FixedFloatArray* deserializeFixedFloatArr(char* buffer) {
        size_t index = 1;
        char* curr;
        FixedFloatArray* arr = new FixedFloatArray(buffer[0]);
        size_t total = strlen(buffer-1);

        while(index < total) {
            size_t iter = buffer[index];
            index += 1;
            curr = new char[iter];
            for(size_t i = 0; i < iter; i++) {
                curr[i] = buffer[index];
                index += 1;
            }
            arr->pushBack(*reinterpret_cast<float *> (curr));
        }

        return arr;
    }

    /**
     * serialize the float array
     * @param arr - the array
     * @return - the serialized float array
     */
    char* serialize(EffFloatArr* arr) {

        size_t bufferSize = arr->numberOfElements;

        for(size_t i = 0; i < arr->numberOfElements; i++) {
            bufferSize += sizeof(arr->get(i));
        }

        char* buffer = new char[bufferSize];

        size_t index = 0;
        size_t word = 0;

        while(index < bufferSize) {
            float currFloat = arr->get(word);
            buffer[index] = sizeof(currFloat);
            index += 1;

            char* serializedFloat = serialize(currFloat);

            for(size_t i = 0; i < strlen(serializedFloat); i++) {
                buffer[index] = serializedFloat[i];
                index += 1;
            }

            word += 1;
        }
        return buffer;
    }

    /**
     * deserialize the serialized float array
     * @param buffer - the serialized array
     * @return - the original float array
     */
    EffFloatArr* deserializeFloatArr(char* buffer) {
        size_t index = 0;
        char* curr;
        EffFloatArr* arr = new EffFloatArr();
        size_t total = strlen(buffer);

        while(index < total) {
            size_t iter = buffer[index];
            index += 1;
            curr = new char[iter];
            for(size_t i = 0; i < iter; i++) {
                curr[i] = buffer[index];
                index += 1;
            }
            arr->pushBack(*reinterpret_cast<float *> (curr));
        }

        return arr;
    }

    /**
     * Serialize the given message
     * @param m - the message
     * @return - the serialized message
     */
    char* serialize(Message* m) {
        if (m->kind_ == MsgKind::Directory) {
            char* buf = serializeDirectory(dynamic_cast<Directory*>(m));
            return buf;
        } else if (m->kind_ == MsgKind::Register) {
            char* buf = serializeRegister(dynamic_cast<Register*>(m));
            return buf;
        }
    }

    char* serializeSend(Send* s) {
        std::ostringstream os;
        os<<"S";
        os<<len(s->sender_);
        os<<"|";
        os<<s->sender_;
        os<<len(s->target_);
        os<<"|";
        os<<s->target_;
        os<<len(s->id_);
        os<<"|";
        os<<s->id_;
        char* type = s->type;
        char* serializedChunk;
        if(strcmp(type, "I") == 0) {
            serializedChunk = serializeFixedIntArray(s->c.fi);
        } else if(strcmp(type, "F") == 0) {
            serializedChunk = serializeFixedFloatArray(s->c.ff);
        } else if(strcmp(type, "B") == 0) {
            serializedChunk = serializeFixedBoolArray(s->c.fb);
        } else if(strcmp(type, "S") == 0) {
            serializedChunk = serializeFixedStrArray(s->c.fs);
        }

        os<<1;
        os<<"|";
        os<<s->type;
        os<<strlen(serializedChunk);
        os<<"|";
        os<<serializedChunk;
        std::string data = os.str();
        char* buffer = strdup(data.c_str());
        return buffer;
    }

    char* serializeGet(Get* m) {
        std::ostringstream os;
        os<<"G";
        os<<len(m->sender_);
        os<<"|";
        os<<m->sender_;
        os<<len(m->target_);
        os<<"|";
        os<<m->target_;
        os<<len(m->id_);
        os<<"|";
        os<<m->id_;
        os<<(strlen(m->type));
        os<<"|";
        os<<(m->type);
        os<<(strlen(m->key));
        os<<("|");
        os<<(m->key);

        std::string data = os.str();
        char* buffer = strdup(data.c_str());
        return buffer;
    }

    char* serializeRegister(Register* m) {
        std::ostringstream os;
        os<<"R";
        os<<len(m->sender_);
        os<<"|";
        os<<m->sender_;
        os<<len(m->target_);
        os<<"|";
        os<<m->target_;
        os<<len(m->id_);
        os<<"|";
        os<<m->id_;
        os<<strlen(m->client);
        os<<"|";
        os<<m->client;
        os<<len(m->port);
        os<<"|";
        os<<m->port;
        std::string data = os.str();
        char* buffer = strdup(data.c_str());
        return buffer;
    }

    char* serializeDirectory(Directory* m) {
        std::ostringstream os;
        os<<"D";
        os<<len(m->sender_);
        os<<"|";
        os<<m->sender_;
        os<<len(m->target_);
        os<<"|";
        os<<m->target_;
        os<<len(m->id_);
        os<<"|";
        os<<m->id_;
        os<<len(m->clients);
        os<<"|";
        os<<m->clients;
        for (size_t i = 0; i < m->clients; i += 1) {
            os<<len(m->ports[i]);
            os<<"|";
            os<<m->ports[i];
        }
        for (size_t i = 0; i < m->clients; i += 1) {
            os<<m->addresses[i]->size();
            os<<"|";
            os<<m->addresses[i]->c_str();
        }
        std::string data = os.str();
        char* buffer = strdup(data.c_str());
        return buffer;
    }

    size_t len(size_t num) {
        size_t len = 1;
        num /= 10;
        while (num > 0) {
            len += 1;
            num /= 10;
        }
        return len;
    }

    /**
     * Deserialize a message - we still have to implement some functionality for the other types, but this works for
     * ACK, for example. We ar just demonstrating use cases right now.
     * @param buffer
     * @return
     */
    Message* deserializeMessage(char* buffer) {
        if (buffer[0] == 'D') {
            return deserializeDirectory(buffer);
        } else if (buffer[0] == 'R') {
            return deserializeRegister(buffer);
        }
    }

    Get* deserializeGet(char* buffer) {
        size_t curIndex = 1;
        std::string ser = buffer;
        size_t sender = nextSizeT(ser, curIndex);
        size_t target = nextSizeT(ser, curIndex);
        size_t id = nextSizeT(ser, curIndex);
        std::string type = nextString(ser, curIndex);
        std::string key = nextString(ser, curIndex);
        char* t = strdup(type.c_str());
        char* k = strdup(key.c_str());
        Get* g = new Get(t, k);
        g->sender_ = sender;
        g->target_ = target;
        g->id_ = id;
        return g;
    }

    Register* deserializeRegister(char* buffer) {
        size_t curIndex = 1;
        std::string ser = buffer;
        size_t sender = nextSizeT(ser, curIndex);
        size_t target = nextSizeT(ser, curIndex);
        size_t id = nextSizeT(ser, curIndex);
        std::string address = nextString(ser, curIndex);
        char* client = strdup(address.c_str());
        size_t port = nextSizeT(ser, curIndex);
        Register* r = new Register(sender, port, client);
        r->id_ = id;
        return r;
    }

    Directory* deserializeDirectory(char* buffer) {
        size_t curIndex = 1;
        std::string ser = buffer;
        size_t sender = nextSizeT(ser, curIndex);
        size_t target = nextSizeT(ser, curIndex);
        size_t id = nextSizeT(ser, curIndex);
        size_t clients = nextSizeT(ser, curIndex);

        size_t* ports = new size_t[clients];
        for (size_t i = 0; i < clients; i += 1) {
            size_t port = nextSizeT(ser, curIndex);
            ports[i] = port;
        }
        String** addresses = new String*[clients];
        for (size_t i = 0; i < clients; i += 1) {
            std::string address = nextString(ser, curIndex);
            addresses[i] = new String(address.c_str());
        }
        Directory* dir = new Directory(clients, ports, addresses);
        dir->sender_ = sender;
        dir->target_ = target;
        dir->id_ = id;
        return dir;
    }

    size_t nextSizeT(const std::string& str, size_t& curIndex) {
        size_t subLen = len(str, curIndex);
        curIndex = str.find("|", curIndex) + 1;
        size_t next = num(str, curIndex, subLen);
        curIndex += subLen;
        return next;
    }

    std::string nextString(const std::string& str, size_t& curIndex) {
        size_t subLen = len(str, curIndex);
        curIndex = str.find("|", curIndex) + 1;
        std::string next = str.substr(curIndex, subLen);
        curIndex += subLen;
        return next;
    }

    size_t len(const std::string& str, size_t ind) {
        size_t endIndex = str.find("|", ind);
        std::string sub = str.substr(ind, endIndex);
        size_t len = std::stoi(sub);
        return len;
    }

    size_t num(const std::string& str, size_t beg, size_t end) {
        std::string sub = str.substr(beg, end);
        size_t num = std::stoi(sub);
        return num;
    }
};
