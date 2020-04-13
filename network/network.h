#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <poll.h>
#include <iostream>
#include "serial.h"
#include "../array/efficientArray.h"
#include <thread>
#include <map>

using namespace std;

class Key : public Object {
    public:
        String* key;
        int node;

        Key(String* strKey, int homeNode) : Object() {
            key = strKey;
            node = homeNode;
        }

        Key(char* strKey, int homeNode) : Object() {
            key = new String(strKey);
            node = homeNode;
        }

        bool equals(Object* o) {
            if (o == this) return true;
            Key* other = dynamic_cast<Key*>(o);
            if (other == nullptr) return false;
            return key->equals(other->key);
        }

        size_t hash_me() {
            return key->hash_me();
        }

        ~Key() {
            delete key;
        };
};

union Value {
    size_t st;
    Object* obj;
};

class Distributable : public Object {
    public:
        map<std::string, Value*> kvStore;
        int index;

        Distributable(int index_var) {
            index = index_var;
        }

        void sendToNode(Key* key, Value* value) {
            kvStore[std::string(key->key->c_str())] = value;
        }

        Value* getFromNode(Key* key) {
            Value* val = kvStore.find(std::string(key->key->c_str()))->second;
            return val;
        }

        ~Distributable() {

        }
};