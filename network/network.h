#pragma once

#include <thread>
#include <mutex>
#include <map>
#include "network_ip.h"

class Key : public Object {
    public:
        String* key;
        size_t node;

        Key(String* strKey, size_t homeNode) : Object() {
            key = strKey;
            node = homeNode;
        }

        Key(const char* strKey, size_t homeNode) : Object() {
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
        std::map<std::string, Value*> kvStore;
        size_t index;
        NetworkIP* network;
        std::thread* pid;
        std::thread* pid2;
        std::thread** pids;
        size_t pid_index;
        std::mutex mtx;

        Distributable(size_t index_var) {
            index = index_var;
            network = new NetworkIP();
            pid = new std::thread(&Distributable::start, this);
            pid2 = new std::thread(&Distributable::startAndReceive, this);
            pid2->detach();
            pids = new std::thread*[5];
            pid_index = 0;
        }

        void start() {
            if (index == 0) {
                network->server_init(index, 9000);
            } else {
                network->client_init(index, 9000 + index, "127.0.0.1", 9000);
            }
        }

        void listen(int* sock) {
            Message* msg = network->recv_msg(*sock, true);
            size_t sender = msg->sender_;
            do {
                mtx.lock();
                if (msg->kind_ == MsgKind::Get) {
                    Get* get = dynamic_cast<Get*>(msg);
                    Value* val = kvStore.find(std::string(get->key))->second;
                    Chunk* chunk = new Chunk();
                    if (get->type == 'I') {
                        chunk->fi = dynamic_cast<FixedIntArray*>(val->obj);
                    } else if (get->type == 'F') {
                        chunk->ff = dynamic_cast<FixedFloatArray*>(val->obj);
                    } else if (get->type == 'B') {
                        chunk->fb = dynamic_cast<FixedBoolArray*>(val->obj);
                    } else {
                        chunk->fs = dynamic_cast<FixedStrArray*>(val->obj);
                    }
                    Send* send = new Send(chunk, get->type, get->key);
                    send->target_ = get->sender_;
                    send->sender_ = get->target_;
                    send->id_ = get->id_;
                    network->send_reply(send, true);
                    delete msg;
                    delete send;
                } else if (msg->kind_ == MsgKind::Send) {
                    Send* send = dynamic_cast<Send*>(msg);
                    Value* val = new Value();
                    if (send->type == 'I') {
                        val->obj = send->c->fi;
                    } else if (send->type == 'F') {
                        val->obj = send->c->ff;
                    } else if (send->type == 'B') {
                        val->obj = send->c->fb;
                    } else {
                        val->obj = send->c->fs;
                    }
                    kvStore[std::string(send->key)] = val;
                }
                mtx.unlock();
            } while ((msg = network->recv_msg(sender, true)));
        }

        void startAndReceive() {
            mtx.lock();
            pid->join();
            delete pid;
            mtx.unlock();
            for (;;) {
                int sock;
                network->accept_connection(sock);
                pids[pid_index] = new std::thread(&Distributable::listen, this, &sock);
                pid_index += 1;
            }
        }

        void sendToNode(Key* key, Value* value) {
            mtx.lock();
            if (key->node == index) {
                kvStore[std::string(key->key->c_str())] = value;
            } else {
                char type = 'F';
                Chunk* c = new Chunk();
                c->ff = dynamic_cast<FixedFloatArray*>(value->obj);
                Send* send = new Send(c, type, key->key->c_str());
                send->sender_ = index;
                send->target_ = key->node;
                std::cout<<send->key<<"\n";
                network->send_msg(send, true);
                delete send;
            }
            mtx.unlock();
        }

        Value* getFromNode(Key* key) {
            if (kvStore.find(key->key->c_str()) != kvStore.end()) {
                return kvStore.find(std::string(key->key->c_str()))->second;
            } else {
                Get* get = new Get('F', key->key->c_str());
                get->sender_ = index;
                get->target_ = key->node;
                network->send_msg(get, true);
                delete get;
                Message* msg = network->recv_reply(key->node, true);
                Send* send = dynamic_cast<Send*>(msg);
                Value* val = new Value();
                if (send->type == 'I') {
                    val->obj = send->c->fi;
                } else if (send->type == 'F') {
                    val->obj = send->c->ff;
                } else if (send->type == 'B') {
                    val->obj = send->c->fb;
                } else {
                    val->obj = send->c->fs;
                }
                kvStore[std::string(send->key)] = val;
                return val;
            }
        }

        ~Distributable() {
            delete pid2;
            for (size_t i = 0; i < 5; i += 1) {
                if (pids[i] != nullptr) {
                    pids[i]->join();
                    delete pids[i];
                }
            }
            delete[] pids;
            delete network;
        }
};

/*************************************************************************
 * DistEffIntArr:
 * Holds Ints in a distributed network
 */
class DistEffIntArr : public Object {
    public:
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;
        String* id;
        Distributable* kvStore;

        /**
         * @brief Construct a new Eff Col Arr object
         *
         */
        DistEffIntArr(String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = getSizeTFromKey("chunkSize");
            capacity = getSizeTFromKey("capacity");
            currentChunkIdx = getSizeTFromKey("currentChunk");
            numberOfElements = getSizeTFromKey("numElements");
        }

        size_t getSizeTFromKey(char* suffix) {
            String* idClone = id->clone();
            idClone->concat("-");
            idClone->concat(suffix);
            Key* sizeTKey = new Key(idClone, 0);
            size_t toReturn = kvStore->getFromNode(sizeTKey)->st;
            delete sizeTKey;
            return toReturn;
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         *
         * @param from
         */
        DistEffIntArr(EffIntArr& from, String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;


            kvStore->sendToNode(createKey("chunkSize"), createValue(chunkSize));
            kvStore->sendToNode(createKey("capacity"), createValue(capacity));
            kvStore->sendToNode(createKey("currentChunk"), createValue(currentChunkIdx));
            kvStore->sendToNode(createKey("numElements"), createValue(numberOfElements));
            for (size_t i = 0; i < capacity; i += 1) {
                char* buf = new char[length(i) + 1];
                sprintf(buf, "%zu", i);
                kvStore->sendToNode(createKey(buf), createValue(from.chunks[i]->clone()));
                delete[] buf;
            }
        }

        size_t length(size_t s) {
            size_t len = 1;
            s = s / 10;
            while (s) {
                s = s / 10;
                len += 1;
            }
            return len;
        }

        Key* createKey(char* suffix) {
            String* idClone = id->clone();
            idClone->concat("-");
            idClone->concat(suffix);
            return new Key(idClone, 0);
        }

        Value* createValue(size_t s) {
            auto* val = new Value;
            val->st = s;
            return val;
        }

        Value* createValue(FixedIntArray* fixedIntArray) {
            auto* val = new Value;
            val->obj = fixedIntArray;
            return val;
        }

        /**
         * @brief Get the column at the given index
         *
         * @param idx
         * @return Column*
         */
        int get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            char* buf = new char[length(idx) + 1];
            sprintf(buf, "%zu", chunkIdx);
            Value* val = kvStore->getFromNode(createKey(buf));
            Object* obj = val->obj;
            FixedIntArray* curChunk = dynamic_cast<FixedIntArray*>(obj);
            return curChunk->get(idx % chunkSize);
        }

        /**
         * @brief get the size of this column
         *
         * @return size_t
         */
        size_t size() {
            return numberOfElements;
        }

        /**
         * @brief Destroy the Eff Col Arr object
         *
         */
        ~DistEffIntArr() {
            delete id;
        }
};

/*************************************************************************
 * DistEffFloatArr:
 * Holds Floats in a distributed network
 */
class DistEffFloatArr : public Object {
    public:
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;
        String* id;
        Distributable* kvStore;

        /**
         * @brief Construct a new Eff Col Arr object
         *
         */
        DistEffFloatArr(String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = getSizeTFromKey("chunkSize");
            capacity = getSizeTFromKey("capacity");
            currentChunkIdx = getSizeTFromKey("currentChunk");
            numberOfElements = getSizeTFromKey("numElements");
        }

        size_t getSizeTFromKey(char* suffix) {
            String* idClone = id->clone();
            idClone->concat("-");
            idClone->concat(suffix);
            Key* sizeTKey = new Key(idClone, 0);
            size_t toReturn = kvStore->getFromNode(sizeTKey)->st;
            delete sizeTKey;
            return toReturn;
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         *
         * @param from
         */
        DistEffFloatArr(EffFloatArr& from, String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;


            kvStore->sendToNode(createKey("chunkSize", 0), createValue(chunkSize));
            kvStore->sendToNode(createKey("capacity", 0), createValue(capacity));
            kvStore->sendToNode(createKey("currentChunk", 0), createValue(currentChunkIdx));
            kvStore->sendToNode(createKey("numElements", 0), createValue(numberOfElements));
            for (size_t i = 0; i < capacity; i += 1) {
                char* buf = new char[length(i) + 1];
                sprintf(buf, "%zu", i);
                kvStore->sendToNode(createKey(buf, (i % 5)), createValue(from.chunks[i]->clone()));
                delete[] buf;
            }
        }

        size_t length(size_t s) {
            size_t len = 1;
            s = s / 10;
            while (s) {
                s = s / 10;
                len += 1;
            }
            return len;
        }

        Key* createKey(char* suffix, size_t node) {
            String* idClone = id->clone();
            idClone->concat("-");
            idClone->concat(suffix);
            return new Key(idClone, node);
        }

        Value* createValue(size_t s) {
            Value* val = new Value;
            val->st = s;
            return val;
        }

        Value* createValue(FixedFloatArray* fixedArray) {
            Value* val = new Value;
            val->obj = fixedArray;
            return val;
        }

        /**
         * @brief Get the column at the given index
         *
         * @param idx
         * @return Column*
         */
        float get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            char* buf = new char[length(idx) + 1];
            sprintf(buf, "%zu", chunkIdx);
            Value* val = kvStore->getFromNode(createKey(buf, (chunkIdx % 5)));
            Object* obj = val->obj;
            delete[] buf;
            FixedFloatArray* curChunk = dynamic_cast<FixedFloatArray*>(obj);
            return curChunk->get(idx % chunkSize);
        }

        /**
         * @brief get the size of this column
         *
         * @return size_t
         */
        size_t size() {
            return numberOfElements;
        }

        /**
         * @brief Destroy the Eff Col Arr object
         *
         */
        ~DistEffFloatArr() {
            delete id;
        }
};

/*************************************************************************
 * DistEffBoolArr:
 * Holds Bools in a distributed network
 */
class DistEffBoolArr : public Object {
    public:
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;
        String* id;
        Distributable* kvStore;

        /**
         * @brief Construct a new Eff Col Arr object
         *
         */
        DistEffBoolArr(String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = getSizeTFromKey("chunkSize");
            capacity = getSizeTFromKey("capacity");
            currentChunkIdx = getSizeTFromKey("currentChunk");
            numberOfElements = getSizeTFromKey("numElements");
        }

        size_t getSizeTFromKey(char* suffix) {
            String* idClone = id->clone();
            idClone->concat("-");
            idClone->concat(suffix);
            Key* sizeTKey = new Key(idClone, 0);
            size_t toReturn = kvStore->getFromNode(sizeTKey)->st;
            delete sizeTKey;
            return toReturn;
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         *
         * @param from
         */
        DistEffBoolArr(EffBoolArr& from, String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;


            kvStore->sendToNode(createKey("chunkSize"), createValue(chunkSize));
            kvStore->sendToNode(createKey("capacity"), createValue(capacity));
            kvStore->sendToNode(createKey("currentChunk"), createValue(currentChunkIdx));
            kvStore->sendToNode(createKey("numElements"), createValue(numberOfElements));
            for (size_t i = 0; i < capacity; i += 1) {
                char* buf = new char[length(i) + 1];
                sprintf(buf, "%zu", i);
                kvStore->sendToNode(createKey(buf), createValue(from.chunks[i]->clone()));
                delete[] buf;
            }
        }

        size_t length(size_t s) {
            size_t len = 1;
            s = s / 10;
            while (s) {
                s = s / 10;
                len += 1;
            }
            return len;
        }

        Key* createKey(char* suffix) {
            String* idClone = id->clone();
            idClone->concat("-");
            idClone->concat(suffix);
            return new Key(idClone, 0);
        }

        Value* createValue(size_t s) {
            Value* val = new Value;
            val->st = s;
            return val;
        }

        Value* createValue(FixedBoolArray* fixedArray) {
            Value* val = new Value;
            val->obj = fixedArray;
            return val;
        }

        /**
         * @brief Get the column at the given index
         *
         * @param idx
         * @return Column*
         */
        bool get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            char* buf = new char[length(idx) + 1];
            sprintf(buf, "%zu", chunkIdx);
            Value* val = kvStore->getFromNode(createKey(buf));
            delete[] buf;
            Object* obj = val->obj;
            FixedBoolArray* curChunk = dynamic_cast<FixedBoolArray*>(obj);
            return curChunk->get(idx % chunkSize);
        }

        /**
         * @brief get the size of this column
         *
         * @return size_t
         */
        size_t size() {
            return numberOfElements;
        }

        /**
         * @brief Destroy the Eff Col Arr object
         *
         */
        ~DistEffBoolArr() {
            delete id;
        }
};

/*************************************************************************
 * DistEffCharArr:
 * Holds Chars in a distributed network
 */
class DistEffCharArr : public Object {
    public:
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;
        String* id;
        Distributable* kvStore;

        /**
         * @brief Construct a new Eff Col Arr object
         *
         */
        DistEffCharArr(String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = getSizeTFromKey("chunkSize");
            capacity = getSizeTFromKey("capacity");
            currentChunkIdx = getSizeTFromKey("currentChunk");
            numberOfElements = getSizeTFromKey("numElements");
        }

        size_t getSizeTFromKey(char* suffix) {
            String* idClone = id->clone();
            idClone->concat("-");
            idClone->concat(suffix);
            Key* sizeTKey = new Key(idClone, 0);
            size_t toReturn = kvStore->getFromNode(sizeTKey)->st;
            delete sizeTKey;
            return toReturn;
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         *
         * @param from
         */
        DistEffCharArr(EffCharArr& from, String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;


            kvStore->sendToNode(createKey("chunkSize"), createValue(chunkSize));
            kvStore->sendToNode(createKey("capacity"), createValue(capacity));
            kvStore->sendToNode(createKey("currentChunk"), createValue(currentChunkIdx));
            kvStore->sendToNode(createKey("numElements"), createValue(numberOfElements));
            for (size_t i = 0; i < capacity; i += 1) {
                char* buf = new char[length(i) + 1];
                sprintf(buf, "%zu", i);
                kvStore->sendToNode(createKey(buf), createValue(from.chunks[i]->clone()));
                delete[] buf;
            }
        }

        size_t length(size_t s) {
            size_t len = 1;
            s = s / 10;
            while (s) {
                s = s / 10;
                len += 1;
            }
            return len;
        }

        Key* createKey(char* suffix) {
            String* idClone = id->clone();
            idClone->concat("-");
            idClone->concat(suffix);
            return new Key(idClone, 0);
        }

        Value* createValue(size_t s) {
            Value* val = new Value;
            val->st = s;
            return val;
        }

        Value* createValue(FixedCharArray* fixedArray) {
            Value* val = new Value;
            val->obj = fixedArray;
            return val;
        }

        /**
         * @brief Get the column at the given index
         *
         * @param idx
         * @return Column*
         */
        char get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            char* buf = new char[length(idx) + 1];
            sprintf(buf, "%zu", chunkIdx);
            Value* val = kvStore->getFromNode(createKey(buf));
            delete[] buf;
            Object* obj = val->obj;
            FixedCharArray* curChunk = dynamic_cast<FixedCharArray*>(obj);
            return curChunk->get(idx % chunkSize);
        }

        /**
         * @brief get the size of this column
         *
         * @return size_t
         */
        size_t size() {
            return numberOfElements;
        }

        /**
         * @brief Destroy the Eff Col Arr object
         *
         */
        ~DistEffCharArr() {
            delete id;
        }
};

/*************************************************************************
 * DistEffStrArr::
 * Holds String values in distributed nodes. Is unmodifiable.
 */
class DistEffStrArr : public Object {
    public:
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;
        String* id;
        Distributable* kvStore;

        DistEffStrArr(String* id_var, Distributable* kvStore_var) {
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = getSizeTFromKey("chunkSize");
            capacity = getSizeTFromKey("capacity");
            currentChunkIdx = getSizeTFromKey("currentChunk");
            numberOfElements = getSizeTFromKey("numElements");
        }

        size_t getSizeTFromKey(char* suffix) {
            String* idClone = id->clone();
            idClone->concat("-");
            idClone->concat(suffix);
            Key* sizeTKey = new Key(idClone, 0);
            size_t toReturn = kvStore->getFromNode(sizeTKey)->st;
            delete sizeTKey;
            return toReturn;
        }

        bool equals(Object* other) {
            DistEffStrArr* o = dynamic_cast<DistEffStrArr *> (other);
            if(o) {
                for(size_t i = 0; i < numberOfElements; i++) {
                    if(!get(i)->equals(o->get(i))) {
                        return false;
                    }
                }
            }
            return numberOfElements == o->numberOfElements;
        }

        DistEffStrArr(EffStrArr& from, String* id_var, Distributable* kvStore_var) {
            // this will basically send all the chunks to the various nodes
            // as well as the metadata, essentially storing the data of this
            // in the nodes
            id = id_var->clone();
            kvStore = kvStore_var;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;

            kvStore->sendToNode(createKey("chunkSize"), createValue(chunkSize));
            kvStore->sendToNode(createKey("capacity"), createValue(capacity));
            kvStore->sendToNode(createKey("currentChunk"), createValue(currentChunkIdx));
            kvStore->sendToNode(createKey("numElements"), createValue(numberOfElements));
            for (size_t i = 0; i < capacity; i += 1) {
                char* buf = new char[length(i) + 1];
                sprintf(buf, "%zu", i);
                kvStore->sendToNode(createKey(buf), createValue(from.chunks[i]));
                delete[] buf;
            }
        }

        size_t length(size_t s) {
            size_t len = 1;
            s = s / 10;
            while (s) {
                s = s / 10;
                len += 1;
            }
            return len;
        }

        Key* createKey(char* suffix) {
            String* idClone = id->clone();
            idClone->concat("-");
            idClone->concat(suffix);
            return new Key(idClone, 0);
        }

        Value* createValue(size_t s) {
            Value* val = new Value;
            val->st = s;
            return val;
        }

        Value* createValue(FixedStrArray* fixedStrArray) {
            Value* val = new Value;
            val->obj = fixedStrArray;
            return val;
        }

        String* get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            char* buf = new char[length(idx) + 1];
            sprintf(buf, "%zu", chunkIdx);
            FixedStrArray* curChunk = dynamic_cast<FixedStrArray*>(kvStore->getFromNode(createKey(buf))->obj);
            delete[] buf;
            return curChunk->get(idx % chunkSize);
        }

        size_t size() {
            return numberOfElements;
        }

        ~DistEffStrArr() {
            delete id;
        }
};