#pragma once

#include <thread>
#include <condition_variable>
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

Key* createKey(String* prefix, const char* suffix, size_t node) {
    String* idClone = prefix->clone();
    idClone->concat("-");
    idClone->concat(suffix);
    return new Key(idClone, node);
}

Value* createValue(size_t s) {
    Value* val = new Value;
    val->st = s;
    return val;
}

Value* createValue(Object* o) {
    Value* val = new Value;
    val->obj = o;
    return val;
}

class Distributable : public Object {
    public:
        std::map<std::string, Transfer*> kvStore;
        size_t index;
        NetworkIP* network;
        std::thread* pid;
        std::thread* pid2;
        std::thread** pids;
        size_t pid_index;
        std::mutex mtx;
        std::condition_variable cv;
        bool ready = false;

        Distributable(size_t index_var) {
            index = index_var;
            network = new NetworkIP();
            pid = new std::thread(&Distributable::start, this);
            pid2 = new std::thread(&Distributable::startAndReceive, this);
            pids = new std::thread*[5];
            pid_index = 0;
        }

        void start() {
            std::unique_lock<std::mutex> lck(mtx);
            if (index == 0) {
                network->server_init(index, 9000);
            } else {
                network->client_init(index, 9000 + index, "127.0.0.1", 9000);
            }
            ready = true;
            lck.unlock();
            cv.notify_all();
        }

        void listen(int* sock) {
            Message* msg = network->recv_msg(*sock, true);
            size_t sender = msg->sender_;
            do {
                mtx.lock();
                if (msg->kind_ == MsgKind::Get) {
                    Get* get = dynamic_cast<Get*>(msg);
                    assert(kvStore.find(get->key->c_str()) != kvStore.end());
                    Transfer* val = kvStore.find(std::string(get->key->c_str()))->second;
                    assert(get->type == val->type);
                    Send* send;
                    if (get->type == 'T') {
                        send = new Send(val->data->st, get->key->c_str());
                    } else if (get->type == 'I') {
                        send = new Send(val->data->fi, get->key->c_str());
                    } else if (get->type == 'F') {
                        send = new Send(val->data->ff, get->key->c_str());
                    } else if (get->type == 'B') {
                        send = new Send(val->data->fb, get->key->c_str());
                    } else {
                        send = new Send(val->data->fs, get->key->c_str());
                    }
                    send->target_ = get->sender_;
                    send->sender_ = get->target_;
                    send->id_ = get->id_;
                    network->send_reply(send, true);
                    delete msg;
                    delete send;
                } else if (msg->kind_ == MsgKind::Send) {
                    Send* send = dynamic_cast<Send*>(msg);
                    kvStore[std::string(send->key->c_str())] = send->transfer;
                    delete send;
                }
                mtx.unlock();
            } while ((msg = network->recv_msg(sender, true)));
        }

        void startAndReceive() {
            std::unique_lock<std::mutex> lck(mtx);
            while (!ready) cv.wait(lck);
            lck.unlock();
            for (;;) {
                int sock;
                network->accept_connection(sock);
                pids[pid_index] = new std::thread(&Distributable::listen, this, &sock);
                pid_index += 1;
            }
        }

        void sendToNode(Key* key, Value* value) {
            std::unique_lock<std::mutex> lck(mtx);
            while (!ready) cv.wait(lck);
            lck.unlock();
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
            delete key;
        }
        
        size_t getSizeT(Key* key) {
            size_t s = 0;
            if (kvStore.find(key->key->c_str()) != kvStore.end()) {
                s = kvStore.find(std::string(key->key->c_str()))->second->st;
            }
            delete key;
            return s;
        }



        Value* getFromNode(Key* key) {
            Value* val;
            if (kvStore.find(key->key->c_str()) != kvStore.end()) {
                 val = kvStore.find(std::string(key->key->c_str()))->second;
            } else {
                Get* get = new Get('F', key->key->c_str());
                get->sender_ = index;
                get->target_ = key->node;
                network->send_msg(get, true);
                delete get;
                Message* msg = network->recv_reply(key->node, true);
                Send* send = dynamic_cast<Send*>(msg);
                val = new Value();
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
            delete key;
            return val;
        }

        ~Distributable() {
            for (std::map<std::string, Value*>::iterator itr = kvStore.begin(); itr != kvStore.end(); itr++) {
                delete (itr->second);
            }
            delete network;
            delete pid;
            delete pid2;
            for (size_t i = 0; i < 5; i += 1) {
                if (pids[i] != nullptr) {
                    pids[i]->join();
                    delete pids[i];
                }
            }
            delete[] pids;
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
            chunkSize = kvStore->getSizeT(createKey(id, "chunkSize", 0));
            capacity = kvStore->getSizeT(createKey(id, "capacity", 0));
            currentChunkIdx = kvStore->getSizeT(createKey(id, "currentChunk", 0));
            numberOfElements = kvStore->getSizeT(createKey(id, "numElements", 0));
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


            kvStore->sendToNode(createKey(id, "chunkSize", 0), createValue(chunkSize));
            kvStore->sendToNode(createKey(id, "capacity", 0), createValue(capacity));
            kvStore->sendToNode(createKey(id, "currentChunk", 0), createValue(currentChunkIdx));
            kvStore->sendToNode(createKey(id, "numElements", 0), createValue(numberOfElements));
            for (size_t i = 0; i < capacity; i += 1) {
                char* buf = new char[length(i) + 1];
                sprintf(buf, "%zu", i);
                kvStore->sendToNode(createKey(id, buf, i % 5), createValue(from.chunks[i]->clone()));
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
            Value* val = kvStore->getFromNode(createKey(id, buf, chunkIdx % 5));
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
            chunkSize = kvStore->getSizeT(createKey(id, "chunkSize", 0));
            capacity = kvStore->getSizeT(createKey(id, "capacity", 0));
            currentChunkIdx = kvStore->getSizeT(createKey(id, "currentChunk", 0));
            numberOfElements = kvStore->getSizeT(createKey(id, "numElements", 0));
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


            kvStore->sendToNode(createKey(id, "chunkSize", 0), createValue(chunkSize));
            kvStore->sendToNode(createKey(id, "capacity", 0), createValue(capacity));
            kvStore->sendToNode(createKey(id, "currentChunk", 0), createValue(currentChunkIdx));
            kvStore->sendToNode(createKey(id, "numElements", 0), createValue(numberOfElements));
            for (size_t i = 0; i < capacity; i += 1) {
                char* buf = new char[length(i) + 1];
                sprintf(buf, "%zu", i);
                kvStore->sendToNode(createKey(id, buf, (i % 5)), createValue(from.chunks[i]->clone()));
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
            Value* val = kvStore->getFromNode(createKey(id, buf, (chunkIdx % 5)));
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
            chunkSize = kvStore->getSizeT(createKey(id, "chunkSize", 0));
            capacity = kvStore->getSizeT(createKey(id, "capacity", 0));
            currentChunkIdx = kvStore->getSizeT(createKey(id, "currentChunk", 0));
            numberOfElements = kvStore->getSizeT(createKey(id, "numElements", 0));
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


            kvStore->sendToNode(createKey(id, "chunkSize", 0), createValue(chunkSize));
            kvStore->sendToNode(createKey(id, "capacity", 0), createValue(capacity));
            kvStore->sendToNode(createKey(id, "currentChunk", 0), createValue(currentChunkIdx));
            kvStore->sendToNode(createKey(id, "numElements", 0), createValue(numberOfElements));
            for (size_t i = 0; i < capacity; i += 1) {
                char* buf = new char[length(i) + 1];
                sprintf(buf, "%zu", i);
                kvStore->sendToNode(createKey(id, buf, i % 5), createValue(from.chunks[i]->clone()));
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
            Value* val = kvStore->getFromNode(createKey(id, buf, chunkIdx % 5));
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
            chunkSize = kvStore->getSizeT(createKey(id, "chunkSize", 0));
            capacity = kvStore->getSizeT(createKey(id, "capacity", 0));
            currentChunkIdx = kvStore->getSizeT(createKey(id, "currentChunk", 0));
            numberOfElements = kvStore->getSizeT(createKey(id, "numElements", 0));
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


            kvStore->sendToNode(createKey(id, "chunkSize", 0), createValue(chunkSize));
            kvStore->sendToNode(createKey(id, "capacity", 0), createValue(capacity));
            kvStore->sendToNode(createKey(id, "currentChunk", 0), createValue(currentChunkIdx));
            kvStore->sendToNode(createKey(id, "numElements", 0), createValue(numberOfElements));
            for (size_t i = 0; i < capacity; i += 1) {
                char* buf = new char[length(i) + 1];
                sprintf(buf, "%zu", i);
                kvStore->sendToNode(createKey(id, buf, i % 5), createValue(from.chunks[i]->clone()));
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
            Value* val = kvStore->getFromNode(createKey(id, buf, chunkIdx % 5));
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
            chunkSize = kvStore->getSizeT(createKey(id, "chunkSize", 0));
            capacity = kvStore->getSizeT(createKey(id, "capacity", 0));
            currentChunkIdx = kvStore->getSizeT(createKey(id, "currentChunk", 0));
            numberOfElements = kvStore->getSizeT(createKey(id, "numElements", 0));
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

            kvStore->sendToNode(createKey(id, "chunkSize", 0), createValue(chunkSize));
            kvStore->sendToNode(createKey(id, "capacity", 0), createValue(capacity));
            kvStore->sendToNode(createKey(id, "currentChunk", 0), createValue(currentChunkIdx));
            kvStore->sendToNode(createKey(id, "numElements", 0), createValue(numberOfElements));
            for (size_t i = 0; i < capacity; i += 1) {
                char* buf = new char[length(i) + 1];
                sprintf(buf, "%zu", i);
                kvStore->sendToNode(createKey(id, buf, i % 5), createValue(from.chunks[i]));
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

        String* get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            char* buf = new char[length(idx) + 1];
            sprintf(buf, "%zu", chunkIdx);
            FixedStrArray* curChunk = dynamic_cast<FixedStrArray*>(kvStore->getFromNode(createKey(id, buf, chunkIdx % 5))->obj);
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