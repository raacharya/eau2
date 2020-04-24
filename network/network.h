#pragma once

#include <thread>
#include <condition_variable>
#include <mutex>
#include <map>
#include <set>
#include "network_ip.h"

class Key : public Object {
    public:
        String* key;
        size_t node;

        Key(const char* strKey, size_t homeNode) : Object() {
            key = new String(strKey);
            node = homeNode;
        }

        ~Key() {
            delete key;
        };

        bool equals(Object* o) {
            if (o == this) return true;
            Key* other = dynamic_cast<Key*>(o);
            if (other == nullptr) return false;
            return key->equals(other->key);
        }

        size_t hash_me() {
            return key->hash_me();
        }
};

class Distributable : public Object {
    public:
        std::map<std::string, Transfer*> kvStore;
        size_t index;
        NetworkIP* network;
        std::thread accept_conn_pid;
        std::thread* individual_conns;
        size_t pid_index;
        std::mutex handshake_lock;
        std::condition_variable handshake_cond;
        bool handshake_done = false;
        std::mutex init_sock_lock;
        std::condition_variable init_sock_cond;
        std::set<std::string> completed_dfs;
        std::mutex complete_df_lock;
        std::condition_variable complete_df_cond;
        std::mutex map_lock;

        Distributable(size_t index_var) {
            index = index_var;
            network = new NetworkIP(&init_sock_lock, &init_sock_cond);
            accept_conn_pid = std::thread(&Distributable::start, this);
            if (index == 0) {
                std::unique_lock<std::mutex> lck(init_sock_lock);
                while (!network->init_sock_done) init_sock_cond.wait(lck);
                lck.unlock();
            }
            individual_conns = new std::thread[5];
            pid_index = 0;
        }

        void start() {
            std::unique_lock<std::mutex> lck(handshake_lock);
            if (index == 0) {
                network->server_init(index, 9000);
            } else {
                network->client_init(index, 9000 + index, "127.0.0.1", 9000);
            }
            handshake_done = true;
            lck.unlock();
            handshake_cond.notify_all();
            for (;;) {
                int sock;
                network->accept_connection(sock);
                if (sock == -1) break;
                Message* msg = network->recv_msg(sock, true);
                if (msg->kind_ == MsgKind::Kill) {
                    delete msg;
                    break;
                } else if (msg->kind_ == MsgKind::Finished) {
                    Finished* finished = dynamic_cast<Finished*>(msg);
                    std::unique_lock<std::mutex> df_lock(complete_df_lock);
                    completed_dfs.insert(std::string(finished->key_->c_str()));
                    df_lock.unlock();
                    complete_df_cond.notify_all();
                    delete finished;
                    continue;
                }
                individual_conns[pid_index] = std::thread(&Distributable::listen, this, msg);
                pid_index += 1;

            }
        }

        void listen(Message* msg) {
            size_t sender = msg->sender_;
            do {
                if (msg->kind_ == MsgKind::Get) {
                    Get* get = dynamic_cast<Get*>(msg);
                    map_lock.lock();
                    assert(kvStore.find(std::string(get->key->c_str())) != kvStore.end());
                    Transfer* val = kvStore.find(std::string(get->key->c_str()))->second;
                    map_lock.unlock();
                    assert(get->type == val->type);
                    Send* send = new Send(val, get->key->c_str());
                    send->target_ = get->sender_;
                    send->sender_ = get->target_;
                    send->id_ = get->id_;
                    network->send_reply(send, true);
                    delete get;
                    delete send;
                } else if (msg->kind_ == MsgKind::Send) {
                    Send* send = dynamic_cast<Send*>(msg);
                    map_lock.lock();
                    if (kvStore.find(std::string(send->key->c_str())) != kvStore.end()) {
                        delete kvStore[std::string(send->key->c_str())];
                    }
                    kvStore[std::string(send->key->c_str())] = send->transfer;
                    map_lock.unlock();
                    Ack* ack = new Ack(send->target_, send->sender_, 0, send->key->c_str());
                    delete send;
                    network->send_reply(ack, true);
                    delete ack;
                } else if (msg->kind_ == MsgKind::Finished) {
                    Finished* finished = dynamic_cast<Finished*>(msg);
                    std::unique_lock<std::mutex> df_lock(complete_df_lock);
                    completed_dfs.insert(std::string(finished->key_->c_str()));
                    df_lock.unlock();
                    complete_df_cond.notify_all();
                    delete finished;
                    continue;
                }
            } while ((msg = network->recv_msg(sender, true)) != nullptr);
        }

        void send_finished_update(const char* key) {
            for (size_t i = 0; i < 5; i += 1) {
                if (index != i) {
                    Finished* finished = new Finished(index, i, 0, key);
                    network->send_msg(finished, true);
                    delete finished;
                } else {
                    completed_dfs.insert(std::string(key));
                }
            }
        }

        void put(size_t node, String* key, size_t val) {
            put_(node, key, new Transfer(val));
        }

        void put(size_t node, String* key, bool val) {
            put_(node, key, new Transfer(val));
        }

        void put(size_t node, String* key, FixedIntArray* val) {
            put_(node, key, new Transfer(val));
        }

        void put(size_t node, String* key, FixedBoolArray* val) {
            put_(node, key, new Transfer(val));
        }

        void put(size_t node, String* key, FixedFloatArray* val) {
            put_(node, key, new Transfer(val));
        }

        void put(size_t node, String* key, FixedStrArray* val) {
            put_(node, key, new Transfer(val));
        }

        void put(size_t node, String* key, FixedCharArray* val) {
            put_(node, key, new Transfer(val));
        }

        void put_(size_t node, String* key, Transfer* transfer) {
            std::unique_lock<std::mutex> lck(handshake_lock);
            while (!handshake_done) handshake_cond.wait(lck);
            lck.unlock();
            if (node == index) {
                map_lock.lock();
                kvStore[std::string(key->c_str())] = transfer;
                map_lock.unlock();
            } else {
                Send* send = new Send(transfer, key->c_str());
                send->sender_ = index;
                send->target_ = node;
                network->send_msg(send, true);
                Ack* msg = dynamic_cast<Ack*>(network->recv_reply(node, true));
                assert(msg->key_->equals(send->key));
                delete transfer;
                delete send;
            }
            delete key;
        }
        
        size_t get_size_t(size_t node, String* key) {
            Transfer* transfer = get_(node, 'T', key);
            return transfer->s_t();
        }

        bool get_bool(size_t node, String* key) {
            Transfer* transfer = get_(node, 'U', key);
            return transfer->b();
        }

        FixedIntArray* get_int_chunk(size_t node, String* key) {
            Transfer* transfer = get_(node, 'I', key);
            return transfer->int_chunk();
        }

        FixedFloatArray* get_float_chunk(size_t node, String* key) {
            Transfer* transfer = get_(node, 'F', key);
            return transfer->float_chunk();
        }

        FixedBoolArray* get_bool_chunk(size_t node, String* key) {
            Transfer* transfer = get_(node, 'B', key);
            return transfer->bool_chunk();
        }

        FixedStrArray* get_str_chunk(size_t node, String* key) {
            Transfer* transfer = get_(node, 'S', key);
            return transfer->str_chunk();
        }

        FixedCharArray* get_char_chunk(size_t node, String* key) {
            Transfer* transfer = get_(node, 'C', key);
            return transfer->char_chunk();
        }

        Transfer* get_(size_t node, char type, String* key) {
            Transfer* transfer;
            map_lock.lock();
            if (kvStore.find(std::string(key->c_str())) != kvStore.end()) {
                transfer = kvStore.find(std::string(key->c_str()))->second;
            } else {
                Get* get = new Get(type, key->c_str());
                get->sender_ = index;
                get->target_ = node;
                network->send_msg(get, true);
                delete get;
                Message* msg = network->recv_reply(node, true);
                Send* send = dynamic_cast<Send*>(msg);
                transfer = send->transfer;
                kvStore[std::string(key->c_str())] = transfer;
                delete send;
            }
            map_lock.unlock();
            delete key;
            assert(type == transfer->type);
            return transfer;
        }

        ~Distributable() {
            std::cout<<"deleting\n";
            for (std::map<std::string, Transfer*>::iterator itr = kvStore.begin(); itr != kvStore.end(); itr++) {
                delete (itr->second);
            }
            network->shutdown();
            std::cout<<"shutdown\n";
            if (accept_conn_pid.joinable()) accept_conn_pid.join();
            std::cout<<"start\n";
            delete network;
            std::cout<<"network\n";
            for (size_t i = 0; i < 5; i += 1) {
                if (individual_conns[i].joinable()) individual_conns[i].join();
                std::cout<<i<<"\n";
            }
            delete[] individual_conns;
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
        size_t metadata_node;
        FixedIntArray* current_chunk;

        DistEffIntArr(String* id_var, Distributable* kvStore_var, size_t node, bool get) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = get ? kvStore->get_size_t(node, id->clone()->concat("-chunkSize")) : 50;
            capacity = get ? kvStore->get_size_t(node, id->clone()->concat("-capacity")) : 1;
            currentChunkIdx = get ? kvStore->get_size_t(node, id->clone()->concat("-currentChunk")) : 0;
            numberOfElements = get ? kvStore->get_size_t(node, id->clone()->concat("-numElements")) : 0;
            current_chunk = get ? nullptr : new FixedIntArray(chunkSize);
        }

        /**
         * @brief Construct a new Eff Col Arr object
         *
         */
        DistEffIntArr(String* id_var, Distributable* kvStore_var, size_t node) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = kvStore->get_size_t(metadata_node, id->clone()->concat("-chunkSize"));
            capacity = kvStore->get_size_t(metadata_node, id->clone()->concat("-capacity"));
            currentChunkIdx = kvStore->get_size_t(metadata_node, id->clone()->concat("-currentChunk"));
            numberOfElements = kvStore->get_size_t(metadata_node, id->clone()->concat("-numElements"));
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         *
         * @param from
         */
        DistEffIntArr(EffIntArr& from, String* id_var, Distributable* kvStore_var, size_t node) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            kvStore->put(metadata_node, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(metadata_node, id->clone()->concat("-capacity"), capacity);
            kvStore->put(metadata_node, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(metadata_node, id->clone()->concat("-numElements"), numberOfElements);
            for (size_t i = 0; i < capacity; i += 1) {
                kvStore->put(i % 5, id->clone()->concat("-")->concat(i), from.chunks[i]->clone());
            }
        }

        /**
         * @brief Get the column at the given index
         *
         * @param idx
         * @return Column*
         */
        int get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            FixedIntArray* curChunk = kvStore->get_int_chunk(chunkIdx % 5, id->clone()->concat("-")->concat(chunkIdx));
            return curChunk->get(idx % chunkSize);
        }

        FixedIntArray* get_chunk(size_t chunkIdx) {
            return kvStore->get_int_chunk(chunkIdx % 5, id->clone()->concat("-")->concat(chunkIdx));
        }

        /**
         * @brief get the size of this column
         *
         * @return size_t
         */
        size_t size() {
            return numberOfElements;
        }

        void push_back(int val) {
            assert(current_chunk != nullptr);
            current_chunk->pushBack(val);
            numberOfElements += 1;
            if (current_chunk->capacity == current_chunk->used) {
                kvStore->put(currentChunkIdx % 5, id->clone()->concat("-")->concat(currentChunkIdx), current_chunk);
                currentChunkIdx += 1;
                current_chunk = new FixedIntArray(chunkSize);
            }
        }

        void lock() {
            kvStore->put(metadata_node, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(metadata_node, id->clone()->concat("-capacity"), capacity);
            kvStore->put(metadata_node, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(metadata_node, id->clone()->concat("-numElements"), numberOfElements);
            if (current_chunk->used > 0) {
                kvStore->put(currentChunkIdx % 5, id->clone()->concat("-")->concat(currentChunkIdx), current_chunk);
            } else {
                delete current_chunk;
            }
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
        size_t metadata_node;
        FixedFloatArray* current_chunk;

        DistEffFloatArr(String* id_var, Distributable* kvStore_var, size_t node, bool get) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = get ? kvStore->get_size_t(node, id->clone()->concat("-chunkSize")) : 50;
            capacity = get ? kvStore->get_size_t(node, id->clone()->concat("-capacity")) : 1;
            currentChunkIdx = get ? kvStore->get_size_t(node, id->clone()->concat("-currentChunk")) : 0;
            numberOfElements = get ? kvStore->get_size_t(node, id->clone()->concat("-numElements")) : 0;
            current_chunk = get ? nullptr : new FixedFloatArray(chunkSize);
        }

        /**
         * @brief Construct a new Eff Col Arr object
         *
         */
        DistEffFloatArr(String* id_var, Distributable* kvStore_var, size_t node) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = kvStore->get_size_t(metadata_node, id->clone()->concat("-chunkSize"));
            capacity = kvStore->get_size_t(metadata_node, id->clone()->concat("-capacity"));
            currentChunkIdx = kvStore->get_size_t(metadata_node, id->clone()->concat("-currentChunk"));
            numberOfElements = kvStore->get_size_t(metadata_node, id->clone()->concat("-numElements"));
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         *
         * @param from
         */
        DistEffFloatArr(EffFloatArr& from, String* id_var, Distributable* kvStore_var, size_t node) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            kvStore->put(metadata_node, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(metadata_node, id->clone()->concat("-capacity"), capacity);
            kvStore->put(metadata_node, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(metadata_node, id->clone()->concat("-numElements"), numberOfElements);
            for (size_t i = 0; i < capacity; i += 1) {
                kvStore->put(i % 5, id->clone()->concat("-")->concat(i), from.chunks[i]->clone());
            }
        }

        /**
         * @brief Get the column at the given index
         *
         * @param idx
         * @return Column*
         */
        float get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            FixedFloatArray* curChunk = kvStore->get_float_chunk(chunkIdx % 5, id->clone()->concat("-")->concat(chunkIdx));
            return curChunk->get(idx % chunkSize);
        }

        FixedFloatArray* get_chunk(size_t chunkIdx) {
            return kvStore->get_float_chunk(chunkIdx % 5, id->clone()->concat("-")->concat(chunkIdx));
        }

        /**
         * @brief get the size of this column
         *
         * @return size_t
         */
        size_t size() {
            return numberOfElements;
        }

        void push_back(float val) {
            assert(current_chunk != nullptr);
            current_chunk->pushBack(val);
            numberOfElements += 1;
            if (current_chunk->capacity == current_chunk->used) {
                kvStore->put(currentChunkIdx % 5, id->clone()->concat("-")->concat(currentChunkIdx), current_chunk);
                currentChunkIdx += 1;
                current_chunk = new FixedFloatArray(chunkSize);
            }
        }

        void lock() {
            kvStore->put(metadata_node, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(metadata_node, id->clone()->concat("-capacity"), capacity);
            kvStore->put(metadata_node, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(metadata_node, id->clone()->concat("-numElements"), numberOfElements);
            if (current_chunk->used > 0) {
                kvStore->put(currentChunkIdx % 5, id->clone()->concat("-")->concat(currentChunkIdx), current_chunk);
            } else {
                delete current_chunk;
            }
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
        size_t metadata_node;
        FixedBoolArray* current_chunk;

        DistEffBoolArr(String* id_var, Distributable* kvStore_var, size_t node, bool get) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = get ? kvStore->get_size_t(node, id->clone()->concat("-chunkSize")) : 50;
            capacity = get ? kvStore->get_size_t(node, id->clone()->concat("-capacity")) : 1;
            currentChunkIdx = get ? kvStore->get_size_t(node, id->clone()->concat("-currentChunk")) : 0;
            numberOfElements = get ? kvStore->get_size_t(node, id->clone()->concat("-numElements")) : 0;
            current_chunk = get ? nullptr : new FixedBoolArray(chunkSize);
        }

        /**
         * @brief Construct a new Eff Col Arr object
         *
         */
        DistEffBoolArr(String* id_var, Distributable* kvStore_var, size_t node) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = kvStore->get_size_t(metadata_node, id->clone()->concat("-chunkSize"));
            capacity = kvStore->get_size_t(metadata_node, id->clone()->concat("-capacity"));
            currentChunkIdx = kvStore->get_size_t(metadata_node, id->clone()->concat("-currentChunk"));
            numberOfElements = kvStore->get_size_t(metadata_node, id->clone()->concat("-numElements"));
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         *
         * @param from
         */
        DistEffBoolArr(EffBoolArr& from, String* id_var, Distributable* kvStore_var, size_t node) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            kvStore->put(metadata_node, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(metadata_node, id->clone()->concat("-capacity"), capacity);
            kvStore->put(metadata_node, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(metadata_node, id->clone()->concat("-numElements"), numberOfElements);
            for (size_t i = 0; i < capacity; i += 1) {
                kvStore->put(i % 5, id->clone()->concat("-")->concat(i), from.chunks[i]->clone());
            }
        }

        /**
         * @brief Get the column at the given index
         *
         * @param idx
         * @return Column*
         */
        bool get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            FixedBoolArray* curChunk = kvStore->get_bool_chunk(chunkIdx % 5, id->clone()->concat("-")->concat(chunkIdx));
            return curChunk->get(idx % chunkSize);
        }

        FixedBoolArray* get_chunk(size_t chunkIdx) {
            return kvStore->get_bool_chunk(chunkIdx % 5, id->clone()->concat("-")->concat(chunkIdx));
        }

        /**
         * @brief get the size of this column
         *
         * @return size_t
         */
        size_t size() {
            return numberOfElements;
        }

        void push_back(bool val) {
            assert(current_chunk != nullptr);
            current_chunk->pushBack(val);
            numberOfElements += 1;
            if (current_chunk->capacity == current_chunk->used) {
                kvStore->put(currentChunkIdx % 5, id->clone()->concat("-")->concat(currentChunkIdx), current_chunk);
                currentChunkIdx += 1;
                current_chunk = new FixedBoolArray(chunkSize);
            }
        }

        void lock() {
            kvStore->put(metadata_node, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(metadata_node, id->clone()->concat("-capacity"), capacity);
            kvStore->put(metadata_node, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(metadata_node, id->clone()->concat("-numElements"), numberOfElements);
            if (current_chunk->used > 0) {
                kvStore->put(currentChunkIdx % 5, id->clone()->concat("-")->concat(currentChunkIdx), current_chunk);
            } else {
                delete current_chunk;
            }
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
        size_t metadata_node;
        FixedCharArray* current_chunk;

        DistEffCharArr(String* id_var, Distributable* kvStore_var, size_t node, bool get) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = get ? kvStore->get_size_t(node, id->clone()->concat("-chunkSize")) : 50;
            capacity = get ? kvStore->get_size_t(node, id->clone()->concat("-capacity")) : 1;
            currentChunkIdx = get ? kvStore->get_size_t(node, id->clone()->concat("-currentChunk")) : 0;
            numberOfElements = get ? kvStore->get_size_t(node, id->clone()->concat("-numElements")) : 0;
            current_chunk = get ? nullptr : new FixedCharArray(chunkSize);
        }

        /**
         * @brief Construct a new Eff Col Arr object
         *
         */
        DistEffCharArr(String* id_var, Distributable* kvStore_var, size_t node) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = kvStore->get_size_t(metadata_node, id->clone()->concat("-chunkSize"));
            capacity = kvStore->get_size_t(metadata_node, id->clone()->concat("-capacity"));
            currentChunkIdx = kvStore->get_size_t(metadata_node, id->clone()->concat("-currentChunk"));
            numberOfElements = kvStore->get_size_t(metadata_node, id->clone()->concat("-numElements"));
            current_chunk = new FixedCharArray(chunkSize);
        }

        /**
         * @brief Construct a new Eff Col Arr object from another eff col arr objects
         *
         * @param from
         */
        DistEffCharArr(EffCharArr& from, String* id_var, Distributable* kvStore_var, size_t node, bool get) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            for (size_t i = 0; i < currentChunkIdx; i += 1) {
                kvStore->put(i % 5, id->clone()->concat("-")->concat(i), from.chunks[i]->clone());
            }
            current_chunk = get ? nullptr : new FixedCharArray(*from.chunks[currentChunkIdx]);
        }

        /**
         * @brief Get the column at the given index
         *
         * @param idx
         * @return Column*
         */
        char get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            FixedCharArray* curChunk = kvStore->get_char_chunk(chunkIdx % 5, id->clone()->concat("-")->concat(chunkIdx));
            return curChunk->get(idx % chunkSize);
        }

        FixedStrArray* get_chunk(size_t chunkIdx) {
            return kvStore->get_str_chunk(chunkIdx % 5, id->clone()->concat("-")->concat(chunkIdx));
        }

        /**
         * @brief get the size of this column
         *
         * @return size_t
         */
        size_t size() {
            return numberOfElements;
        }

        void push_back(char val) {
            assert(current_chunk != nullptr);
            current_chunk->pushBack(val);
            numberOfElements += 1;
            if (current_chunk->capacity == current_chunk->used) {
                kvStore->put(currentChunkIdx % 5, id->clone()->concat("-")->concat(currentChunkIdx), current_chunk);
                currentChunkIdx += 1;
                current_chunk = new FixedCharArray(chunkSize);
            }
        }

        void lock() {
            kvStore->put(metadata_node, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(metadata_node, id->clone()->concat("-capacity"), capacity);
            kvStore->put(metadata_node, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(metadata_node, id->clone()->concat("-numElements"), numberOfElements);
            if (current_chunk->used > 0) {
                kvStore->put(currentChunkIdx % 5, id->clone()->concat("-")->concat(currentChunkIdx), current_chunk);
            } else {
                delete current_chunk;
            }
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
        size_t metadata_node;
        FixedStrArray* current_chunk;

        DistEffStrArr(String* id_var, Distributable* kvStore_var, size_t node, bool get) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = get ? kvStore->get_size_t(node, id->clone()->concat("-chunkSize")) : 50;
            capacity = get ? kvStore->get_size_t(node, id->clone()->concat("-capacity")) : 1;
            currentChunkIdx = get ? kvStore->get_size_t(node, id->clone()->concat("-currentChunk")) : 0;
            numberOfElements = get ? kvStore->get_size_t(node, id->clone()->concat("-numElements")) : 0;
            current_chunk = get ? nullptr : new FixedStrArray(chunkSize);
        }

        DistEffStrArr(String* id_var, Distributable* kvStore_var, size_t node) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = kvStore->get_size_t(metadata_node, id->clone()->concat("-chunkSize"));
            capacity = kvStore->get_size_t(metadata_node, id->clone()->concat("-capacity"));
            currentChunkIdx = kvStore->get_size_t(metadata_node, id->clone()->concat("-currentChunk"));
            numberOfElements = kvStore->get_size_t(metadata_node, id->clone()->concat("-numElements"));
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

        DistEffStrArr(EffStrArr& from, String* id_var, Distributable* kvStore_var, size_t node) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            kvStore->put(metadata_node, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(metadata_node, id->clone()->concat("-capacity"), capacity);
            kvStore->put(metadata_node, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(metadata_node, id->clone()->concat("-numElements"), numberOfElements);
            for (size_t i = 0; i < capacity; i += 1) {
                kvStore->put(i % 5, id->clone()->concat("-")->concat(i), from.chunks[i]->clone());
            }
        }

        String* get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            FixedStrArray* curChunk = kvStore->get_str_chunk(chunkIdx % 5, id->clone()->concat("-")->concat(chunkIdx));
            return curChunk->get(idx % chunkSize);
        }

        FixedStrArray* get_chunk(size_t chunkIdx) {
            return kvStore->get_str_chunk(chunkIdx % 5, id->clone()->concat("-")->concat(chunkIdx));
        }

        size_t size() {
            return numberOfElements;
        }

        void push_back(String* val) {
            assert(current_chunk != nullptr);
            current_chunk->pushBack(val);
            numberOfElements += 1;
            if (current_chunk->size() == current_chunk->numElements()) {
                kvStore->put(currentChunkIdx % 5, id->clone()->concat("-")->concat(currentChunkIdx), current_chunk);
                currentChunkIdx += 1;
                current_chunk = new FixedStrArray(chunkSize);
            }
        }

        void lock() {
            kvStore->put(metadata_node, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(metadata_node, id->clone()->concat("-capacity"), capacity);
            kvStore->put(metadata_node, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(metadata_node, id->clone()->concat("-numElements"), numberOfElements);
            if (current_chunk->numElements() > 0) {
                kvStore->put(currentChunkIdx % 5, id->clone()->concat("-")->concat(currentChunkIdx), current_chunk);
            } else {
                delete current_chunk;
            }
        }

        ~DistEffStrArr() {
            delete id;
        }
};