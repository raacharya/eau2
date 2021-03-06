#include "../network/serial.h"
#include "../dataframe/schema.h"
#include "../dataframe/dataframe.h"
#include "application.h"
#include <stdio.h>

/**
 * The first four tests test functionality of serializing
 * and deserializing different types of messages.
 */
void testMessageDirectory() {
    size_t sender = 90;
    size_t target = 91;
    size_t id = 92;
    size_t clients = 5;
    auto* ports = new size_t[clients];
    for (size_t i = 0; i < clients; i += 1) {
        ports[i] = i;
    }
    auto** addresses = new String*[clients];
    addresses[0] = new String("hello");
    addresses[1] = new String("there");
    addresses[2] = new String("world");
    addresses[3] = new String("what");
    addresses[4] = new String("good");
    auto* m = new Directory(clients, ports, addresses);
    m->sender_ = sender;
    m->target_ = target;
    m->id_ = id;
    size_t size;
    char* serializedMessage = Serializer::serialize(m, size);
    auto* deserializedMessage = dynamic_cast<Directory*>(Serializer::deserializeMessage(serializedMessage));
    assert(sender == deserializedMessage->sender_);
    assert(target == deserializedMessage->target_);
    assert(id == deserializedMessage->id_);
    assert(clients == deserializedMessage->clients);
    for (size_t i = 0; i < clients; i += 1) {
        assert(ports[i] == deserializedMessage->ports[i]);
    }
    assert(addresses[0]->equals(deserializedMessage->addresses[0]));
    assert(addresses[1]->equals(deserializedMessage->addresses[1]));
    assert(addresses[2]->equals(deserializedMessage->addresses[2]));
    assert(addresses[3]->equals(deserializedMessage->addresses[3]));
    assert(addresses[4]->equals(deserializedMessage->addresses[4]));
    delete m;
    delete[] serializedMessage;
    delete deserializedMessage;
}

void testMessageGet() {
    size_t sender = 90;
    size_t target = 91;
    size_t id = 1;
    char type = 'B';
    const char* key = "df1-chunk1";
    Get* g = new Get(type, key);
    g->sender_ = sender;
    g->target_ = target;
    g->id_ = id;
    size_t size;
    char* serializedMessage = Serializer::serializeGet(g, size);
    Get* get = dynamic_cast<Get *>(Serializer::deserializeGet(serializedMessage));
    assert(sender == get->sender_);
    assert(target == get->target_);
    assert(id == get->id_);
    assert(type == get->type);
    assert(strcmp(key, get->key->c_str()) == 0);
    delete[] serializedMessage;
    delete get;
    delete g;
}

void testMessageSend() {
    size_t sender = 90;
    size_t target = 91;
    size_t id = 1;
    auto* arr = new FixedIntArray(5);
    for(size_t i = 0; i < 5; i++) {
        arr->pushBack(i);
    }
    char type = 'I';
    Send* s = new Send(arr, "finna");
    s->sender_ = sender;
    s->target_ = target;
    s->id_ = id;
    size_t size;
    char* serializedMessage = Serializer::serializeSend(s, size);
    Send* send = dynamic_cast<Send*>(Serializer::deserializeMessage(serializedMessage));
    assert(sender == send->sender_);
    assert(target == send->target_);
    assert(id == send->id_);
    assert(type == send->type());
    assert(strcmp("finna", send->key->c_str()) == 0);
    assert(0 == send->transfer->int_chunk()->get(0));
    assert(1 == send->transfer->int_chunk()->get(1));
    assert(2 == send->transfer->int_chunk()->get(2));
    assert(3 == send->transfer->int_chunk()->get(3));
    assert(4 == send->transfer->int_chunk()->get(4));
    delete[] serializedMessage;
    delete send->transfer;
    delete send;
    delete s->transfer;
    delete s;
}

void testMessageRegister() {
    size_t sender = 90;
    size_t id = 92;
    const char* client = "192.0.2.33";
    size_t port = 8080;
    auto* m = new Register(sender, port, client);
    m->id_ = id;
    size_t size;
    char* serializedMessage = Serializer::serialize(m, size);
    auto* deserializedMessage = dynamic_cast<Register*>(Serializer::deserializeMessage(serializedMessage));
    assert(sender == deserializedMessage->sender_);
    assert(0 == deserializedMessage->target_);
    assert(id == deserializedMessage->id_);
    assert(strcmp(client, deserializedMessage->client->c_str()) == 0);
    assert(port == deserializedMessage->port);
    delete[] serializedMessage;
    delete deserializedMessage;
    delete m;
}

/**
 * The next test tests the Trivial application.
 */
void testTrivial() {
    auto** kds = new KDStore*[5];
    auto* pids = new std::thread[5];
    auto** wcs = new Trivial*[5];
    for (size_t i = 0; i < 5; i += 1) {
        kds[i] = new KDStore(i);
        wcs[i] = new Trivial(i, kds[i]);
    }
    for (size_t i = 0; i < 5; i += 1) {
        pids[i] = std::thread(&Trivial::run_, wcs[i]);
    }
    for (size_t i = 0; i < 5; i += 1) {
        pids[i].join();
    }
    for (size_t i = 0; i < 5; i += 1) {
        kds[i]->kvStore->network->shutdown();
        kds[i]->kvStore->network->shutdown_open_conns();
    }
    for (size_t i = 0; i < 5; i += 1) {
        delete wcs[i];
        delete kds[i];
    }
    delete[] wcs;
    delete[] kds;
    delete[] pids;
}

/**
 * The next test tests the WordCount application.
 */
void testWordCount() {
    auto** kds = new KDStore*[5];
    auto* pids = new std::thread[5];
    auto** wcs = new WordCount*[5];
    for (size_t i = 0; i < 5; i += 1) {
        kds[i] = new KDStore(i);
        wcs[i] = new WordCount(i, kds[i], "100k.txt");
    }
    for (size_t i = 0; i < 5; i += 1) {
        pids[i] = std::thread(&WordCount::run_, wcs[i]);
    }
    for (size_t i = 0; i < 5; i += 1) {
        pids[i].join();
    }
    for (size_t i = 0; i < 5; i += 1) {
        kds[i]->kvStore->network->shutdown();
        kds[i]->kvStore->network->shutdown_open_conns();
    }
    for (size_t i = 0; i < 5; i += 1) {
        delete wcs[i];
        delete kds[i];
    }
    delete[] wcs;
    delete[] kds;
    delete[] pids;
}

int main(int argc, char** argv) {
    testMessageDirectory();
    testMessageGet();
    testMessageSend();
    testMessageRegister();
    testTrivial();
    testWordCount();
    std::cout<<"Tests passed\n";
    return 0;
}
