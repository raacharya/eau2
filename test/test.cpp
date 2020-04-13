#include "../network/serial.h"
#include "../dataframe/schema.h"
#include "../dataframe/dataframe.h"
#include "application.h"
#include <stdio.h>
#include "../network/network_ip.h"

/**
 * Ensures that the serializer correctly serializes and deserializes a string array
 */
void testStringArr() {
    Schema s("S");
    DataFrame df(s);
    Row r(df.get_schema());
    for (size_t i = 0; i < 5; i+=1) {
        r.set(0, new String("finna"));
        df.add_row(r);
    }

    EffStrArr* arr = df.columns->get(0)->as_string()->array;

    char* serializedString = Serializer::serialize(arr);

    EffStrArr* serializedArr = Serializer::deserializeEffStrArr(serializedString);

    assert(arr->equals(serializedArr));

    std::cout << "works for string array" << "\n";
}

/**
 * Ensures that the serializer correctly serializes and deserializes a float array
 */
void testFloatArr() {
    Schema s("F");
    DataFrame df(s);
    Row r(df.get_schema());
    for (size_t i = 0; i < 5; i+=1) {
        r.set(0, (float) 156.7);
        df.add_row(r);
    }

    EffFloatArr* arr = df.columns->get(0)->as_float()->array;

    char* serializedFloat = Serializer::serialize(arr);

    EffFloatArr* serializedArr = Serializer::deserializeEffFloatArr(serializedFloat);

    assert(arr->equals(serializedArr));

    std::cout << "works for float array" << "\n";
}

/**
 * Ensures that the serializer correctly serializes and deserializes a message
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

    std::cout << "works for directory" << "\n";
}

/**
 * Ensures that the serializer correctly serializes and deserializes a message
 */
void testMessageGet() {
    size_t sender = 90;
    size_t target = 91;
    size_t id = 1;
    char type = 'B';
    char* key = "df1-chunk1";

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
    assert(strcmp(key, get->key) == 0);

    std::cout << "works for get" << "\n";
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
    auto* c = new Chunk();
    c->fi = arr;

    Send* s = new Send(c, type);

    s->sender_ = sender;
    s->target_ = target;
    s->id_ = id;

    size_t size;
    char* serializedMessage = Serializer::serializeSend(s, size);

    Send* send = dynamic_cast<Send*>(Serializer::deserializeMessage(serializedMessage));

    assert(sender == send->sender_);
    assert(target == send->target_);
    assert(id == send->id_);
    assert(type == send->type);
    assert(0 == send->c->fi->get(0));
    assert(1 == send->c->fi->get(1));
    assert(2 == send->c->fi->get(2));
    assert(3 == send->c->fi->get(3));
    assert(4 == send->c->fi->get(4));

    std::cout << "works for send" << "\n";
}

/**
 * Ensures that the serializer correctly serializes and deserializes a message
 */
void testMessageRegister() {
    size_t sender = 90;
    size_t id = 92;
    char* client = "192.0.2.33";
    size_t port = 8080;

    auto* m = new Register(sender, port, client);
    m->id_ = id;

    size_t size;
    char* serializedMessage = Serializer::serialize(m, size);

    auto* deserializedMessage = dynamic_cast<Register*>(Serializer::deserializeMessage(serializedMessage));

    assert(sender == deserializedMessage->sender_);
    assert(0 == deserializedMessage->target_);
    assert(id == deserializedMessage->id_);
    assert(strcmp(client, deserializedMessage->client) == 0);
    assert(port == deserializedMessage->port);

    std::cout << "works for registry" << "\n";
}

void testNetwork() {
    size_t num_nodes = 5;
    auto* ips = new NetworkIP[num_nodes];
    auto* pids = new std::thread[num_nodes];
    auto* serverAddr = new String("127.0.0.1");
    pids[0] = std::thread(&NetworkIP::server_init, ips[0], 0, 9000);
    for (size_t i = 1; i < num_nodes; i += 1) {
        pids[i] = std::thread(&NetworkIP::client_init, ips[i], i, 9000 + i, serverAddr->c_str(), 9000);
    }
    for (size_t i = 0; i < num_nodes; i += 1) {
        pids[i].join();
    }
    delete serverAddr;
    delete[] ips;
    delete[] pids;

    std::cout << "works for network" << "\n";
}

/**
 * Runs our use cases
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char** argv) {
    testStringArr();
    testFloatArr();
    testMessageDirectory();
    testMessageGet();
    testMessageSend();
    testMessageRegister();
    testNetwork();
    std::cout << "ALL PASSED";
    Trivial triv(0);
    triv.run_();
    return 0;
}
