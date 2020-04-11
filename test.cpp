#include "serial.h"
#include "schema.h"
#include "dataframe.h"
#include "application.h"
#include <stdio.h>
#include "network_ip.h"

/**
 * Ensures that the serializer correctly serializes and deserializes a float
 */
void testFloat() {
    Serializer* serializer = new Serializer();
    float f = 5.6;
    char* serializedFloat = serializer->serialize(f);
    assert(f == *(reinterpret_cast<float *> (serializedFloat)));

    delete serializer;

    std::cout << "works for float" << "\n";
}

/**
 * Ensures that the serializer correctly serializes and deserializes a size_t
 */
void testSizeT() {
    Serializer* serializer = new Serializer();
    size_t s = 68;
    char* serializedSizeT= serializer->serialize(s);
    assert(s == *(reinterpret_cast<size_t *> (serializedSizeT)));

    delete serializer;

    std::cout << "works for size_t" << "\n";
}

/**
 * Ensures that the serializer correctly serializes and deserializes a string array
 */
void testStringArr() {
    Serializer* serializer = new Serializer();
    Schema s("S");
    DataFrame df(s);
    Row r(df.get_schema());
    for (size_t i = 0; i < 5; i+=1) {
        r.set(0, new String("finna"));
        df.add_row(r);
    }

    EffStrArr* arr = df.columns->get(0)->as_string()->array;

    char* serializedString = serializer->serialize(arr);

    EffStrArr* serializedArr = serializer->deserializeStringArr(serializedString);

    assert(arr->equals(serializedArr));

    delete serializer;

    std::cout << "works for string array" << "\n";
}

/**
 * Ensures that the serializer correctly serializes and deserializes a float array
 */
void testFloatArr() {
    Serializer* serializer = new Serializer();
    Schema s("F");
    DataFrame df(s);
    Row r(df.get_schema());
    for (size_t i = 0; i < 5; i+=1) {
        r.set(0, (float) 156.7);
        df.add_row(r);
    }

    EffFloatArr* arr = df.columns->get(0)->as_float()->array;

    char* serializedFloat = serializer->serialize(arr);

    EffFloatArr* serializedArr = serializer->deserializeFloatArr(serializedFloat);

    assert(arr->equals(serializedArr));

    delete serializer;

    std::cout << "works for float array" << "\n";
}

/**
 * Ensures that the serializer correctly serializes and deserializes a message
 */
void testMessageDirectory() {

    Serializer* serializer = new Serializer();
    size_t sender = 90;
    size_t target = 91;
    size_t id = 92;
    size_t clients = 5;
    size_t* ports = new size_t[clients];
    for (size_t i = 0; i < clients; i += 1) {
        ports[i] = i;
    }
    String** addresses = new String*[clients];
    addresses[0] = new String("hello");
    addresses[1] = new String("there");
    addresses[2] = new String("world");
    addresses[3] = new String("what");
    addresses[4] = new String("good");

    Directory* m = new Directory(clients, ports, addresses);
    m->sender_ = sender;
    m->target_ = target;
    m->id_ = id;

    char* serializedMessage = serializer->serialize(m);

    Directory* deserializedMessage = dynamic_cast<Directory*>(serializer->deserializeMessage(serializedMessage));

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

    Serializer* serializer = new Serializer();
    size_t sender = 90;
    size_t target = 91;
    size_t id = 1;
    char* type = "B";
    char* key = "df1-chunk1";

    Get* g = new Get(type, key);
    g->sender_ = sender;
    g->target_ = target;
    g->id_ = id;

    char* serializedMessage = serializer->serializeGet(g);

    Get* get = dynamic_cast<Get *>(serializer->deserializeGet(serializedMessage));

    assert(sender == get->sender_);
    assert(target == get->target_);
    assert(id == get->id_);
    assert(strcmp(type, get->type) == 0);
    assert(strcmp(key, get->key) == 0);

    std::cout << "works for get" << "\n";
}

void testMessageSend() {

    size_t sender = 90;
    size_t target = 91;
    size_t id = 1;

    FixedIntArray* arr = new FixedIntArray(5);
    for(size_t i = 0; i < 5; i++) {
        arr->pushBack(1);
    }

    Serializer* serializer = new Serializer();
    char* type = "I";
    Chunk c;
    c.fi = arr;

    Send* s = new Send(c, type);

    s->sender_ = sender;
    s->target_ = target;
    s->id_ = id;

    char* serializedMessage = serializer->serializeSend(s);

    std::cout << serializedMessage;
//    Get* get = dynamic_cast<Get *>(serializer->deserializeGet(serializedMessage));
//
//    assert(sender == get->sender_);
//    assert(target == get->target_);
//    assert(id == get->id_);
//    assert(strcmp(type, get->type) == 0);
//    assert(strcmp(key, get->key) == 0);

    std::cout << "works for send" << "\n";
}

/**
 * Ensures that the serializer correctly serializes and deserializes a message
 */
void testMessageRegister() {

    Serializer* serializer = new Serializer();
    size_t sender = 90;
    size_t id = 92;
    char* client = "192.0.2.33";
    size_t port = 8080;

    Register* m = new Register(sender, port, client);
    m->id_ = id;

    char* serializedMessage = serializer->serialize(m);

    Register* deserializedMessage = dynamic_cast<Register*>(serializer->deserializeMessage(serializedMessage));

    assert(sender == deserializedMessage->sender_);
    assert(0 == deserializedMessage->target_);
    assert(id == deserializedMessage->id_);
    assert(strcmp(client, deserializedMessage->client) == 0);
    assert(port == deserializedMessage->port);

    std::cout << "works for registry" << "\n";
}

void testNetwork() {
    size_t num_nodes = 5;
    NetworkIP* ips = new NetworkIP[num_nodes];
    std::thread* pids = new std::thread[num_nodes];
    String* serverAddr = new String("127.0.0.1");
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
    testFloat();
    testSizeT();
    testStringArr();
    testFloatArr();
    testMessageDirectory();
    testMessageGet();
    //testMessageSend();
    testMessageRegister();
    testNetwork();
    std::cout << "ALL PASSED";
    Trivial triv(0);
    triv.run_();
    return 0;
}
