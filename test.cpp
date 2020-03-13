#include "serial.h"
#include "schema.h"
#include "dataframe.h"
#include <stdio.h>

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
void testMessage() {

    Serializer* serializer = new Serializer();
    MsgKind a = MsgKind ::Ack;
    size_t sender = 90;
    size_t target = 91;
    size_t id = 92;

    Message* m = new Message(a, sender, target, id);

    char* serializedMessage = serializer->serialize(m);

    Message* deserializedMessage = serializer->deserializeMessage(serializedMessage);

    std::cout << deserializedMessage->sender_ << "\n";
    std::cout << deserializedMessage->target_ << "\n";
    std::cout << deserializedMessage->id_ << "\n";

    std::cout << "works for message" << "\n";
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
    testMessage();

    std::cout << "ALL PASSED";
}
