//
// Created by Rahul Acharya on 2/25/20.
//
#pragma once
#include "../util/object.h"
#include "../array/efficientArray.h"
#include "message.h"
#include <stdlib.h>
#include <sstream>
#include <netinet/in.h>

/**
 * Serializes different values of various data types. Makes the networking layer more efficient.
 */
class Serializer: public Object {

    public:

        /**
         * Creates a serializer object
         */
        Serializer() = default;


        /**
         * Destroys this serializer object
         */
        ~Serializer() override = default;

        static void putInBuffer(char* buffer, size_t& curIndex, unsigned char* bytes, size_t size) {
            for (size_t i = 0; i < size; i += 1, curIndex += 1) {
                buffer[curIndex] = bytes[i];
            }
        }

        static void serializeInBuffer(char* buffer, size_t& curIndex, int num) {
            auto *numChar = reinterpret_cast<unsigned char*>(&num);
            putInBuffer(buffer, curIndex, numChar, sizeof(num));
        }

        static void serializeInBuffer(char* buffer, size_t& curIndex, size_t num) {
            auto *numChar = reinterpret_cast<unsigned char*>(&num);
            putInBuffer(buffer, curIndex, numChar, sizeof(num));
        }

        static void serializeInBuffer(char* buffer, size_t& curIndex, float num) {
            auto *numChar = reinterpret_cast<unsigned char*>(&num);
            putInBuffer(buffer, curIndex, numChar, sizeof(num));
        }

        static void serializeInBuffer(char* buffer, size_t& curIndex, bool b) {
            auto *numChar = reinterpret_cast<unsigned char*>(&b);
            putInBuffer(buffer, curIndex, numChar, sizeof(b));
        }

        static void serializeInBuffer(char* buffer, size_t& curIndex, String* str) {
            serializeInBuffer(buffer, curIndex, str->c_str());
        }

        static void serializeInBuffer(char* buffer, size_t& curIndex, char* str) {
            for (size_t i = 0; i < strlen(str); i += 1, curIndex += 1) {
                buffer[curIndex] = str[i];
            }
            buffer[curIndex] = '\0';
            curIndex += 1;
        }

        static char* serialize(size_t num, size_t size) {
            char* buffer = new char[sizeof(num)];
            serializeInBuffer(buffer, size, num);
            return buffer;
        }

        /**
         * Serialize this fixed int array
         * @param arr
         * @return
         */
        static char* serialize(FixedIntArray* arr, size_t& endIndex) {
            size_t bufferSize = 2 * sizeof(size_t);
            bufferSize += (arr->used) * sizeof(int);
            size_t curIndex = 0;
            char* buffer = new char[bufferSize];
            serializeInBuffer(buffer, curIndex, arr->capacity);
            serializeInBuffer(buffer, curIndex, arr->used);
            for (size_t wordIndex = 0; wordIndex < arr->used; wordIndex += 1) {
                serializeInBuffer(buffer, curIndex, arr->get(wordIndex));
            }
            endIndex += curIndex;
            return buffer;
        }

        /**
         * Serialize this fixed float array
         * @param arr
         * @return
         */
        static char* serialize(FixedFloatArray* arr, size_t& endIndex) {
            size_t bufferSize = 2 * sizeof(size_t);
            bufferSize += (arr->used) * sizeof(float);
            size_t curIndex = 0;
            char* buffer = new char[bufferSize];
            serializeInBuffer(buffer, curIndex, arr->capacity);
            serializeInBuffer(buffer, curIndex, arr->used);
            for (size_t wordIndex = 0; wordIndex < arr->used; wordIndex += 1) {
                serializeInBuffer(buffer, curIndex, arr->get(wordIndex));
            }
            endIndex += curIndex;
            return buffer;
        }

        /**
         * Serialize this fixed boolean array
         * @param arr
         * @return
         */
        static char* serialize(FixedBoolArray* arr, size_t& endIndex) {
            size_t bufferSize = 2 * sizeof(size_t);
            bufferSize += (arr->used) * sizeof(bool);
            size_t curIndex = 0;
            char* buffer = new char[bufferSize];
            serializeInBuffer(buffer, curIndex, arr->capacity);
            serializeInBuffer(buffer, curIndex, arr->used);
            for (size_t wordIndex = 0; wordIndex < arr->used; wordIndex += 1) {
                serializeInBuffer(buffer, curIndex, arr->get(wordIndex));
            }
            endIndex += curIndex;
            return buffer;
        }

        /**
         * Serialize this fixed String* array
         * @param arr
         * @return
         */
        static char* serialize(FixedStrArray* arr, size_t& endIndex) {
            size_t bufferSize = 2 * sizeof(size_t);
            for (size_t i = 0; i < arr->numElements(); i += 1) {
                bufferSize += (arr->size() + 1);
            }
            size_t curIndex = 0;
            char* buffer = new char[bufferSize];
            serializeInBuffer(buffer, curIndex, arr->size());
            serializeInBuffer(buffer, curIndex, arr->numElements());
            for (size_t wordIndex = 0; wordIndex < arr->numElements(); wordIndex += 1) {
                serializeInBuffer(buffer, curIndex, arr->get(wordIndex));
            }
            endIndex += curIndex;
            return buffer;
        }

        static char* serialize(EffIntArr* arr) {
            size_t bufferSize = sizeof(size_t);
            bufferSize += (arr->numberOfElements) * sizeof(int);
            size_t curIndex = 0;
            char* buffer = new char[bufferSize];
            serializeInBuffer(buffer, curIndex, arr->numberOfElements);
            for (size_t wordIndex = 0; wordIndex < arr->numberOfElements; wordIndex += 1) {
                serializeInBuffer(buffer, curIndex, arr->get(wordIndex));
            }
            return buffer;
        }

        static char* serialize(EffFloatArr* arr) {
            size_t bufferSize = sizeof(size_t);
            bufferSize += (arr->numberOfElements) * sizeof(float);
            size_t curIndex = 0;
            char* buffer = new char[bufferSize];
            serializeInBuffer(buffer, curIndex, arr->numberOfElements);
            for (size_t wordIndex = 0; wordIndex < arr->numberOfElements; wordIndex += 1) {
                serializeInBuffer(buffer, curIndex, arr->get(wordIndex));
            }
            return buffer;
        }

        static char* serialize(EffBoolArr* arr) {
            size_t bufferSize = sizeof(size_t);
            bufferSize += (arr->numberOfElements) * sizeof(bool);
            size_t curIndex = 0;
            char* buffer = new char[bufferSize];
            serializeInBuffer(buffer, curIndex, arr->numberOfElements);
            for (size_t wordIndex = 0; wordIndex < arr->numberOfElements; wordIndex += 1) {
                serializeInBuffer(buffer, curIndex, arr->get(wordIndex));
            }
            return buffer;
        }

        static char* serialize(EffStrArr* arr) {
            size_t bufferSize = sizeof(size_t);
            for (size_t i = 0; i < arr->numberOfElements; i += 1) {
                bufferSize += (arr->size() + 1);
            }
            size_t curIndex = 0;
            char* buffer = new char[bufferSize];
            serializeInBuffer(buffer, curIndex, arr->numberOfElements);
            for (size_t wordIndex = 0; wordIndex < arr->numberOfElements; wordIndex += 1) {
                serializeInBuffer(buffer, curIndex, arr->get(wordIndex));
            }
            return buffer;
        }

        static FixedIntArray* deserializeFixedIntArr(const char* buffer, size_t& curIndex) {
            size_t capacity = deserializeSizeT(buffer, curIndex);
            size_t used = deserializeSizeT(buffer, curIndex);
            auto* arr = new FixedIntArray(capacity);
            for (size_t i = 0; i < used; i += 1) {
                int curVal = deserializeInt(buffer, curIndex);
                arr->pushBack(curVal);
            }
            return arr;
        }

        static FixedFloatArray* deserializeFixedFloatArr(const char* buffer, size_t& curIndex) {
            size_t capacity = deserializeSizeT(buffer, curIndex);
            size_t used = deserializeSizeT(buffer, curIndex);
            auto* arr = new FixedFloatArray(capacity);
            for (size_t i = 0; i < used; i += 1) {
                float curVal = deserializeFloat(buffer, curIndex);
                arr->pushBack(curVal);
            }
            return arr;
        }

        static FixedBoolArray* deserializeFixedBoolArr(const char* buffer, size_t& curIndex) {
            size_t capacity = deserializeSizeT(buffer, curIndex);
            size_t used = deserializeSizeT(buffer, curIndex);
            auto* arr = new FixedBoolArray(capacity);
            for (size_t i = 0; i < used; i += 1) {
                bool curVal = deserializeBool(buffer, curIndex);
                arr->pushBack(curVal);
            }
            return arr;
        }

        static FixedStrArray* deserializeFixedStrArr(const char* buffer, size_t& curIndex) {
            size_t capacity = deserializeSizeT(buffer, curIndex);
            size_t used = deserializeSizeT(buffer, curIndex);
            auto* arr = new FixedStrArray(capacity);
            for (size_t i = 0; i < used; i += 1) {
                String* curVal = deserializeString(buffer, curIndex);
                arr->pushBack(curVal);
            }
            return arr;
        }

        static EffStrArr* deserializeEffStrArr(char* buffer) {
            size_t curIndex = 0;
            size_t numberOfElements = deserializeSizeT(buffer, curIndex);
            auto* arr = new EffStrArr();
            for (size_t wordIndex = 0; wordIndex < numberOfElements; wordIndex += 1) {
                arr->pushBack(deserializeString(buffer, curIndex));
            }
            return arr;
        }

        static EffFloatArr* deserializeEffFloatArr(char* buffer) {
            size_t curIndex = 0;
            size_t numberOfElements = deserializeSizeT(buffer, curIndex);
            auto* arr = new EffFloatArr();
            for (size_t wordIndex = 0; wordIndex < numberOfElements; wordIndex += 1) {
                arr->pushBack(deserializeFloat(buffer, curIndex));
            }
            return arr;
        }

        static char* serialize(Message* m, size_t& size) {
            char* buf = nullptr;
            if (m->kind_ == MsgKind::Directory) {
                buf = serializeDirectory(dynamic_cast<Directory*>(m), size);
            } else if (m->kind_ == MsgKind::Register) {
                buf = serializeRegister(dynamic_cast<Register*>(m), size);
            } else if (m->kind_ == MsgKind::Send) {
                buf = serializeSend(dynamic_cast<Send*>(m), size);
            } else if (m->kind_ == MsgKind::Get) {
                buf = serializeGet(dynamic_cast<Get*>(m), size);
            }
            return buf;
        }

        static char* serializeRegister(Register* m, size_t& size) {
            char msgAbbr = 'R';
            size_t msgAttributesSize = 0;
            char* msgAttributes = serializeMsgAttributes(m, msgAttributesSize);
            char* buffer = new char[1 + msgAttributesSize + sizeof(size_t) + m->client->size() + 1];
            size_t curIndex = 0;
            buffer[curIndex] = msgAbbr;
            curIndex += 1;
            for (size_t i = 0; i < msgAttributesSize; i += 1, curIndex += 1) {
                buffer[curIndex] = msgAttributes[i];
            }
            delete[] msgAttributes;
            serializeInBuffer(buffer, curIndex, m->port);
            serializeInBuffer(buffer, curIndex, m->client);
            size += curIndex;
            return buffer;
        }

        static char* serializeDirectory(Directory* m, size_t& size) {
            char msgAbbr = 'D';
            size_t msgAttributesSize = 0;
            char* msgAttributes = serializeMsgAttributes(m, msgAttributesSize);
            size_t addressesSize = 0;
            for (size_t i = 0; i < m->clients; i += 1) {
                addressesSize += m->addresses[i]->size() + 1;
            }
            char* buffer = new char[1 + msgAttributesSize + sizeof(size_t) +
                                    (m->clients * sizeof(size_t)) + addressesSize];
            size_t curIndex = 0;
            buffer[curIndex] = msgAbbr;
            curIndex += 1;
            for (size_t i = 0; i < msgAttributesSize; i += 1, curIndex += 1) {
                buffer[curIndex] = msgAttributes[i];
            }
            delete[] msgAttributes;
            serializeInBuffer(buffer, curIndex, m->clients);
            for (size_t i = 0; i < m->clients; i += 1) {
                serializeInBuffer(buffer, curIndex, m->ports[i]);
            }
            for (size_t i = 0; i < m->clients; i += 1) {
                serializeInBuffer(buffer, curIndex, m->addresses[i]);
            }
            size += curIndex;
            return buffer;
        }

        static char* serializeGet(Get* m, size_t& size) {
            char msgAbbr = 'G';
            size_t msgAttributesSize = 0;
            char* msgAttributes = serializeMsgAttributes(m, msgAttributesSize);
            char type = m->type;
            char* buffer = new char[1 + msgAttributesSize + 1 + m->key->size() + 1];
            size_t curIndex = 0;
            buffer[curIndex] = msgAbbr;
            curIndex += 1;
            for (size_t i = 0; i < msgAttributesSize; i += 1, curIndex += 1) {
                buffer[curIndex] = msgAttributes[i];
            }
            delete[] msgAttributes;
            buffer[curIndex] = type;
            curIndex += 1;
            serializeInBuffer(buffer, curIndex, m->key);
            size += curIndex;
            return buffer;
        }

        static char* serializeSend(Send* s, size_t& size) {
            char msgAbbr = 'S';
            size_t msgAttributesSize = 0;
            char* msgAttributes = serializeMsgAttributes(s, msgAttributesSize);
            char type = s->transfer->type;
            char* serializedChunk;
            size_t serializedChunkSize = 0;
            if (type == 'T') {
                serializedChunk = serialize(s->transfer->data->st, serializedChunkSize);
            } else if (type == 'I') {
                serializedChunk = serialize(s->transfer->data->fi, serializedChunkSize);
            } else if(type == 'F') {
                serializedChunk = serialize(s->transfer->data->ff, serializedChunkSize);
            } else if(type == 'B') {
                serializedChunk = serialize(s->transfer->data->fb, serializedChunkSize);
            } else {
                serializedChunk = serialize(s->transfer->data->fs, serializedChunkSize);
            }
            char* buffer = new char[1 + msgAttributesSize + 1 + s->key->size() + 1 + serializedChunkSize];
            size_t curIndex = 0;
            buffer[curIndex] = msgAbbr;
            curIndex += 1;
            for (size_t i = 0; i < msgAttributesSize; i += 1, curIndex += 1) {
                buffer[curIndex] = msgAttributes[i];
            }
            delete[] msgAttributes;
            buffer[curIndex] = type;
            curIndex += 1;
            serializeInBuffer(buffer, curIndex, s->key);
            for (size_t i = 0; i < serializedChunkSize; i += 1, curIndex += 1) {
                buffer[curIndex] = serializedChunk[i];
            }
            delete[] serializedChunk;
            size += curIndex;
            return buffer;
        }

        static char* serializeMsgAttributes(Message* msg, size_t& endIndex) {
            size_t bufSize = sizeof(size_t) * 3;
            char* buffer = new char[bufSize];
            size_t curIndex = 0;
            serializeInBuffer(buffer, curIndex, msg->sender_);
            serializeInBuffer(buffer, curIndex, msg->target_);
            serializeInBuffer(buffer, curIndex, msg->id_);
            endIndex += curIndex;
            return buffer;
        }

        /**
         * Deserialize a message - we still have to implement some functionality for the other types, but this works for
         * ACK, for example. We ar just demonstrating use cases right now.
         * @param buffer
         * @return
         */
        static Message* deserializeMessage(char* buffer) {
            if (buffer[0] == 'D') {
                return deserializeDirectory(buffer);
            } else if (buffer[0] == 'R') {
                return deserializeRegister(buffer);
            } else if (buffer[0] == 'S') {
                return deserializeSend(buffer);
            } else if (buffer[0] == 'G') {
                return deserializeGet(buffer);
            }
            return nullptr;
        }

        static Register* deserializeRegister(char* buffer) {
            size_t curIndex = 1;
            size_t sender = deserializeSizeT(buffer, curIndex);
            size_t target = deserializeSizeT(buffer, curIndex);
            size_t id = deserializeSizeT(buffer, curIndex);
            size_t port = deserializeSizeT(buffer, curIndex);
            char* client = deserializeChar(buffer, curIndex);
            auto* r = new Register(sender, port, client);
            delete[] client;
            r->target_ = target;
            r->id_ = id;
            return r;
        }

        static Get* deserializeGet(char* buffer) {
            size_t curIndex = 1;
            size_t sender = deserializeSizeT(buffer, curIndex);
            size_t target = deserializeSizeT(buffer, curIndex);
            size_t id = deserializeSizeT(buffer, curIndex);
            char type = buffer[curIndex];
            curIndex += 1;
            char* key = deserializeChar(buffer, curIndex);
            Get* g = new Get(type, key);
            delete[] key;
            g->sender_ = sender;
            g->target_ = target;
            g->id_ = id;
            return g;
        }

        static Send* deserializeSend(char* buffer) {
            size_t curIndex = 1;
            size_t sender = deserializeSizeT(buffer, curIndex);
            size_t target = deserializeSizeT(buffer, curIndex);
            size_t id = deserializeSizeT(buffer, curIndex);
            char type = buffer[curIndex];
            curIndex += 1;
            char* key = deserializeChar(buffer, curIndex);
            Send* send;
            if (type == 'I') {
                FixedIntArray* arr = deserializeFixedIntArr(buffer, curIndex);
                send = new Send(arr, key);
            } else if (type == 'F') {
                FixedFloatArray* arr = deserializeFixedFloatArr(buffer, curIndex);
                send = new Send(arr, key);
            } else if (type == 'B') {
                FixedBoolArray* arr = deserializeFixedBoolArr(buffer, curIndex);
                send = new Send(arr, key);
            } else {
                FixedStrArray* arr = deserializeFixedStrArr(buffer, curIndex);
                send = new Send(arr, key);
            }
            delete[] key;
            send->sender_ = sender;
            send->target_ = target;
            send->id_ = id;
            return send;
        }

        static Directory* deserializeDirectory(char* buffer) {
            size_t curIndex = 1;
            size_t sender = deserializeSizeT(buffer, curIndex);
            size_t target = deserializeSizeT(buffer, curIndex);
            size_t id = deserializeSizeT(buffer, curIndex);
            size_t clients = deserializeSizeT(buffer, curIndex);
            auto* ports = new size_t[clients];
            for (size_t i = 0; i < clients; i += 1) {
                ports[i] = deserializeSizeT(buffer, curIndex);
            }
            auto** addresses = new String*[clients];
            for (size_t i = 0; i < clients; i += 1) {
                addresses[i] = deserializeString(buffer, curIndex);
            }
            auto* dir = new Directory(clients, ports, addresses);
            dir->sender_ = sender;
            dir->target_ = target;
            dir->id_ = id;
            return dir;
        }

        static int deserializeInt(const char* buffer, size_t& curIndex) {
            int num;
            auto *tmp = reinterpret_cast<unsigned char*>(&num);
            for (size_t i = 0; i < sizeof(num); i += 1, curIndex += 1) {
                tmp[i] = buffer[curIndex];
            }
            return num;
        }

        static size_t deserializeSizeT(const char* buffer, size_t& curIndex) {
            size_t num;
            auto *tmp = reinterpret_cast<unsigned char*>(&num);
            for (size_t i = 0; i < sizeof(num); i += 1, curIndex += 1) {
                tmp[i] = buffer[curIndex];
            }
            return num;
        }

        static float deserializeFloat(const char* buffer, size_t& curIndex) {
            float num;
            auto *tmp = reinterpret_cast<unsigned char*>(&num);
            for (size_t i = 0; i < sizeof(num); i += 1, curIndex += 1) {
                tmp[i] = buffer[curIndex];
            }
            return num;
        }

        static bool deserializeBool(const char* buffer, size_t& curIndex) {
            bool num;
            auto *tmp = reinterpret_cast<unsigned char*>(&num);
            for (size_t i = 0; i < sizeof(num); i += 1, curIndex += 1) {
                tmp[i] = buffer[curIndex];
            }
            return num;
        }

        static String* deserializeString(const char* buffer, size_t& curIndex) {
            char* c_str = deserializeChar(buffer, curIndex);
            String* str = new String(c_str);
            delete[] c_str;
            return str;
        }

        static char* deserializeChar(const char* buffer, size_t& curIndex) {
            size_t strSize = 0;
            for (size_t i = curIndex; buffer[i] != '\0'; i += 1) {
                strSize += 1;
            }
            char* cpyChar = new char[strSize + 1];
            for (size_t i = 0; i <= strSize; i += 1, curIndex += 1) {
                cpyChar[i] = buffer[curIndex];
            }
            return cpyChar;
        }
};
