//
// Created by Rahul Acharya on 2/25/20.
//
#pragma once
#include "object.h"
#include "efficientArray.h"
#include "message.h"
#include "msgKind.h"
#include <stdlib.h>

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

        /**
         * deserialize the serialized fixed string array
         * @param buffer - the serialized form of the string array
         * @return - the string array in its original format
         */
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
            if(index != bufferSize - 1) {
                buffer[index] = sizeof(currFloat);
                index += 1;
            }

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

        char* prefix = "ACK";
        char* sender = serialize(m->sender_);
        char* target = serialize(m->target_);
        char* id = serialize(m->id_);

        size_t bufferSize = strlen(prefix) + strlen(sender) + strlen(target) + strlen(id) + 1;

        char* buffer = new char[bufferSize];

        size_t index = 0;

        buffer[index] = strlen(prefix);
        index += 1;

        for(size_t i = 0; i < strlen(prefix); i++) {
            buffer[index] = prefix[i];
            index += 1;
        }

        for(size_t i = 0; i < strlen(sender); i++) {
            buffer[index] = sender[i];
            index += 1;
        }

        for(size_t i = 0; i < strlen(target); i++) {
            buffer[index] = target[i];
            index += 1;
        }

        for(size_t i = 0; i < strlen(id); i++) {
            buffer[index] = id[i];
            index += 1;
        }

        return buffer;
    }

    /**
     * Deserialize a message - we still have to implement some functionality for the other types, but this works for
     * ACK, for example. We ar just demonstrating use cases right now.
     * @param buffer
     * @return
     */
    Message* deserializeMessage(char* buffer) {
        size_t msgKindSize = buffer[0];
        char* curr = new char[msgKindSize];

        for(size_t i = 0; i < msgKindSize; i++) {
            curr[i] = buffer[i];
        }
        MsgKind msgKind;
        size_t sender;
        size_t target;
        size_t id;

        if(strcmp(curr, "ACK") == 0) {
            msgKind = MsgKind ::Ack;
        } else if (strcmp(curr, "NACK") == 0) {
            msgKind = MsgKind ::Nack;
        } else if(strcmp(curr, "PUT") == 0){
            msgKind = MsgKind ::Put;
        } else if(strcmp(curr, "REPLY") == 0){
            msgKind = MsgKind ::Reply;
        } else if(strcmp(curr, "GET") == 0){
            msgKind = MsgKind ::Get;
        } else if(strcmp(curr, "WAITANDGET") == 0){
            msgKind = MsgKind ::WaitAndGet;
        } else if(strcmp(curr, "STATUS") == 0){
            msgKind = MsgKind ::Status;
        } else if(strcmp(curr, "KILL") == 0){
            msgKind = MsgKind ::Kill;
        } else if(strcmp(curr, "REGISTER") == 0){
            msgKind = MsgKind ::Register;
        } else if(strcmp(curr, "DIRECTORY") == 0){
            msgKind = MsgKind ::Directory;
        }

        size_t index = msgKindSize + 1;
        sender = buffer[index];
        target = buffer[index+1];
        id = buffer[index+2];

        return new Message(msgKind, sender, target, id);
    }
};
