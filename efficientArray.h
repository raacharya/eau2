#pragma once
#include "array.h"
#include "string.h"
#include <stdarg.h>


/** The efficient arr uses chunks to improve performance. We store fixed size arrays as these chunks and then have pointers to
 * these chunks. With this implementaiton, when we extend size, we no longer have to copy the storage one by one. Instead, we copy
 * pointers to these chunks and extend size that way. This leads to significant performance improvements. 
 */

class EffIntArr : public Object {
    public:
        FixedIntArray** chunks;
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;

        EffIntArr() : Object() {
            chunkSize = 50;
            capacity = 1;
            currentChunkIdx = 0;
            numberOfElements = 0;
            chunks = new FixedIntArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedIntArray(chunkSize);
            }
        }

        ~EffIntArr() {
            for (int i = 0; i < capacity; i += 1) {
                delete chunks[i];
            }
            delete[] chunks;
        }

        EffIntArr(int n, ...) {
            EffIntArr();
            va_list argp;
            va_start(argp, n);
            for(size_t i = 0; i < n; i++) {
                pushBack(va_arg(argp, int));
            }
            va_end(argp);
        }

        EffIntArr(EffIntArr& from) {
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            chunks = new FixedIntArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedIntArray(*from.chunks[i]);
            }
        }

        int get(size_t index) {
            size_t chunkIdx = index / chunkSize;
            return chunks[chunkIdx]->get(index % chunkSize);
        }

        void set(size_t index, int value) {
            assert(index < numberOfElements);
            size_t chunkIdx = index / chunkSize;
            chunks[chunkIdx]->set(index % chunkSize, value);
        }

        void pushBack(int value) {
            FixedIntArray* currentChunk = chunks[currentChunkIdx];
            if (currentChunk->capacity == currentChunk->used) {
                currentChunkIdx += 1;
                if (currentChunkIdx == capacity) {
                    extendSize();
                }
                currentChunk = chunks[currentChunkIdx];
            }
            currentChunk->pushBack(value);
            numberOfElements += 1;
        }

        int indexOf(int item) {
            for (int i = 0; i <= currentChunkIdx; i += 1) {
                int indexOf = chunks[i]->indexOf(item);
                if (indexOf != -1) {
                    return indexOf;
                }
            }
            return -1;
        }

        void extendSize() {
            FixedIntArray** newChunks = new FixedIntArray*[capacity * 2];
            int i = 0;
            for (i; i < capacity; i += 1) {
                newChunks[i] = chunks[i];
            }
            capacity *= 2;
            for (i; i < capacity; i += 1) {
                newChunks[i] = new FixedIntArray(chunkSize);
            }
            delete[] chunks;
            chunks = newChunks;
        }

        size_t size() {
            return numberOfElements;
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
            id = id_var;
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
            id = id_var;
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

        Value* createValue(FixedIntArray* fixedIntArray) {
            Value* val = new Value;
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
        }
};

/*************************************************************************
 * EffFloatArr::
 * Holds float values.
 */
class EffFloatArr : public Object {
    public:
        size_t chunkSize = 50;
        size_t capacity = 1;
        size_t currentChunkIdx = 0;
        size_t numberOfElements = 0;
        FixedFloatArray** chunks;
    
        EffFloatArr() {
            chunks = new FixedFloatArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedFloatArray(chunkSize);
            }
        }

        EffFloatArr(int n, ...) {
            EffFloatArr();
            va_list argp;
            va_start(argp, n);
            for(size_t i = 0; i < n; i++) {
                pushBack(va_arg(argp, int));
            }
            va_end(argp);
        }

        EffFloatArr(EffFloatArr& from) {
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            chunks = new FixedFloatArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedFloatArray(*from.chunks[i]);
            }
        }

        bool equals(Object* other) {
            EffFloatArr* o = dynamic_cast<EffFloatArr *> (other);
            if(o) {
                for(size_t i = 0; i < numberOfElements; i++) {
                    if(!get(i) == (o->get(i))) {
                        return false;
                    }
                }
            }
            return numberOfElements == o->numberOfElements;
        }

        void extendSize() {
            FixedFloatArray** newChunks = new FixedFloatArray*[capacity * 2];
            int i = 0;
            for (i; i < capacity; i += 1) {
                newChunks[i] = chunks[i];
            }
            capacity *= 2;
            for (i; i < capacity; i += 1) {
                newChunks[i] = new FixedFloatArray(chunkSize);
            }
            delete[] chunks;
            chunks = newChunks;
        }

        void pushBack(float value) {
            FixedFloatArray* currentChunk = chunks[currentChunkIdx];
            if (currentChunk->capacity == currentChunk->used) {
                currentChunkIdx += 1;
                if (currentChunkIdx == capacity) {
                    extendSize();
                }
                currentChunk = chunks[currentChunkIdx];
            }
            currentChunk->pushBack(value);
            numberOfElements += 1;
        }

        int indexOf(float item) {
            for (int i = 0; i <= currentChunkIdx; i += 1) {
                int indexOf = chunks[i]->indexOf(item);
                if (indexOf != -1) {
                    return indexOf;
                }
            }
            return -1;
        }

        float get(size_t idx) {

            size_t chunkIdx = idx / chunkSize;

            return chunks[chunkIdx]->get(idx % chunkSize);
        }

        
        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, float val) {
            size_t chunkIdx = idx / chunkSize;

            chunks[chunkIdx]->set(idx % chunkSize, val);
        }

        size_t size() {
            return numberOfElements;
        }

        ~EffFloatArr() {
          for (int i = 0; i < capacity; i += 1) {
            delete chunks[i];
          }
          delete[] chunks;
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
            id = id_var;
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
            id = id_var;
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
            Value* val = kvStore->getFromNode(createKey(buf));
            Object* obj = val->obj;
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
        }
};

/*************************************************************************
 * EffBoolArr::
 * Holds bool values.
 */
class EffBoolArr : public Object {
    public:
        size_t chunkSize = 50;
        size_t capacity = 1;
        size_t currentChunkIdx = 0;
        size_t numberOfElements = 0;
        FixedBoolArray** chunks;
    
        EffBoolArr() {
            chunks = new FixedBoolArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedBoolArray(chunkSize);
            }
        }

        EffBoolArr(int n, ...) {
            EffBoolArr();
            va_list argp;
            va_start(argp, n);
            for(size_t i = 0; i < n; i++) {
                pushBack(va_arg(argp, int));
            }
            va_end(argp);
        }

        EffBoolArr(EffBoolArr& from) {
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            chunks = new FixedBoolArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedBoolArray(*from.chunks[i]);
            }
        }

        void extendSize() {
            FixedBoolArray** newChunks = new FixedBoolArray*[capacity * 2];
            int i = 0;
            for (i; i < capacity; i += 1) {
                newChunks[i] = chunks[i];
            }
            capacity *= 2;
            for (i; i < capacity; i += 1) {
                newChunks[i] = new FixedBoolArray(chunkSize);
            }
            delete[] chunks;
            chunks = newChunks;
        }

        void pushBack(bool value) {
            FixedBoolArray* currentChunk = chunks[currentChunkIdx];
            if (currentChunk->capacity == currentChunk->used) {
                currentChunkIdx += 1;
                if (currentChunkIdx == capacity) {
                    extendSize();
                }
                currentChunk = chunks[currentChunkIdx];
            }
            currentChunk->pushBack(value);
            numberOfElements += 1;
        }

        bool get(size_t idx) {

            size_t chunkIdx = idx / chunkSize;

            return chunks[chunkIdx]->get(idx % chunkSize);
        }

        
        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, bool val) {
            size_t chunkIdx = idx / chunkSize;

            chunks[chunkIdx]->set(idx % chunkSize, val);
        }

        size_t size() {
            return numberOfElements;
        }

        int indexOf(bool item) {
            for (int i = 0; i <= currentChunkIdx; i += 1) {
                int indexOf = chunks[i]->indexOf(item);
                if (indexOf != -1) {
                    return indexOf;
                }
            }
            return -1;
        }

        ~EffBoolArr() {
          for (int i = 0; i < capacity; i += 1) {
            delete chunks[i];
          }
          delete[] chunks;
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
            id = id_var;
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
            id = id_var;
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
        }
};

/*************************************************************************
 * EffCharArr::
 * Holds char values.
 */
class EffCharArr : public Object {
    public:
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;
        FixedCharArray** chunks;
    
        EffCharArr() {
            chunkSize = 50;
            capacity = 1;
            currentChunkIdx = 0;
            numberOfElements = 0;
            chunks = new FixedCharArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedCharArray(chunkSize);
            }
        }

        EffCharArr(EffCharArr& from) {
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            chunks = new FixedCharArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedCharArray(*from.chunks[i]);
            }
        }

        EffCharArr(int n, ...) {
            EffCharArr();
            va_list argp;
            va_start(argp, n);
            for(size_t i = 0; i < n; i++) {
                pushBack(va_arg(argp, char));
            }
            va_end(argp);
        }

        void extendSize() {
            FixedCharArray** newChunks = new FixedCharArray*[capacity * 2];
            int i = 0;
            for (i; i < capacity; i += 1) {
                newChunks[i] = chunks[i];
            }
            capacity *= 2;
            for (i; i < capacity; i+=1) {
                newChunks[i] = new FixedCharArray(chunkSize);
            }
            delete[] chunks;
            chunks = newChunks;
        }

        void pushBack(char value) {
            FixedCharArray* currentChunk = chunks[currentChunkIdx];
            if (currentChunk->capacity == currentChunk->used) {
                currentChunkIdx += 1;
                if (currentChunkIdx == capacity) {
                    extendSize();
                }
                currentChunk = chunks[currentChunkIdx];
            }
            currentChunk->pushBack(value);
            numberOfElements += 1;
        }

        char get(size_t idx) {

            size_t chunkIdx = idx / chunkSize;

            return chunks[chunkIdx]->get(idx % chunkSize);
        }

        
        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, char val) {
            size_t chunkIdx = idx / chunkSize;

            chunks[chunkIdx]->set(idx % chunkSize, val);
        }

        size_t size() {
            return numberOfElements;
        }

        int indexOf(char item) {
            for (int i = 0; i <= currentChunkIdx; i += 1) {
                int indexOf = chunks[i]->indexOf(item);
                if (indexOf != -1) {
                    return indexOf;
                }
            }
            return -1;
        }

        ~EffCharArr() {
          for (int i = 0; i < capacity; i += 1) {
            delete chunks[i];
          }
          delete[] chunks;
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
            id = id_var;
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
            id = id_var;
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
        }
};

/*************************************************************************
 * EffStrArr::
 * Holds String values.
 */
class EffStrArr : public Object {
    public:
        size_t chunkSize = 50;
        size_t capacity = 1;
        size_t currentChunkIdx = 0;
        size_t numberOfElements = 0;
        FixedStrArray** chunks;
    
        EffStrArr() {
            chunks = new FixedStrArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedStrArray(chunkSize);
            }
        }

        bool equals(Object* other) {
            EffStrArr* o = dynamic_cast<EffStrArr *> (other);
            if(o) {
                for(size_t i = 0; i < numberOfElements; i++) {
                    if(!get(i)->equals(o->get(i))) {
                        return false;
                    }
                }
            }
            return numberOfElements == o->numberOfElements;
        }

        EffStrArr(EffStrArr& from) {
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            chunks = new FixedStrArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedStrArray(*from.chunks[i]);
            }
        }

        EffStrArr(int n, ...) {
            EffStrArr();
            va_list argp;
            va_start(argp, n);
            for(size_t i = 0; i < n; i++) {
                char* str = va_arg(argp, char*);
                size_t len = strlen(str);
                pushBack(new String(true, str, len));
            }
            va_end(argp);
        }

        void extendSize() {
            FixedStrArray** newChunks = new FixedStrArray*[capacity * 2];
            int i = 0;
            for (i; i < capacity; i += 1) {
                newChunks[i] = chunks[i];
            }
            capacity *= 2;
            for (i; i < capacity; i += 1) {
                newChunks[i] = new FixedStrArray(chunkSize);
            }
            delete[] chunks;
            chunks = newChunks;
        }

        void pushBack(String* value) {
            FixedStrArray* currentChunk = chunks[currentChunkIdx];
            if (currentChunk->size() == currentChunk->numElements()) {
                currentChunkIdx += 1;
                if (currentChunkIdx == capacity) {
                    extendSize();
                }
                currentChunk = chunks[currentChunkIdx];
            }
            currentChunk->pushBack(value);
            numberOfElements += 1;
        }

        String* get(size_t idx) {

            size_t chunkIdx = idx / chunkSize;

            return chunks[chunkIdx]->get(idx % chunkSize);
        }

        
        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, String* val) {
            size_t chunkIdx = idx / chunkSize;

            chunks[chunkIdx]->set(idx % chunkSize, val);
        }

        size_t size() {
            return numberOfElements;
        }

        int indexOf(String* item) {
            for (int i = 0; i <= currentChunkIdx; i += 1) {
                int indexOf = chunks[i]->indexOf(item);
                if (indexOf != -1) {
                    return indexOf;
                }
            }
            return -1;
        }

        ~EffStrArr() {
          for (int i = 0; i < capacity; i += 1) {
            delete chunks[i];
          }
          delete[] chunks;
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
            id = id_var;
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
            id = id_var;
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
            return curChunk->get(idx % chunkSize);
        }

        size_t size() {
            return numberOfElements;
        }

        ~DistEffStrArr() {}
};
        
/*************************************************************************
 * EffTypeArr::
 * Holds Type values.
 */
class EffTypeArr : public Object {
    public:
        size_t chunkSize = 50;
        size_t capacity = 1;
        size_t currentChunkIdx = 0;
        size_t numberOfElements = 0;
        FixedTypeArray** chunks;
    
        EffTypeArr() {
            chunks = new FixedTypeArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedTypeArray(chunkSize);
            }
        }

        EffTypeArr(int n, ...) {
            EffTypeArr();
            va_list argp;
            va_start(argp, n);
            for(size_t i = 0; i < n; i++) {
                Type* type = va_arg(argp, Type*);
                pushBack(type);
            }
            va_end(argp);
        }

        EffTypeArr(EffTypeArr& from) {
            chunkSize = from.chunkSize;
            capacity = from.capacity;
            currentChunkIdx = from.currentChunkIdx;
            numberOfElements = from.numberOfElements;
            chunks = new FixedTypeArray*[capacity];
            for (int i = 0; i < capacity; i += 1) {
                chunks[i] = new FixedTypeArray(*from.chunks[i]);
            }
        }

        void extendSize() {
            FixedTypeArray** newChunks = new FixedTypeArray*[capacity * 2];
            int i = 0;
            for (i; i < capacity; i += 1) {
                newChunks[i] = chunks[i];
            }
            capacity *= 2;
            for (i; i < capacity; i += 1) {
                newChunks[i] = new FixedTypeArray(chunkSize);
            }
            delete[] chunks;
            chunks = newChunks;
        }

        void pushBack(Type* value) {
            FixedTypeArray* currentChunk = chunks[currentChunkIdx];
            if (currentChunk->capacity == currentChunk->used) {
                currentChunkIdx += 1;
                if (currentChunkIdx == capacity) {
                    extendSize();
                }
                currentChunk = chunks[currentChunkIdx];
            }
            currentChunk->pushBack(value);
            numberOfElements += 1;
        }

        Type* get(size_t idx) {

            size_t chunkIdx = idx / chunkSize;

            return chunks[chunkIdx]->get(idx % chunkSize);
        }

        
        /** Set value at idx. An out of bound idx is undefined.  */
        void set(size_t idx, Type* val) {
            size_t chunkIdx = idx / chunkSize;

            chunks[chunkIdx]->set(idx % chunkSize, val);
        }

        size_t size() {
            return numberOfElements;
        }

        int indexOf(Type* item) {
            for (int i = 0; i <= currentChunkIdx; i += 1) {
                int indexOf = chunks[i]->indexOf(item);
                if (indexOf != -1) {
                    return indexOf;
                }
            }
            return -1;
        }

        ~EffTypeArr() {
          for (int i = 0; i < capacity; i += 1) {
            delete chunks[i];
          }
          delete[] chunks;
        }
};