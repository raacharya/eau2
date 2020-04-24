#pragma once
#include "array.h"
#include "../util/string.h"
#include <stdarg.h>


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