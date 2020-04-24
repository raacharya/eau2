#pragma once
#include <stdlib.h>
#include "../util/string.h"
#include <stdarg.h>
#include "../array/efficientArray.h"

class DistIntColumn;
class DistBoolColumn;
class DistFloatColumn;
class DistStringColumn;

class DistColumn : public Object {
    public:
        bool locked_;
        Distributable* kvStore;
        size_t metadata_node;
        String* id;

        void init_(String* id_var, Distributable* kvStore_var, size_t node) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
        }

        /** Type converters: Return same column under its actual type, or
         *  nullptr if of the wrong type.  */
        virtual DistIntColumn* as_int() {
            assert(false);
        }
        virtual DistBoolColumn*  as_bool() {
            assert(false);
        }
        virtual DistFloatColumn* as_float() {
            assert(false);
        }
        virtual DistStringColumn* as_string() {
            assert(false);
        }

        virtual void lock() {
            assert(false);
        }

        virtual char get_type() {
            assert(false);
        }

        virtual ~DistColumn() {
            delete id;
        }

        virtual size_t size() {
            assert(false);
        }

};

/*************************************************************************
 * DistIntColumn::
 * Holds int values in a distributed network.
 */
class DistIntColumn : public DistColumn {
    public:
        DistEffIntArr* array;

        DistIntColumn(String *id_var, Distributable *kvStore_var, size_t node, bool get) {
            locked_ = get ? kvStore_var->get_bool(node, id_var->clone()->concat("-locked")) : false;
            init_(id_var, kvStore_var, node);
            array = new DistEffIntArr(id_var, kvStore_var, node, get);
        }

        /**
         * @brief get the int at the given index
         *
         * @param idx
         * @return int
         */
        int get(size_t idx) {
            return array->get(idx);
        }

        size_t size() override {
            return array->numberOfElements;
        }

        void push_back(int val) {
            array->push_back(val);
        }

        void lock() override {
            locked_ = true;
            kvStore->put(metadata_node, id->clone()->concat("-locked"), locked_);
            array->lock();
        }

        char get_type() override {
            return 'I';
        }

        /**
         * @brief return this int column as an int column
         *
         * @return IntColumn*
         */
        DistIntColumn* as_int() override {
            return this;
        }

        /**
         * @brief Destroy the Int Column object
         *
         */
        ~DistIntColumn() {
            delete array;
        }
};

/*************************************************************************
 * DistBoolColumn::
 * Holds bool values in a distributed network.
 */
class DistBoolColumn : public DistColumn {
    public:
        DistEffBoolArr* array;

        DistBoolColumn(String *id_var, Distributable *kvStore_var, size_t node, bool get) {
            locked_ = get ? kvStore_var->get_bool(node, id_var->clone()->concat("-locked")) : false;
            init_(id_var, kvStore_var, node);
            array = new DistEffBoolArr(id_var, kvStore_var, node, get);
        }

        /**
         * @brief get the int at the given index
         *
         * @param idx
         * @return int
         */
        bool get(size_t idx) {
            return array->get(idx);
        }

        size_t size() override {
            return array->numberOfElements;
        }

        void push_back(bool val) {
            array->push_back(val);
        }

        void lock() override {
            locked_ = true;
            kvStore->put(metadata_node, id->clone()->concat("-locked"), locked_);
            array->lock();
        }

        char get_type() override {
            return 'B';
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return BoolColumn*
         */
        DistBoolColumn* as_bool() override {
            return this;
        }

        /**
         * @brief Destroy the Int Column object
         *
         */
        ~DistBoolColumn() {
            delete array;
        }
};

/*************************************************************************
 * DistFloatColumn::
 * Holds float values in a distributed network.
 */
class DistFloatColumn : public DistColumn {
    public:
        DistEffFloatArr* array;

        DistFloatColumn(String *id_var, Distributable *kvStore_var, size_t node, bool get) {
            locked_ = get ? kvStore_var->get_bool(node, id_var->clone()->concat("-locked")) : false;
            init_(id_var, kvStore_var, node);
            array = new DistEffFloatArr(id_var, kvStore_var, node, get);
        }

        /**
         * @brief get the int at the given index
         *
         * @param idx
         * @return int
         */
        float get(size_t idx) {
            return array->get(idx);
        }

        size_t size() override {
            return array->numberOfElements;
        }

        void push_back(float val) {
            array->push_back(val);
        }

        void lock() override {
            locked_ = true;
            kvStore->put(metadata_node, id->clone()->concat("-locked"), locked_);
            array->lock();
        }

        char get_type() override {
            return 'F';
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return FloatColumn*
         */
        DistFloatColumn* as_float() override {
            return this;
        }

        /**
         * @brief Destroy the Int Column object
         *
         */
        ~DistFloatColumn() {
            delete array;
        }
};

/*************************************************************************
 * DistStringColumn::
 * Holds String values in a distributed network.
 */
class DistStringColumn : public DistColumn {
    public:
        DistEffStrArr* array;

        DistStringColumn(String *id_var, Distributable *kvStore_var, size_t node, bool get) {
            locked_ = get ? kvStore_var->get_bool(node, id_var->clone()->concat("-locked")) : false;
            init_(id_var, kvStore_var, node);
            array = new DistEffStrArr(id_var, kvStore_var, node, get);
        }

        /**
         * @brief get the int at the given index
         *
         * @param idx
         * @return int
         */
        String* get(size_t idx) {
            return array->get(idx);
        }

        size_t size() override {
            return array->numberOfElements;
        }

        void push_back(String* val) {
            array->push_back(val);
        }

        void lock() override {
            locked_ = true;
            kvStore->put(metadata_node, id->clone()->concat("-locked"), locked_);
            array->lock();
        }

        char get_type() override {
            return 'S';
        }

        /**
         * @brief throw error since this is an int column
         *
         * @return StringColumn*
         */
        DistStringColumn* as_string() override {
            return this;
        }

        /**
         * @brief Destroy the Int Column object
         *
         */
        ~DistStringColumn() {
            delete array;
        }
};

/**
 * Represents an array of Columns.
 */
class DistFixedColArray : public Object {
    public:
        String* id;
        size_t used;
        size_t capacity;
        Distributable* kvStore;
        DistColumn** array;
        size_t metadata_node;

        DistFixedColArray(size_t size, DistEffCharArr* types, String* id_var, Distributable* kvStore_var, size_t node, bool get) : Object() {
            id = id_var->clone();
            used = get ? kvStore_var->get_size_t(node, id->clone()->concat("-used")) : 0;
            capacity = size;
            kvStore = kvStore_var;
            metadata_node = node;
            array = new DistColumn*[capacity];
            for (size_t i = 0; i < used; i += 1) {
                String* col_id = id_var->clone();
                col_id->concat("-");
                col_id->concat(i);
                if (types->get(i) == 'I') {
                    DistIntColumn* copy = new DistIntColumn(col_id, kvStore_var, node, get);
                    array[i] = copy;
                } else if (types->get(i) == 'F') {
                    DistFloatColumn* copy = new DistFloatColumn(col_id, kvStore_var, node, get);
                    array[i] = copy;
                } else if (types->get(i) == 'B') {
                    DistBoolColumn* copy = new DistBoolColumn(col_id, kvStore_var, node, get);
                    array[i] = copy;
                } else {
                    DistStringColumn* copy = new DistStringColumn(col_id, kvStore_var, node, get);
                    array[i] = copy;
                }
                delete col_id;
            }
        }


        /**
         * Returns an element at the given index.
         *
         * @param index the index of the element
         * @return the element of the array at the given index
         */
        virtual DistColumn* get(size_t index) {
            DistColumn* element = dynamic_cast <DistColumn*> (array[index]);
            return element;
        }

        virtual void push_back(DistColumn* item) {
            assert(used < capacity);
            array[used] = item;
            used += 1;
        }

        void lock() {
            kvStore->put(metadata_node, id->clone()->concat("-used"), used);
            for (size_t i = 0; i < used; i += 1) {
                array[i]->lock();
            }
        }

        /**
         * @brief return the size of this array
         *
         * @return size_t
         */
        size_t size() {
            return capacity;
        }

        /**
         * @brief get the number of elements in this arrays
         *
         * @return size_t
         */
        size_t numElements() {
            return used;
        }


        /**
         * Destructor of this class.
         */
        ~DistFixedColArray() {
            delete id;
            for (size_t i = 0; i < used; i += 1) {
                delete array[i];
            }
            delete[] array;
        }
};

/*************************************************************************
 * DistEffColArr:
 * Holds Columns in a distributed network
 */
class DistEffColArr : public Object {
    public:
        size_t chunkSize;
        size_t capacity;
        size_t currentChunkIdx;
        size_t numberOfElements;
        String* id;
        Distributable* kvStore;
        DistFixedColArray** array;
        size_t metadata_node;
        DistEffCharArr* types;

        DistEffColArr(DistEffCharArr* types_var, String* id_var, Distributable* kvStore_var, size_t node, bool get) {
            id = id_var->clone();
            kvStore = kvStore_var;
            metadata_node = node;
            types = types_var;
            chunkSize = get ? kvStore->get_size_t(node, id->clone()->concat("-chunkSize")) : 50;
            capacity = get ? kvStore->get_size_t(node, id->clone()->concat("-capacity")) : 1;
            currentChunkIdx = get ? kvStore->get_size_t(node, id->clone()->concat("-currentChunk")) : 0;
            numberOfElements = get ? kvStore->get_size_t(node, id->clone()->concat("-numElements")) : 0;
            array = new DistFixedColArray*[capacity];
            for (size_t i = 0; i < capacity; i += 1) {
                String* col_id = id->clone()->concat("-")->concat(i);
                array[i] = new DistFixedColArray(chunkSize, types, col_id, kvStore, node, get);
                delete col_id;
            }
        }

        /**
         * @brief Get the column at the given index
         *
         * @param idx
         * @return Column*
         */
        DistColumn* get(size_t idx) {
            size_t chunkIdx = idx / chunkSize;
            DistFixedColArray* curChunk = array[chunkIdx];
            return curChunk->get(idx % chunkSize);
        }

        void extendSize() {
            DistFixedColArray** newChunks = new DistFixedColArray*[capacity * 2];
            size_t i = 0;
            for (; i < capacity; i += 1) {
                newChunks[i] = array[i];
            }
            capacity *= 2;
            for (; i < capacity; i += 1) {
                String* col_id = id->clone()->concat("-")->concat(i);
                newChunks[i] = new DistFixedColArray(chunkSize, types, col_id, kvStore, metadata_node, false);
                delete col_id;
            }
            delete[] array;
            array = newChunks;
        }

        /**
         * @brief add a column to this efficient col array
         *
         * @param value
         */
        void push_back(DistColumn* value) {
            DistFixedColArray* currentChunk = array[currentChunkIdx];
            if (currentChunk->size() == currentChunk->numElements()) {
                currentChunkIdx += 1;
                if (currentChunkIdx == capacity) {
                    extendSize();
                }
                currentChunk = array[currentChunkIdx];
            }
            currentChunk->push_back(value);
            numberOfElements += 1;
        }

        void lock() {
            kvStore->put(metadata_node, id->clone()->concat("-chunkSize"), chunkSize);
            kvStore->put(metadata_node, id->clone()->concat("-capacity"), capacity);
            kvStore->put(metadata_node, id->clone()->concat("-currentChunk"), currentChunkIdx);
            kvStore->put(metadata_node, id->clone()->concat("-numElements"), numberOfElements);
            for (size_t i = 0; i < capacity; i += 1) {
                array[i]->lock();
            }
        }

        size_t next_chunk_idx() {
            return currentChunkIdx == capacity ? currentChunkIdx + 1 : currentChunkIdx;
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
        ~DistEffColArr() {
            delete id;
            for (size_t i = 0; i < capacity; i += 1) {
                delete array[i];
            }
            delete[] array;
        }
};