#pragma once

#include "../util/object.h"
#include "schema.h"
#include "../array/array.h"
#include "column.h"
#include "row.h"
#include "rower.h"

class KDStore;

/****************************************************************************
 * DistDataFrame::
 *
 * A DistDataFrame is a DataFrame which is distributed among many nodes
 */
class DistDataFrame : public Object {

    public:

        DistSchema *schema;
        DistEffColArr *columns;
        String* id;
        Distributable* kvStore;
        bool locked_;

        DistDataFrame(Schema& schema_var, Key* key, Distributable* kvStore_var) {
            id = key->key->clone()->concat("-df");
            kvStore = kvStore_var;
            locked_ = false;
            schema = new DistSchema(schema_var, key, kvStore);
            String* cols_id = id->clone()->concat("-cols");
            columns = new DistEffColArr(schema->types, cols_id, kvStore, key->node, false);
            delete cols_id;
            for (int i = 0; i < schema_var.types->size(); i += 1) {
                char curType = schema_var.types->get(i);
                size_t chunkIdx = columns->next_chunk_idx();
                String* col_id = id->clone()->concat("-cols-")->concat(chunkIdx)->concat("-")
                        ->concat(columns->numberOfElements);
                if (curType == 'I') {
                    columns->push_back(new DistIntColumn(col_id, kvStore, key->node, false));
                } else if (curType == 'F') {
                    columns->push_back(new DistFloatColumn(col_id, kvStore, key->node, false));
                } else if (curType == 'B') {
                    columns->push_back(new DistBoolColumn(col_id, kvStore, key->node, false));
                } else {
                    columns->push_back(new DistStringColumn(col_id, kvStore, key->node, false));
                }
                delete col_id;
            }
        }

        DistDataFrame(Key* key, Distributable* kvStore_var) {
            kvStore = kvStore_var;
            id = key->key->clone();
            id->concat("-df");
            schema = new DistSchema(key, kvStore);
            String* cols_id = id->clone();
            cols_id->concat("-cols");
            columns = new DistEffColArr(schema->types, cols_id, kvStore, key->node, true);
            delete cols_id;
            locked_ = true;
        }

        /**
         * @brief Destroy the Data Frame object
         *
         */
        ~DistDataFrame() override {
            delete columns;
            delete schema;
            delete id;
        }

        /** Return the value at the given column and row. Accessing rows or
         *  columns out of bounds, or request the wrong type throws an error.*/
        int get_int(size_t col, size_t row) {
            return columns->get(col)->as_int()->get(row);
        }
        bool get_bool(size_t col, size_t row) {
            return columns->get(col)->as_bool()->get(row);
        }

        float get_float(size_t col, size_t row) {
            return columns->get(col)->as_float()->get(row);
        }

        String *get_string(size_t col, size_t row) {
            return columns->get(col)->as_string()->get(row);
        }

        void add_column(DistColumn* col) {
            assert(!locked_);
            schema->add_column(col->get_type());
            columns->push_back(col);
        }

        void add_row(Row& row) {
            size_t cols_size = columns->size();
            for (size_t colIndex = 0; colIndex < cols_size; colIndex += 1) {
                DistColumn* column = columns->get(colIndex);
                char colType = column->get_type();
                if (colType == 'I') {
                    column->as_int()->push_back(row.get_int(colIndex));
                } else if (colType == 'B') {
                    column->as_bool()->push_back(row.get_bool(colIndex));
                } else if (colType == 'F') {
                    column->as_float()->push_back(row.get_float(colIndex));
                } else {
                    column->as_string()->push_back(row.get_string(colIndex));
                    delete row.get_string(colIndex);
                }
            }
        }

        void lock() {
            locked_ = true;
            columns->lock();
            schema->lock();
        }

        void fill_rows(Row** rows, DistIntColumn* col, size_t chunkNum, size_t col_num) {
            FixedIntArray* curr = col->array->get_chunk(chunkNum);
            for(size_t i = 0; i < curr->used; i++) {
                rows[i]->set(col_num, curr->get(i));
            }
        }

        void fill_rows(Row** rows, DistFloatColumn* col, size_t chunkNum, size_t col_num) {
            FixedFloatArray* curr = col->array->get_chunk(chunkNum);
            for(size_t i = 0; i < curr->used; i++) {
                rows[i]->set(col_num, curr->get(i));
            }
        }

        void fill_rows(Row** rows, DistBoolColumn* col, size_t chunkNum, size_t col_num) {
            FixedBoolArray* curr = col->array->get_chunk(chunkNum);
            for(size_t i = 0; i < curr->used; i++) {
                rows[i]->set(col_num, curr->get(i));
            }

        }

        void fill_rows(Row** rows, DistStringColumn* col, size_t chunkNum, size_t col_num) {
            FixedStrArray* curr = col->array->get_chunk(chunkNum);
            for(size_t i = 0; i < curr->numElements(); i++) {
                rows[i]->set(col_num, curr->get(i));
            }
        }

        /**
         * Read this distributed df with the given reader
         */
        void mapHelp(Reader* reader, size_t inc, size_t index) {
            size_t r = columns->get(0)->size();
            if (r == 0) return;
            size_t c = columns->size();
            size_t chunkSize = columns->chunkSize;
            size_t numChunks = ((r - 1) / chunkSize) + 1;
            Row** rows = new Row*[std::min(r, chunkSize)];
            for (size_t i = 0; i < std::min(r, chunkSize); i += 1) {
                rows[i] = new Row(c);
            }
            while(index < numChunks) {
                for(size_t i = 0; i < c; i++) {
                    DistColumn* dcol = columns->get(i);
                    if(dcol->get_type() == 'I') {
                        fill_rows(rows, dcol->as_int(), index, i);
                    } else if(dcol->get_type() == 'F') {
                        fill_rows(rows, dcol->as_float(), index, i);
                    } else if(dcol->get_type() == 'B') {
                        fill_rows(rows, dcol->as_bool(), index, i);
                    } else {
                        fill_rows(rows, dcol->as_string(), index, i);
                    }
                }
                for(size_t i = 0; i < std::min(r, chunkSize); i++) {
                    reader->visit(*rows[i]);
                }
                index += inc;
            }
            for (size_t i = 0; i < std::min(r, chunkSize); i += 1) {
                delete rows[i];
            }
            delete[] rows;
        }

        void map(Reader* reader) {
            mapHelp(reader, 1, 0);
        }

        void local_map(Reader* reader) {
            mapHelp(reader, 5, kvStore->index);
        }

        static DistDataFrame *fromArray(Key *key, KDStore *kdStore, size_t size, bool *vals);

        static DistDataFrame *fromArray(Key *key, KDStore *kdStore, size_t size, float *vals);

        static DistDataFrame *fromArray(Key *key, KDStore *kdStore, size_t size, String **vals);

        static DistDataFrame *fromArray(Key *key, KDStore *kdStore, size_t size, int *vals);

        static DistDataFrame *fromVisitor(Key* key, KDStore *kdStore, const char* schema, Writer* r);
};

class KDStore : public Object {
    public:
        size_t index;
        Distributable* kvStore;


        explicit KDStore(size_t idx) : Object() {
            index = idx;
            kvStore = new Distributable(index);
        }

        ~KDStore() {
            delete kvStore;
        }

        DistDataFrame *get(Key &key);

        DistDataFrame *waitAndGet(Key &key);
};

/**
 * Creates a distributed dataframe that contains the given values and stores it in the kv store
 * @param key - the key the df is being stored under
 * @param kvstore - the store it will be in
 * @param size - the number of values
 * @param vals - the values
 * @return
 */
DistDataFrame* DistDataFrame::fromArray(Key* key, KDStore* kdStore, size_t size, int* vals) {
    Schema s{};
    auto* ddf = new DistDataFrame(s, key, kdStore->kvStore);
    String* col_id = key->key->clone()->concat("-df-cols-0-0");
    auto* col = new DistIntColumn(col_id, kdStore->kvStore, key->node, false);
    delete col_id;
    for(size_t i = 0; i < size; i++) {
        col->push_back(vals[i]);
    }
    ddf->add_column(col);
    ddf->lock();
    kdStore->kvStore->send_finished_update(key->key->c_str());
    return ddf;
}

DistDataFrame* DistDataFrame::fromArray(Key* key, KDStore* kdStore, size_t size, float* vals) {
    Schema s{};
    auto* ddf = new DistDataFrame(s, key, kdStore->kvStore);
    String* col_id = key->key->clone()->concat("-df-cols-0-0");
    auto* col = new DistFloatColumn(col_id, kdStore->kvStore, key->node, false);
    delete col_id;
    for(size_t i = 0; i < size; i++) {
        col->push_back(vals[i]);
    }
    ddf->add_column(col);
    ddf->lock();
    kdStore->kvStore->send_finished_update(key->key->c_str());
    return ddf;
}

/**
 * Creates a distributed dataframe that contains the given values and stores it in the kv store
 * @param key - the key the df is being stored under
 * @param kvstore - the store it will be in
 * @param size - the number of values
 * @param vals - the values
 * @return
 */
DistDataFrame* DistDataFrame::fromArray(Key* key, KDStore* kdStore, size_t size, bool* vals) {
    Schema s{};
    auto* ddf = new DistDataFrame(s, key, kdStore->kvStore);
    String* col_id = key->key->clone()->concat("-df-cols-0-0");
    auto* col = new DistBoolColumn(col_id, kdStore->kvStore, key->node, false);
    delete col_id;
    for(size_t i = 0; i < size; i++) {
        col->push_back(vals[i]);
    }
    ddf->add_column(col);
    ddf->lock();
    kdStore->kvStore->send_finished_update(key->key->c_str());
    return ddf;
}

/**
 * Creates a distributed dataframe that contains the given values and stores it in the kv store
 * @param key - the key the df is being stored under
 * @param kvstore - the store it will be in
 * @param size - the number of values
 * @param vals - the values
 * @return
 */
DistDataFrame* DistDataFrame::fromArray(Key* key, KDStore* kdStore, size_t size, String** vals) {
    Schema s{};
    auto* ddf = new DistDataFrame(s, key, kdStore->kvStore);
    String* col_id = key->key->clone()->concat("-df-cols-0-0");
    auto* col = new DistStringColumn(col_id, kdStore->kvStore, key->node, false);
    delete col_id;
    for(size_t i = 0; i < size; i++) {
        col->push_back(vals[i]);
    }
    ddf->add_column(col);
    ddf->lock();
    kdStore->kvStore->send_finished_update(key->key->c_str());
    return ddf;
}

/**
 * Get the DistDataframe associated with the key
 * @param key - the key for the DistDataframe
 * @return - DistDatafram associated with the key
 */
DistDataFrame* KDStore::get(Key& key) {
    return new DistDataFrame(&key, kvStore);
}

/**
 * Get the DistDataframe associated with the key when it exists
 * @param key - the key for the DistDataframe
 * @return - DistDatafram associated with the key
 */
DistDataFrame* KDStore::waitAndGet(Key& key) {
    std::unique_lock<std::mutex> df_lock(kvStore->complete_df_lock);
    while (kvStore->completed_dfs.find(std::string(key.key->c_str())) == kvStore->completed_dfs.end())
        kvStore->complete_df_cond.wait(df_lock);
    df_lock.unlock();
    return new DistDataFrame(&key, kvStore);
}

/**
 * Create a DistDataFrame from the given writer
 */
DistDataFrame* DistDataFrame::fromVisitor(Key* key, KDStore* kdStore, const char* type, Writer* r) {
    Schema schema(type);
    DistDataFrame* ddf = new DistDataFrame(schema, key, kdStore->kvStore);

    while(!r->done()) {
        Row row(strlen(type));
        r->visit(row);
        ddf->add_row(row);
    }
    ddf->lock();
    kdStore->kvStore->send_finished_update(key->key->c_str());
    return ddf;
}
