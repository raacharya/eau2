#pragma once

#include "../util/object.h"
#include "schema.h"
#include "../array/array.h"
#include "column.h"
#include "row.h"
#include "rower.h"

class KDStore;

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame : public Object {

    public:

        Schema *schema;
        EffColArr *columns;

        /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
        DataFrame(DataFrame &df) : DataFrame(*df.schema) {
        }

        /** Create a data frame from a schema and columns. All columns are created
          * empty. */
        explicit DataFrame(Schema &schemaRef) {
            schema = new Schema();
            columns = new EffColArr();
            for (int i = 0; i < schemaRef.types->size(); i += 1) {
                char curType = schemaRef.types->get(i);
                schema->add_column(curType, nullptr);
                if (curType == 'I') {
                    columns->pushBack(new IntColumn());
                } else if (curType == 'F') {
                    columns->pushBack(new FloatColumn());
                } else if (curType == 'B') {
                    columns->pushBack(new BoolColumn());
                } else {
                    columns->pushBack(new StringColumn());
                }
            }
        }

        /** Returns the dataframe's schema. Modifying the schema after a dataframe
          * has been created in undefined. */
        Schema &get_schema() {
            return *schema;
        }

        /** Adds a column this dataframe, updates the schema, the new column
          * is external, and appears as the last column of the dataframe, the
          * name is optional and external. A nullptr colum is undefined. */
        void add_column(Column *col, String *name) {
            assert(col != nullptr);
            //assert(nrows() == col->size());
            Column *copy;
            if (col->get_type() == 'I') {
                copy = new IntColumn(*(col->as_int()));
            } else if (col->get_type() == 'F') {
                copy = new FloatColumn(*(col->as_float()));
            } else if (col->get_type() == 'B') {
                copy = new BoolColumn(*(col->as_bool()));
            } else {
                copy = new StringColumn(*(col->as_string()));
            }
            columns->pushBack(copy);
            schema->add_column(copy->get_type(), name);
            if(nrows() == 0) {
                for(size_t i = 0; i < col->size(); i++) {
                    schema->add_row(nullptr);
                }
            }
        }

        /** Return the value at the given column and row. Accessing rows or
         *  columns out of bounds, or request the wrong type throws an error.*/
        int get_int(size_t col, size_t row) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'I');
            return columns->get(col)->as_int()->get(row);
        }
        bool get_bool(size_t col, size_t row) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'B');
            return columns->get(col)->as_bool()->get(row);
        }

        float get_float(size_t col, size_t row) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'F');
            return columns->get(col)->as_float()->get(row);
        }

        String *get_string(size_t col, size_t row) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'S');
            return columns->get(col)->as_string()->get(row);
        }

        /** Return the offset of the given column name or -1 if no such col. */
        int get_col(String &col) {
            return schema->col_idx(col.cstr_);
        }

        /** Return the offset of the given row name or -1 if no such row. */
        int get_row(String &col) {
            return schema->row_idx(col.cstr_);
        }

        /** Set the value at the given column and row to the given value.
          * If the column is not  of the right type or the indices are out of
          * bound, then we throw an error */
        void set(size_t col, size_t row, int val) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'I');
            columns->get(col)->as_int()->set(row, val);
        }

        void set(size_t col, size_t row, bool val) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'B');
            columns->get(col)->as_bool()->set(row, val);
        }

        void set(size_t col, size_t row, float val) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'F');
            columns->get(col)->as_float()->set(row, val);
        }

        void set(size_t col, size_t row, String *val) {
            assert(0 <= col && col < schema->width());
            assert(0 <= row && row < schema->length());
            assert(columns->get(col)->get_type() == 'S');
            columns->get(col)->as_string()->set(row, val);
        }

        /** Set the fields of the given row object with values from the columns at
          * the given offset.  If the row is not form the same schema as the
          * dataframe, then we throw an error
          */
        void fill_row(size_t idx, Row &row) {
            for (int col = 0; col < columns->size(); col++) {
                Column *column = columns->get(col);
                char colType = column->get_type();
                if (colType == 'I') {
                    row.set(col, column->as_int()->get(idx));
                } else if (colType == 'B') {
                    row.set(col, column->as_bool()->get(idx));
                } else if (colType == 'F') {
                    row.set(col, column->as_float()->get(idx));
                } else {
                    row.set(col, column->as_string()->get(idx));
                }
            }
        }

        /** Add a row at the end of this dataframe. The row is expected to have
         *  the right schema and be filled with values, otherwise undedined.  */
        void add_row(Row &row) {
            for (size_t col = 0; col < columns->size(); col++) {
                Column *column = columns->get(col);
                char colType = column->get_type();
                if (colType == 'I') {
                    column->as_int()->push_back(row.get_int(col));
                } else if (colType == 'B') {
                    column->as_bool()->push_back(row.get_bool(col));
                } else if (colType == 'F') {
                    column->as_float()->push_back(row.get_float(col));
                } else {
                    column->as_string()->push_back(row.get_string(col));
                }
            }

            row.set_idx(schema->length());
            schema->add_row(nullptr);
        }

        /** The number of rows in the dataframe. */
        size_t nrows() {
            return schema->length();
        }

        /** The number of columns in the dataframe.*/
        size_t ncols() {
            return schema->width();
        }

        /** Visit rows in order */
        void map(Rower &r) {
            for(size_t i = 0; i < 50; i++) {
                Row* row = new Row(*schema);
                row->set_idx(i);
                fill_row(i, *row);
                r.accept(*row);
                for(size_t j = 0; j < row->width(); j++) {
                    char colType = columns->get(j)->get_type();
                    if (colType == 'I') {
                        set(j, i, row->get_int(j));
                    } else if (colType == 'B') {
                        set(j, i, row->get_bool(j));
                    } else if (colType == 'F') {
                        set(j, i, row->get_float(j));
                    } else {
                        set(j, i, row->get_string(j));
                    }
                }
                delete row;
            }
        }

        /** Create a new dataframe, constructed from rows for which the given Rower
          * returned true from its accept method. */
        DataFrame *filter(Rower &r) {
            auto* newDf = new DataFrame(*schema);
            for(size_t i = 0; i < schema->length(); i++) {
                Row* row = new Row(*schema);
                fill_row(i, *row);
                if(r.accept(*row)) {
                    newDf->add_row(*row);
                }
                else {
                    delete row;
                }
            }
            return newDf;
        }

        /** Print the dataframe in SoR format to standard output. */
        void print() {
            auto* pr = new PrintRower();
            map(*pr);
            delete pr;
        }


        /**
         * @brief Destroy the Data Frame object
         *
         */
        ~DataFrame() override {
            delete columns;
            delete schema;
        }
};

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
                DistFixedColArray* currentChunk = columns->array[columns->currentChunkIdx];
                size_t chunkIdx = currentChunk->size() == currentChunk->numElements() ?
                        columns->currentChunkIdx + 1 : columns->currentChunkIdx;
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

        DistDataFrame(DataFrame &from, Key* key, Distributable* kvStore_var) {
            kvStore = kvStore_var;
            id = key->key->clone();
            id->concat("-df");
            schema = new DistSchema(*from.schema, key, kvStore);
            String* cols_id = id->clone();
            cols_id->concat("-cols");
            columns = new DistEffColArr(*from.columns, cols_id, kvStore);
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
        void map(Reader* reader) {
            size_t r = columns->get(0)->size();
            size_t c = columns->size();
            size_t chunkSize = columns->chunkSize;
            //fix
            assert(r > 0);
            size_t numChunks = ((r - 1) / chunkSize) + 1;
            size_t index = 0;

            Row** rows = new Row*[chunkSize];

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

                for(size_t i = 0; i < chunkSize; i++) {
                    reader->visit(*rows[i]);
                }

                index += 1;
            }

            delete[] rows;
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

        void put(Key &key, DataFrame *df);

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
 * Construct a new DistDataframe from a Dataframe and store it in the nodes
 * @param key - the key associated with the new Dataframe
 * @param df - the Dataframe to base the new DistDataframe off of
 */
void KDStore::put(Key& key, DataFrame* df) {
    delete new DistDataFrame(*df, &key, kvStore);
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
        Row row(schema);
        r->visit(row);
        ddf->add_row(row);
    }
    ddf->lock();
    return ddf;
}
