#pragma once

#include "network.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {
    public:

        EffStrArr *rows;
        EffStrArr *columns;
        EffCharArr *types;

        /** Copying constructor */
        Schema(Schema &from) : Object() {
            rows = new EffStrArr(*from.rows);
            columns = new EffStrArr(*from.columns);
            types = new EffCharArr(*from.types);

        }

        /** Create an empty schema **/
        Schema() : Object() {
            rows = new EffStrArr();
            columns = new EffStrArr();
            types = new EffCharArr();
        }

        /** Create a schema from a string of types. A string that contains
          * characters other than those identifying the four type results in
          * undefined behavior. The argument is external, a nullptr argument is
          * undefined. **/
        Schema(const char *strTypes) : Schema() {
            for (size_t i = 0; i < strlen(strTypes); i++) {
                add_column(strTypes[i], nullptr);
            }
        }

        /** Add a column of the given type and name (can be nullptr), name
          * is external. Names are expectd to be unique, duplicates result
          * in undefined behavior. */
        void add_column(char typ, String *name) {
            types->pushBack(typ);
            columns->pushBack(name);
        }

        /** Add a row with a name (possibly nullptr), name is external.  Names are
         *  expectd to be unique, duplicates result in undefined behavior. */
        void add_row(String *name) {
            rows->pushBack(name);
        }

        /** Return name of row at idx; nullptr indicates no name. An idx >= width
          * is undefined. */
        String *row_name(size_t idx) {
            return rows->get(idx);
        }

        /** Return name of column at idx; nullptr indicates no name given.
          *  An idx >= width is undefined.*/
        String *col_name(size_t idx) {
            return columns->get(idx);

        }

        /** Return type of column at idx. An idx >= width is undefined. */
        char col_type(size_t idx) {
            return types->get(idx);

        }

        /** Given a column name return its index, or -1. */
        int col_idx(const char *name) {
            assert(name != nullptr);
            String *str = new String(name);
            int indexOf = columns->indexOf(str);
            delete str;
            return indexOf;
        }

        /** Given a row name return its index, or -1. */
        int row_idx(const char *name) {
            assert(name != nullptr);
            String *str = new String(name);
            int indexOf = rows->indexOf(str);
            delete str;
            return indexOf;
        }

        /** The number of columns */
        size_t width() {
            return columns->size();

        }

        /** The number of rows */
        size_t length() {
            return rows->size();
        }

        ~Schema() {
            delete rows;
            delete columns;
            delete types;
        }
};

/*************************************************************************
 * DistSchema::
 * A DistSchema is a Schema which is distributed across the nodes of the network.
 */
class DistSchema : public Object {
    public:

        DistEffCharArr *types;
        String* id;
        Distributable* kvStore;

        /** Copying constructor */
        DistSchema(Schema &from, String* id_var, Distributable* kvStore_var) : Object() {
            kvStore = kvStore_var;
            id = id_var->clone();
            id->concat("-schema");
            String* types_id = id->clone();
            types_id->concat("-types");
            types = new DistEffCharArr(*from.types, types_id, kvStore);

        }

        /** Create an empty schema **/
        DistSchema(String* id_var, Distributable* kvStore_var) : Object() {
            kvStore = kvStore_var;
            id = id_var->clone();
            id->concat("-schema");
            String* types_id = id->clone();
            types_id->concat("-types");
            types = new DistEffCharArr(types_id, kvStore);
        }

        /** Return type of column at idx. An idx >= width is undefined. */
        char col_type(size_t idx) {
            return types->get(idx);

        }

        ~DistSchema() {
            delete types;
        }
};
 
