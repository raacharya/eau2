#pragma once

#include "../network/network.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {
    public:

        EffCharArr *types;

        /** Copying constructor */
        Schema(Schema &from) : Object() {
            types = new EffCharArr(*from.types);
        }

        /** Create an empty schema **/
        Schema() : Object() {
            types = new EffCharArr();
        }

        /** Create a schema from a string of types. A string that contains
          * characters other than those identifying the four type results in
          * undefined behavior. The argument is external, a nullptr argument is
          * undefined. **/
        Schema(const char *strTypes) : Schema() {
            for (size_t i = 0; i < strlen(strTypes); i++) {
                add_column(strTypes[i]);
            }
        }

        /** Add a column of the given type and name (can be nullptr), name
          * is external. Names are expectd to be unique, duplicates result
          * in undefined behavior. */
        void add_column(char typ) {
            types->pushBack(typ);
        }

        ~Schema() {
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
        String* id; // owned
        Distributable* kvStore;

        /** Copying constructor */
        DistSchema(Schema &from, Key* key, Distributable* kvStore_var) : Object() {
            kvStore = kvStore_var;
            id = key->key->clone();
            id->concat("-schema");
            String* types_id = id->clone();
            types_id->concat("-types");
            types = new DistEffCharArr(*from.types, types_id, kvStore, key->node, false);
            delete types_id;
        }

        /** Create an empty schema **/
        DistSchema(Key* key, Distributable* kvStore_var) : Object() {
            kvStore = kvStore_var;
            id = key->key->clone();
            id->concat("-schema");
            String* types_id = id->clone();
            types_id->concat("-types");
            types = new DistEffCharArr(types_id, kvStore, key->node, true);
            delete types_id;
        }

        void add_column(char type) {
            types->push_back(type);
        }

        void lock() {
            types->lock();
        }

        ~DistSchema() {
            delete id;
            delete types;
        }
};
 
