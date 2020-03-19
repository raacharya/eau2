#pragma once

#include <map>

using namespace std;

class Key : public Object {
    public:
        String* key;
        int node;

        Key(String* strKey, int homeNode) : Object() {
            key = strKey;
            node = homeNode;
        }

        bool equals(Object* o) {
            if (o == this) return true;
            Key* other = dynamic_cast<Key*>(o);
            if (other == nullptr) return false;
            return key->equals(other->key);
        }

        size_t hash_me() {
            return key->hash_me();
        }
};

class KDStore : public Object {
    public:
        int index;
        map<Key, DataFrame*> store;

        KDStore(int idx) : Object() {
            index = idx;
        }

        DataFrame* get(Key key) {
            if (key.node == index) {
                return store.find(key)->second;
            }
            // implement distributed part later
        }

        void put(Key key, DataFrame* df) {
            if (key.node == index) {
                store.at(key) = df;
            }
            // implement distributed part later
        }

        DataFrame* waitAndGet(Key key) {
            // implement blocking when distributed part is done
            return get(key);
        }
};