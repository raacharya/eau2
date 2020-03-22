#pragma once

#include <map>
#include "dataframe.h"
#include "network.h"

using namespace std;

class KDStore : public Object {
    public:
        int index;
        map<Key, DistDataFrame*> store;

        KDStore(int idx) : Object() {
            index = idx;
        }

        DistDataFrame* get(Key key) {
            return new DistDataFrame(key.key->c_str());

            if (key.node == index) {
                return store.find(key)->second;
            }
            // implement distributed part later
        }

        void put(Key key, DataFrame* df) {
            delete new DistDataFrame(*df, key.key->c_str());
        }

        DistDataFrame* waitAndGet(Key key) {
            while (true) {
                DistDataFrame* df = get(key);
                if (df) {
                    return df;
                }
            }
        }
};