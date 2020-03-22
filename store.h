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
            return new DistDataFrame(key.key);
        }

        void put(Key key, DataFrame* df) {
            delete new DistDataFrame(*df, key.key);
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