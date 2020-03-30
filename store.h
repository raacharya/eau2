//#pragma once
//
//#include <map>
//#include "dataframe.h"
//#include "network.h"
//
//using namespace std;
//
//class KDStore : public Object {
//    public:
//        int index;
//        Distributable* kvStore;
//
//
//        KDStore(int idx) : Object() {
//            index = idx;
//            kvStore = new Distributable(index);
//        }
//
//        DistDataFrame* get(Key key) {
//            return new DistDataFrame(key.key, kvStore);
//        }
//
//        void put(Key key, DataFrame* df) {
//            delete new DistDataFrame(*df, key.key, kvStore);
//        }
//
//        DistDataFrame* waitAndGet(Key key) {
//            while (true) {
//                DistDataFrame* df = get(key);
//                if (df) {
//                    return df;
//                }
//            }
//        }
//};