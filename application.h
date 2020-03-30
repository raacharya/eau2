#pragma once

#include "dataframe.h"
#include "store.h"
#include "network.h"

class Application {
    public:
        KDStore kd = NULL;


        Application(int idx) {
            kd = KDStore(idx);
        }

        virtual void run_() {
            assert(false);
        }
};

class Trivial : public Application {
    public:
        Trivial(size_t idx) : Application(idx) { }
        void run_() {
            size_t SZ = 1000*1000;
            float* vals = new float[SZ];
            float sum = 0;
            for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
            Key key("triv",0);
            DistDataFrame* df = fromArray(&key, &kd, SZ, vals);
            assert(df->get_float(0,1) == 1);
            DistDataFrame* df2 = kd.get(key);
            for (size_t i = 0; i < SZ; ++i) sum -= df2->get_float(0,i);
            assert(sum==0);
            delete df; delete df2;
        }
};

