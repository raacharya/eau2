#pragma once

#include "../dataframe/dataframe.h"
#include "../network/network.h"

class Application {
    public:
        KDStore* kd;
        size_t index;

        explicit Application(size_t idx, KDStore* kd_var) {
            index = idx;
            kd = kd_var;
        }

        virtual void run_() {
            assert(false);
        }
};

class Trivial : public Application {
    public:
        explicit Trivial(size_t idx, KDStore* kd_var) : Application(idx, kd_var) { }
        void run_() override {
            if (index == 0) {
                size_t SZ = 1000 * 1;
                auto *vals = new float[SZ];
                double sum = 0;
                for (size_t i = 0; i < SZ; ++i) {
                    vals[i] = i;
                    sum += i;
                }
                Key key("triv", 0);
                DistDataFrame *df = DistDataFrame::fromArray(&key, kd, SZ, vals);
                assert(df->get_float(0, 1) == 1);
                DistDataFrame *df2 = kd->get(key);
                for (size_t i = 0; i < SZ; ++i) {
                    float cur = df2->get_float(0, i);
                    sum -= cur;
                }
                assert(sum == 0);
                delete df;
                delete df2;
                delete[] vals;
            }
        }
};

