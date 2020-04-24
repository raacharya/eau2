#include "application.h"

int main(int argc, char** argv) {
    assert(argc == 3);
    char* file_name = argv[2];
    auto** kds = new KDStore*[5];
    auto* pids = new std::thread[5];
    auto** wcs = new WordCount*[5];
    for (size_t i = 0; i < 5; i += 1) {
        kds[i] = new KDStore(i);
        wcs[i] = new WordCount(i, kds[i], file_name, true);
    }
    for (size_t i = 0; i < 5; i += 1) {
        pids[i] = std::thread(&WordCount::run_, wcs[i]);
    }
    for (size_t i = 0; i < 5; i += 1) {
        pids[i].join();
    }
    for (size_t i = 0; i < 5; i += 1) {
        kds[i]->kvStore->network->shutdown();
        kds[i]->kvStore->network->shutdown_open_conns();
    }
    for (size_t i = 0; i < 5; i += 1) {
        delete wcs[i];
        delete kds[i];
    }
    delete[] wcs;
    delete[] kds;
    delete[] pids;
    return 0;
}