#pragma once
#include "../util/object.h"

class NetworkIfc : public Object {
    public:
        virtual void register_node(size_t idx) {}

        virtual size_t index() { assert(false); }

        virtual void send_m(Message* msg) = 0;

        virtual Message* recv_m() = 0;
};