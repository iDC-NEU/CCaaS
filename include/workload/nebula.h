//
// Created by zwx on 23-8-29.
//

#ifndef TAAS_NEBULA_H
#define TAAS_NEBULA_H

#pragma once
#include "tools/context.h"
#include "txn_type.h"

#include <nebula/client/Config.h>
#include <nebula/client/SessionPool.h>
#include <bthread/countdown_event.h>

namespace workload {
    class Nebula {
    private:
        static nebula::SessionPoolConfig nebulaSessionPoolConfig;
        static std::unique_ptr<nebula::SessionPool> nebulaSessionPool;
    public:
        static void Init(const Taas::Context& ctx);
        static void InsertData(const uint64_t& tid);
        static void RunTxn(const uint64_t& tid, const std::shared_ptr<std::atomic<uint64_t>>& sunTxnNum);
    };

}




#endif //TAAS_NEBULA_H
