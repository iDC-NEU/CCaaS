//
// Created by zwx on 23-6-30.
//

#ifndef TAAS_HBASE_H
#define TAAS_HBASE_H

#pragma once

#include "tools/context.h"
#include "tools/atomic_counters.h"
#include "tools/blocking_concurrent_queue.hpp"

#include <proto/transaction.pb.h>

#include <queue>

namespace Taas {
    class HBase {
    public:
        static Context ctx;
        static AtomicCounters_Cache
                epoch_should_push_down_txn_num, epoch_pushed_down_txn_num;
        static std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> task_queue, redo_log_queue;
        static std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
                epoch_redo_log_queue; ///store transactions receive from clients, wait to push down
        static std::vector<std::unique_ptr<std::atomic<bool>>> epoch_redo_log_complete;

        static void StaticInit(const Context &ctx_);
        static void StaticClear(uint64_t &epoch);

        static void SendTransactionToDB_Usleep();
        static void SendTransactionToDB_Block();

        static bool CheckEpochPushDownComplete(uint64_t &epoch);
        static void DBRedoLogQueueEnqueue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr);
        static bool DBRedoLogQueueTryDequeue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr);

        static bool GeneratePushDownTask(uint64_t &epoch);
    };
}


#endif //TAAS_HBASE_H
