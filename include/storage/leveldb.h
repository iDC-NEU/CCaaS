//
// Created by user on 23-6-30.
//

#ifndef TAAS_LEVELDB_H
#define TAAS_LEVELDB_H

#pragma once

#include "tools/context.h"
#include "tools/atomic_counters.h"
#include "tools/blocking_concurrent_queue.hpp"

#include "brpc/channel.h"
#include <proto/transaction.pb.h>

#include <queue>
#include <condition_variable>

namespace Taas {
    class LevelDB {
    public:
        static Context ctx;
        static std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>> task_queue, redo_log_queue;
        static std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
                epoch_redo_log_queue; ///store transactions receive from clients, wait to push down

        static std::atomic<uint64_t> pushed_down_epoch;
        static AtomicCounters_Cache
                epoch_should_push_down_txn_num, epoch_pushed_down_txn_num;
        static std::atomic<uint64_t> total_commit_txn_num, success_commit_txn_num, failed_commit_txn_num;
        static std::vector<std::unique_ptr<std::atomic<bool>>> epoch_redo_log_complete;

        static std::condition_variable commit_cv;

        static void StaticInit(const Context &ctx_);
        static void StaticClear(const uint64_t &epoch);

        static void SendTransactionToDB_Usleep();
        static void SendTransactionToDB_Block();

        static bool CheckEpochPushDownComplete(const uint64_t &epoch);
        static void DBRedoLogQueueEnqueue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr);
        static bool DBRedoLogQueueTryDequeue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr);

        static bool GeneratePushDownTask(const uint64_t &epoch);

        static brpc::Channel channel;

        static void PushDownTxn(const uint64_t &epoch, const std::shared_ptr<proto::Transaction> &txn_ptr);
    };
}


#endif //TAAS_LEVELDB_H
