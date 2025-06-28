//
// Created by user on 23-3-26.
//

#ifndef TAAS_TIKV_H
#define TAAS_TIKV_H

#pragma once

#include "tools/atomic_counters.h"
#include "tools/blocking_concurrent_queue.hpp"

#include "proto/transaction.pb.h"
#include "tikv_client.h"


namespace Taas {
    class TiKV {
    public:
        static tikv_client::TransactionClient* tikv_client_ptr;

        static std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>> task_queue, redo_log_queue;
        static std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>>
                epoch_redo_log_queue; ///store transactions receive from clients, wait to push down

        static std::atomic<uint64_t> pushed_down_epoch;
        static std::atomic<uint64_t> total_commit_txn_num, success_commit_txn_num, failed_commit_txn_num;
        static std::vector<std::unique_ptr<std::atomic<bool>>> epoch_redo_log_complete;

        static std::condition_variable commit_cv;


        static std::atomic<uint64_t> inc_id;
        uint64_t thread_id = 0, max_length = 0, shard_num = 0, local_server_id;
        std::shared_ptr<AtomicCounters_Cache>
                epoch_should_push_down_txn_num_local,
                epoch_pushed_down_txn_num_local;

        static std::vector<std::shared_ptr<AtomicCounters_Cache>>
                epoch_should_push_down_txn_num_local_vec,
                epoch_pushed_down_txn_num_local_vec;

        void Init();
        static void StaticInit();
        static void StaticClear(const uint64_t &epoch);

        static void ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) ;
        static uint64_t GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) ;
        static uint64_t GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &sharding_id, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec);

        static bool CheckEpochPushDownComplete(const uint64_t &epoch);
        static void DBRedoLogQueueEnqueue(const uint64_t& thread_id, const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr);
        static bool DBRedoLogQueueTryDequeue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr);

        static bool GeneratePushDownTask(const uint64_t &epoch);
        void SendTransactionToDB_Usleep();
        void SendTransactionToDB_Block();

    };
}

#endif //TAAS_TIKV_H
