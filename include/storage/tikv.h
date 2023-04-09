//
// Created by user on 23-3-26.
//

#ifndef TAAS_TIKV_H
#define TAAS_TIKV_H

#include <proto/transaction.pb.h>
#include "tikv_client.h"
#include "tools/atomic_counters.h"
#include "tools/blocking_concurrent_queue.hpp"

namespace Taas {
    class TiKV {
    public:
        static Context ctx;
        static tikv_client::TransactionClient* tikv_client_ptr;
        static AtomicCounters_Cache
                tikv_epoch_should_push_down_txn_num, tikv_epoch_pushed_down_txn_num;
        static std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>> task_queue, redo_log_queue;
        static std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
            tikv_epoch_redo_log_queue; ///store transactions receive from clients, wait to push down
        static std::vector<std::unique_ptr<std::atomic<bool>>> epoch_redo_log_complete;

        static void StaticInit(const Context& ctx_);
        static void StaticClear(uint64_t &epoch);

        static bool GeneratePushDownTask(uint64_t &epoch);

        static void sendTransactionToTiKV_usleep();
        static void sendTransactionToTiKV();
        static void sendTransactionToTiKV_Wait();

        static bool CheckEpochPushDownComplete(uint64_t &epoch);
        static void TiKVRedoLogQueueEnqueue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr);
        static bool TiKVRedoLogQueueTryDequeue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr);



    };
}

#endif //TAAS_TIKV_H
