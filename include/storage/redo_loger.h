//
// Created by 周慰星 on 23-3-30.
//

#ifndef TAAS_REDO_LOGER_H
#define TAAS_REDO_LOGER_H

#include "tools/context.h"
#include "tools/concurrent_hash_map.h"
#include "tools/atomic_counters.h"
#include "proto/message.pb.h"
#include "tikv_client.h"
#include "tools/blocking_concurrent_queue.hpp"

namespace Taas {
    class RedoLoger {
    public:
        static AtomicCounters ///epoch, value        for epoch log (each epoch has single one counter)
            epoch_log_lsn;
        static std::vector<std::unique_ptr<std::vector<proto::Transaction>>> redo_log; // [epoch_no]<no, serialize(PB::txn)>
        static std::vector<std::unique_ptr<concurrent_unordered_map<std::string, proto::Transaction>>> committed_txn_cache;
        static std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
            epoch_redo_log_queue; ///store transactions receive from clients, wait to push down
        static std::atomic<uint64_t> pushed_down_mot_epoch, pushed_down_tikv_epoch;
        static void StaticInit(const Context& ctx);
        static void ClearRedoLog(const Context& ctx, uint64_t& epoch_mod);
        static bool RedoLog(const Context& ctx, proto::Transaction& txn);

        static uint64_t IncPushedDownMOTEpoch() {
            return pushed_down_mot_epoch.fetch_add(1);
        }
        static uint64_t GetPushedDownMOTEpoch() {
            return pushed_down_mot_epoch.load();
        }
        static uint64_t IncPushedDownTiKVEpoch() {
            return pushed_down_tikv_epoch.fetch_add(1);
        }
        static uint64_t GetPushedDownTiKVEpoch() {
            return pushed_down_tikv_epoch.load();
        }

        static void RedoLogQueueEnqueue(const Context &ctx, uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr);
        static bool RedoLogQueueTryDequeue(const Context &ctx, uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr);

    };
}



#endif //TAAS_REDO_LOGER_H
