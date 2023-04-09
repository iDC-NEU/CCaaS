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
        static AtomicCounters epoch_log_lsn;///epoch, value        for epoch log (each epoch has single one counter)
        static std::vector<std::unique_ptr<concurrent_unordered_map<std::string, proto::Transaction>>> committed_txn_cache;
        static void StaticInit(const Context& ctx);
        static void ClearRedoLog(const Context& ctx, uint64_t& epoch_mod);
        static bool RedoLog(const Context& ctx, proto::Transaction& txn);

        static bool GeneratePushDownTask(const Context& ctx, uint64_t& epoch);

        static bool CheckPushDownComplete(const Context& ctx, uint64_t& epoch);

    };
}



#endif //TAAS_REDO_LOGER_H
