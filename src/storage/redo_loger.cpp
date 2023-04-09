//
// Created by 周慰星 on 23-3-30.
//

#include "storage/redo_loger.h"
#include "epoch/epoch_manager.h"
#include "storage/tikv.h"
#include "storage/mot.h"

namespace Taas {

    AtomicCounters RedoLoger::epoch_log_lsn(10);
    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, proto::Transaction>>> RedoLoger::committed_txn_cache;
    void RedoLoger::StaticInit(const Context& ctx) {
        auto max_length = ctx.kCacheMaxLength;
        epoch_log_lsn.Init(max_length);
        committed_txn_cache.resize(max_length);
        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            committed_txn_cache[i] = std::make_unique<concurrent_unordered_map<std::string, proto::Transaction>>();
        }
        if(ctx.is_tikv_enable) {
            TiKV::StaticInit(ctx);
        }
        MOT::StaticInit(ctx);
    }

    void RedoLoger::ClearRedoLog(const Context& ctx, uint64_t& epoch) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        committed_txn_cache[epoch_mod]->clear();
        epoch_log_lsn.SetCount(epoch_mod, 0);
        if(ctx.is_tikv_enable) {
            TiKV::StaticClear(epoch);
        }
    }


    bool RedoLoger::RedoLog(const Context& ctx, proto::Transaction &txn) {
        uint64_t epoch_id = txn.commit_epoch();
        auto lsn = epoch_log_lsn.IncCount(epoch_id, 1);
        auto key = std::to_string(epoch_id) + ":" + std::to_string(lsn);
        committed_txn_cache[epoch_id % ctx.kCacheMaxLength]->insert(key, txn);
        if(ctx.is_tikv_enable) {
            TiKV::tikv_epoch_should_push_down_txn_num.IncCount(epoch_id, txn.server_id(), 1);
            TiKV::TiKVRedoLogQueueEnqueue(epoch_id, std::make_unique<proto::Transaction>(txn));
        }
        return true;
    }

    bool RedoLoger::GeneratePushDownTask(const Context &ctx, uint64_t &epoch) {
        if(ctx.is_tikv_enable) {
            TiKV::GeneratePushDownTask(epoch);
        }
        MOT::GeneratePushDownTask(epoch);
        return true;
    }

    bool RedoLoger::CheckPushDownComplete(const Context &ctx, uint64_t &epoch) {
        return MOT::IsMOTPushDownComplete(epoch) &&
                TiKV::CheckEpochPushDownComplete(epoch);
    }
}
