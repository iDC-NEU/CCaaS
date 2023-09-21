//
// Created by 周慰星 on 23-3-30.
//

#include "storage/redo_loger.h"
#include "epoch/epoch_manager.h"
#include "storage/tikv.h"
#include "storage/leveldb.h"
#include "storage/hbase.h"
#include "storage/mot.h"
#include "storage/nebula.h"

namespace Taas {
    Context RedoLoger::ctx;
    AtomicCounters RedoLoger::epoch_log_lsn(10);
    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>> RedoLoger::committed_txn_cache;
    void RedoLoger::StaticInit(const Context& ctx_) {
        ctx = ctx_;
        auto max_length = ctx.taasContext.kCacheMaxLength;
        epoch_log_lsn.Init(max_length);
        committed_txn_cache.resize(max_length);
        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            committed_txn_cache[i] = std::make_unique<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>();
        }
        if(ctx.storageContext.is_tikv_enable) {
            TiKV::StaticInit(ctx);
        }
        if(ctx.storageContext.is_leveldb_enable) {
            LevelDB::StaticInit(ctx);
        }
        if(ctx.storageContext.is_hbase_enable) {
            HBase::StaticInit(ctx);
        }
        MOT::StaticInit(ctx);
        Nebula::StaticInit(ctx);
    }

    void RedoLoger::ClearRedoLog(uint64_t& epoch) {
        auto epoch_mod = epoch % ctx.taasContext.kCacheMaxLength;
        committed_txn_cache[epoch_mod]->clear();
//        committed_txn_cache[epoch_mod] = std::make_unique<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>();
        epoch_log_lsn.SetCount(epoch_mod, 0);
        if(ctx.storageContext.is_mot_enable) {
            MOT::StaticClear(epoch);
        }
        if(ctx.storageContext.is_tikv_enable) {
            TiKV::StaticClear(epoch);
        }
        if(ctx.storageContext.is_leveldb_enable) {
            LevelDB::StaticClear(epoch);
        }
        if(ctx.storageContext.is_hbase_enable) {
            HBase::StaticClear(epoch);
        }
    }


    bool RedoLoger::RedoLog(std::shared_ptr<proto::Transaction> txn_ptr) {
        uint64_t epoch_id = txn_ptr->commit_epoch();
        auto lsn = epoch_log_lsn.IncCount(epoch_id, 1);
        auto key = std::to_string(epoch_id) + ":" + std::to_string(lsn);
        committed_txn_cache[epoch_id % ctx.taasContext.kCacheMaxLength]->insert(key, txn_ptr);
        if(ctx.storageContext.is_mot_enable) {
            if(txn_ptr->storage_type() == "mot")
                MOT::DBRedoLogQueueEnqueue(epoch_id, txn_ptr);
            if(txn_ptr->storage_type() == "nebula")
                Nebula::DBRedoLogQueueEnqueue(epoch_id, txn_ptr);
        }
        if(ctx.storageContext.is_tikv_enable) {
            if(txn_ptr->storage_type() == "kv")
                TiKV::DBRedoLogQueueEnqueue(epoch_id, txn_ptr);
        }
        if(ctx.storageContext.is_leveldb_enable) {
            if(txn_ptr->storage_type() == "kv")
                LevelDB::DBRedoLogQueueEnqueue(epoch_id, txn_ptr);
        }
        if(ctx.storageContext.is_hbase_enable) {
            if(txn_ptr->storage_type() == "kv")
                HBase::DBRedoLogQueueEnqueue(epoch_id, txn_ptr);
        }
        txn_ptr.reset();
        return true;
    }

    bool RedoLoger::GeneratePushDownTask(const uint64_t &epoch) {
        if(ctx.storageContext.is_mot_enable) {
            MOT::GeneratePushDownTask(epoch);
        }
        if(ctx.storageContext.is_tikv_enable) {
            TiKV::GeneratePushDownTask(epoch);
        }
        if(ctx.storageContext.is_leveldb_enable) {
            LevelDB::GeneratePushDownTask(epoch);
        }
        if(ctx.storageContext.is_hbase_enable) {
            HBase::GeneratePushDownTask(epoch);
        }
        return true;
    }

    bool RedoLoger::CheckPushDownComplete(const uint64_t &epoch) {
        return (ctx.storageContext.is_mot_enable == 0 || MOT::CheckEpochPushDownComplete(epoch))
            && (ctx.storageContext.is_tikv_enable == 0 || TiKV::CheckEpochPushDownComplete(epoch))
            && (ctx.storageContext.is_leveldb_enable == 0 || LevelDB::CheckEpochPushDownComplete(epoch))
            && (ctx.storageContext.is_hbase_enable == 0 || HBase::CheckEpochPushDownComplete(epoch));
    }
}
