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
    AtomicCounters RedoLoger::epoch_log_lsn(10);
    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>> RedoLoger::committed_txn_cache;
    void RedoLoger::StaticInit() {
        auto max_length = TaasContext::kCacheMaxLength;
        epoch_log_lsn.Init(max_length);
        committed_txn_cache.resize(max_length);

        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            committed_txn_cache[i] = std::make_unique<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>();
        }
        if(StorageContext::is_tikv_enable) {
            TiKV::StaticInit();
        }
        if(StorageContext::is_leveldb_enable) {
            LevelDB::StaticInit();
        }
        if(StorageContext::is_hbase_enable) {
            HBase::StaticInit();
        }
        if(StorageContext::is_mot_enable) {
            MOT::StaticInit();
        }
        if(StorageContext::is_nebula_enable) {
            Nebula::StaticInit();
        }
    }

    void RedoLoger::ClearRedoLog(const uint64_t& epoch) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        committed_txn_cache[epoch_mod]->clear();
        epoch_log_lsn.SetCount(epoch_mod, 0);
        if(StorageContext::is_mot_enable) {
            MOT::StaticClear(epoch);
        }
        if(StorageContext::is_nebula_enable) {
            Nebula::StaticClear(epoch);
        }
        if(StorageContext::is_tikv_enable) {
            TiKV::StaticClear(epoch);
        }
        if(StorageContext::is_leveldb_enable) {
            LevelDB::StaticClear(epoch);
        }
        if(StorageContext::is_hbase_enable) {
            HBase::StaticClear(epoch);
        }
    }


    bool RedoLoger::RedoLog(const uint64_t& thread_id, std::shared_ptr<proto::Transaction> txn_ptr) {
        uint64_t epoch_id = txn_ptr->commit_epoch();
        auto lsn = epoch_log_lsn.IncCount(epoch_id, 1);
        auto key = std::to_string(epoch_id) + ":" + std::to_string(lsn);
        committed_txn_cache[epoch_id % TaasContext::kCacheMaxLength]->insert(key, txn_ptr);
        if(StorageContext::is_mot_enable) {
            if (txn_ptr->storage_type() == "mot")
                MOT::DBRedoLogQueueEnqueue(thread_id, epoch_id, txn_ptr);
        }
        if(StorageContext::is_nebula_enable) {
            if(txn_ptr->storage_type() == "nebula")
                Nebula::DBRedoLogQueueEnqueue(thread_id, epoch_id, txn_ptr);
        }
        if(StorageContext::is_tikv_enable) {
            if(txn_ptr->storage_type() == "kv")
                TiKV::DBRedoLogQueueEnqueue(thread_id, epoch_id, txn_ptr);
        }
        if(StorageContext::is_leveldb_enable) {
            if(txn_ptr->storage_type() == "kv")
                LevelDB::DBRedoLogQueueEnqueue(thread_id, epoch_id, txn_ptr);
        }
        if(StorageContext::is_hbase_enable) {
            if(txn_ptr->storage_type() == "kv")
                HBase::DBRedoLogQueueEnqueue(thread_id, epoch_id, txn_ptr);
        }
        txn_ptr.reset();
        return true;
    }

    bool RedoLoger::GeneratePushDownTask(const uint64_t &epoch) {
        if(StorageContext::is_mot_enable) {
            MOT::GeneratePushDownTask(epoch);
        }
        if(StorageContext::is_nebula_enable) {
            Nebula::GeneratePushDownTask(epoch);
        }
        if(StorageContext::is_tikv_enable) {
            TiKV::GeneratePushDownTask(epoch);
        }
        if(StorageContext::is_leveldb_enable) {
            LevelDB::GeneratePushDownTask(epoch);
        }
        if(StorageContext::is_hbase_enable) {
            HBase::GeneratePushDownTask(epoch);
        }
        return true;
    }

    bool RedoLoger::CheckPushDownComplete(const uint64_t &epoch) {
        return (StorageContext::is_mot_enable == 0 || MOT::CheckEpochPushDownComplete(epoch))
            && (StorageContext::is_nebula_enable == 0 || Nebula::CheckEpochPushDownComplete(epoch))
            && (StorageContext::is_tikv_enable == 0 || TiKV::CheckEpochPushDownComplete(epoch))
            && (StorageContext::is_leveldb_enable == 0 || LevelDB::CheckEpochPushDownComplete(epoch))
            && (StorageContext::is_hbase_enable == 0 || HBase::CheckEpochPushDownComplete(epoch));
    }
}
