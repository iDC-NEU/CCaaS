//
// Created by 周慰星 on 23-3-30.
//

#include "storage/redo_loger.h"
#include "epoch/epoch_manager.h"
#include "storage/tikv.h"
#include "storage/leveldb.h"
#include "storage/hbase.h"
#include "storage/mot.h"

namespace Taas {

    AtomicCounters RedoLoger::epoch_log_lsn(10);
    std::unique_ptr<util::thread_pool_light> RedoLoger::workers;
    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>> RedoLoger::committed_txn_cache;
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>> RedoLoger::epoch_redo_log_queue;

    void RedoLoger::StaticInit(const Context &ctx) {
        auto max_length = ctx.kCacheMaxLength;
        workers = std::make_unique<util::thread_pool_light>(ctx.kTikvThreadNum, "epoch storage send");
        epoch_log_lsn.Init(max_length);
        committed_txn_cache.resize(max_length);
        epoch_redo_log_queue.resize(max_length);
        for (int i = 0; i < static_cast<int>(max_length); i++) {
            committed_txn_cache[i] = std::make_unique<concurrent_unordered_map<std::string, std::shared_ptr<proto::Transaction>>>();
            epoch_redo_log_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }
        if (ctx.is_tikv_enable) {
            TiKV::StaticInit(ctx);
        }
        if (ctx.is_leveldb_enable) {
            LevelDB::StaticInit(ctx);
        }
        if (ctx.is_hbase_enable) {
            HBase::StaticInit(ctx);
        }
        MOT::StaticInit(ctx);
    }

    void RedoLoger::ClearRedoLog(const Context &ctx, uint64_t &epoch) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        committed_txn_cache[epoch_mod]->clear();
        epoch_log_lsn.SetCount(epoch_mod, 0);
        if (ctx.is_tikv_enable) {
            TiKV::StaticClear(epoch);
        }
        if (ctx.is_leveldb_enable) {
            LevelDB::StaticClear(epoch);
        }
        if (ctx.is_hbase_enable) {
            HBase::StaticClear(epoch);
        }
    }


    bool RedoLoger::RedoLog(const Context &ctx, std::shared_ptr<proto::Transaction> txn_ptr) {
        uint64_t epoch_id = txn_ptr->commit_epoch();
        auto lsn = epoch_log_lsn.IncCount(epoch_id, 1);
        auto key = std::to_string(epoch_id) + ":" + std::to_string(lsn);
        committed_txn_cache[epoch_id % ctx.kCacheMaxLength]->insert(key, txn_ptr);
        epoch_redo_log_queue[epoch_id]->enqueue(txn_ptr);
        if (ctx.is_mot_enable) {
//            MOT::DBRedoLogQueueEnqueue(epoch_id, txn_ptr);
            MOT::epoch_should_push_down_txn_num.IncCount(epoch_id, txn_ptr->server_id(), 1);
//            workers->push_task([=] {
//                MOT::PushDownTxn(epoch_id, txn_ptr);
//            });
        }
        if (ctx.is_tikv_enable) {
//            TiKV::DBRedoLogQueueEnqueue(epoch_id, txn_ptr);
            TiKV::epoch_should_push_down_txn_num.IncCount(epoch_id, txn_ptr->server_id(), 1);
//            workers->push_task([=] {
//                TiKV::PushDownTxn(epoch_id, txn_ptr);
//            });
        }
        if (ctx.is_leveldb_enable) {
//            LevelDB::DBRedoLogQueueEnqueue(epoch_id, txn_ptr);
            LevelDB::epoch_should_push_down_txn_num.IncCount(epoch_id, txn_ptr->server_id(), 1);
//            workers->push_task([=] {
//                LevelDB::PushDownTxn(epoch_id, txn_ptr);
//            });
        }
        if (ctx.is_hbase_enable) {
//            HBase::DBRedoLogQueueEnqueue(epoch_id, txn_ptr);
            HBase::epoch_should_push_down_txn_num.IncCount(epoch_id, txn_ptr->server_id(), 1);
//            workers->push_task([=] {
//                HBase::PushDownTxn(epoch_id, txn_ptr);
//            });
        }
        return true;
    }

    bool RedoLoger::GeneratePushDownTask(const Context &ctx, const uint64_t &epoch) {
        if (ctx.is_mot_enable) {
            MOT::GeneratePushDownTask(epoch);
        }
        if (ctx.is_tikv_enable) {
            TiKV::GeneratePushDownTask(epoch);
        }
        if (ctx.is_leveldb_enable) {
            LevelDB::GeneratePushDownTask(epoch);
        }
        if (ctx.is_hbase_enable) {
            HBase::GeneratePushDownTask(epoch);
        }
        return true;
    }

    bool RedoLoger::CheckPushDownComplete(const Context &ctx, const uint64_t &epoch) {
        return (ctx.is_mot_enable == 0 || MOT::CheckEpochPushDownComplete(epoch))
               && (ctx.is_tikv_enable == 0 || TiKV::CheckEpochPushDownComplete(epoch))
               && (ctx.is_leveldb_enable == 0 || LevelDB::CheckEpochPushDownComplete(epoch))
               && (ctx.is_hbase_enable == 0 || HBase::CheckEpochPushDownComplete(epoch));
    }

    bool RedoLoger::SendTransactionToDB_Usleep(const Context &ctx) {
        std::mutex mtx;
        std::unique_lock lck(mtx);
        uint64_t epoch, epoch_mod;
        bool sleep_flag;
        std::shared_ptr<proto::Transaction> txn_ptr;
        while (!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetPushDownEpoch();
            while (!EpochManager::IsCommitComplete(epoch)) {
                usleep(storage_sleep_time);
                epoch = EpochManager::GetPushDownEpoch();
            }
            epoch_mod = epoch % ctx.kCacheMaxLength;
            sleep_flag = true;
            while (epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if (txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                if (ctx.is_mot_enable) {
                    MOT::PushDownTxn(epoch, txn_ptr);
                }
                if (ctx.is_tikv_enable) {
                    TiKV::PushDownTxn(epoch, txn_ptr);
                }
                if (ctx.is_leveldb_enable) {
                    LevelDB::PushDownTxn(epoch, txn_ptr);
                }
                if (ctx.is_hbase_enable) {
                    HBase::PushDownTxn(epoch, txn_ptr);
                }
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(storage_sleep_time);
        }
    }
}
