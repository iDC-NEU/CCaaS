//
// Created by 周慰星 on 23-3-30.
//

#include "storage/redo_loger.h"

namespace Taas {

    AtomicCounters RedoLoger::epoch_log_lsn(10);
    std::vector<std::unique_ptr<std::vector<proto::Transaction>>> RedoLoger::redo_log;

    ///committed_txn_cache[epoch][lsn]->txn 用于打包发送给mot
    std::vector<std::unique_ptr<concurrent_unordered_map<std::string, proto::Transaction>>> RedoLoger::committed_txn_cache;
    ///存放完成的事务 发送给tikv或其他只提供接口的系统，进行日志下推, 按照epoch先后进行
    std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>> RedoLoger::epoch_redo_log_queue;

    std::atomic<uint64_t> RedoLoger::pushed_down_mot_epoch(1), RedoLoger::pushed_down_tikv_epoch(1);

    void RedoLoger::StaticInit(Context &ctx) {
        auto max_length = ctx.kCacheMaxLength;
        epoch_log_lsn.Init(max_length);
        committed_txn_cache.resize(max_length);
        redo_log.resize(max_length);
        epoch_redo_log_queue.resize(max_length);
        for(int i = 0; i < static_cast<int>(max_length); i ++) {
            committed_txn_cache[i] = std::make_unique<concurrent_unordered_map<std::string, proto::Transaction>>();
            redo_log[i] = std::make_unique<std::vector<proto::Transaction>>();
            epoch_redo_log_queue[i] = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        }
    }

    void RedoLoger::ClearRedoLog(uint64_t& epoch, Context &ctx) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        redo_log[epoch_mod]->clear();
        committed_txn_cache[epoch_mod]->clear();
        epoch_log_lsn.SetCount(epoch_mod, 0);
    }


    bool RedoLoger::RedoLog(Context &ctx, proto::Transaction &txn) {
        uint64_t epoch_id = txn.commit_epoch();
        auto epoch_mod = epoch_id % ctx.kCacheMaxLength;
        auto lsn = epoch_log_lsn.IncCount(epoch_id, 1);
        auto key = std::to_string(epoch_id) + ":" + std::to_string(lsn);
        if(ctx.is_tikv_enable) {
            epoch_redo_log_queue[epoch_mod]->enqueue(std::make_unique<proto::Transaction>(txn));
            epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
        }
        committed_txn_cache[epoch_id % ctx.kCacheMaxLength]->insert(key, txn);
        return true;
    }

    void RedoLoger::RedoLogQueueEnqueue(uint64_t& epoch, std::unique_ptr<proto::Transaction>&& txn_ptr, Context& ctx) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        epoch_redo_log_queue[epoch_mod]->enqueue(std::move(txn_ptr));
        epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
    }
    bool RedoLoger::RedoLogQueueTryDequeue(uint64_t& epoch, std::unique_ptr<proto::Transaction>& txn_ptr, Context& ctx) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        return epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
    }

}
