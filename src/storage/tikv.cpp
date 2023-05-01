//
// Created by user on 23-3-26.
//

#include <queue>
#include "epoch/epoch_manager.h"
#include "storage/tikv.h"
#include "tikv_client.h"

namespace Taas {

    Context TiKV::ctx;
    tikv_client::TransactionClient* TiKV::tikv_client_ptr = nullptr;
    AtomicCounters_Cache
            TiKV::tikv_epoch_should_push_down_txn_num(10, 1), TiKV::tikv_epoch_pushed_down_txn_num(10, 1);
    std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>  TiKV::task_queue, TiKV::redo_log_queue;
    std::vector<std::unique_ptr<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>>
            TiKV::tikv_epoch_redo_log_queue;
    std::vector<std::unique_ptr<std::atomic<bool>>> TiKV::epoch_redo_log_complete;


    void TiKV::StaticInit(const Context &ctx_) {
        ctx = ctx_;
        task_queue = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        redo_log_queue = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        tikv_epoch_should_push_down_txn_num.Init(ctx.kCacheMaxLength, ctx.kTxnNodeNum);
        tikv_epoch_pushed_down_txn_num.Init(ctx.kCacheMaxLength, ctx.kTxnNodeNum);
        epoch_redo_log_complete.resize(ctx.kCacheMaxLength);
        tikv_epoch_redo_log_queue.resize(ctx.kCacheMaxLength);
        for(int i = 0; i < static_cast<int>(ctx.kCacheMaxLength); i ++) {
            epoch_redo_log_complete[i] = std::make_unique<std::atomic<bool>>(false);
            tikv_epoch_redo_log_queue[i] = std::make_unique<moodycamel::BlockingConcurrentQueue<std::unique_ptr<proto::Transaction>>>();
        }
    }

    void TiKV::StaticClear(uint64_t &epoch) {
        tikv_epoch_should_push_down_txn_num.Clear(epoch);
        tikv_epoch_pushed_down_txn_num.Clear(epoch);
        epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(false);
        auto txn_ptr = std::make_unique<proto::Transaction>();
        while(tikv_epoch_redo_log_queue[epoch % ctx.kCacheMaxLength]->try_dequeue(txn_ptr));
    }

    bool TiKV::GeneratePushDownTask(uint64_t &epoch) {
        auto txn_ptr = std::make_unique<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(std::move(txn_ptr));
        task_queue->enqueue(nullptr);
        return true;
    }

    void TiKV::sendTransactionToTiKV_usleep() {
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch;
        epoch = EpochManager::GetPushDownEpoch();
//        auto epoch_mod = epoch % ctx.kCacheMaxLength;
//        while(TiKV::tikv_epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
        while(redo_log_queue->try_dequeue(txn_ptr)) {
//            TiKV::redo_log_queue->enqueue(std::move(txn_ptr));
//        }
//        TiKV::redo_log_queue->enqueue(nullptr);
//        while(redo_log_queue->try_dequeue(txn_ptr)) {
            if(txn_ptr == nullptr) continue ;
            if(tikv_client_ptr == nullptr) continue ;
            auto tikv_txn = tikv_client_ptr->begin();
            for (auto i = 0; i < txn_ptr->row_size(); i++) {
                const auto& row = txn_ptr->row(i);
                if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                    tikv_txn.put(row.key(), row.data());
                }
            }
            tikv_txn.commit();
            tikv_epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        }
        CheckEpochPushDownComplete(epoch);
    }


    void TiKV::sendTransactionToTiKV() {
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch, epoch_mod;
        while(!EpochManager::IsTimerStop()) {
            task_queue->wait_dequeue(txn_ptr);
            if(txn_ptr != nullptr) {
                epoch = txn_ptr->commit_epoch();
                epoch_mod = epoch % ctx.kCacheMaxLength;
                while(!TiKV::tikv_epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                    TiKV::redo_log_queue->enqueue(std::move(txn_ptr));
                }
                TiKV::redo_log_queue->enqueue(nullptr);
                while(redo_log_queue->try_dequeue(txn_ptr)) {
                    if(txn_ptr == nullptr) continue;
                    if(tikv_client_ptr == nullptr) continue;
                    auto tikv_txn = tikv_client_ptr->begin();
                    for (auto i = 0; i < txn_ptr->row_size(); i++) {
                        const auto& row = txn_ptr->row(i);
                        if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                            tikv_txn.put(row.key(), row.data());
                        }
                    }
                    tikv_txn.commit();
                    tikv_epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
                }
                CheckEpochPushDownComplete(epoch);
            }
        }
    }


    void TiKV::sendTransactionToTiKV_usleep1() {
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch;
        epoch = EpochManager::GetPushDownEpoch();
//        auto epoch_mod = epoch % ctx.kCacheMaxLength;
//        while(TiKV::tikv_epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
//            if(txn_ptr == nullptr) continue ;
//            if(tikv_client_ptr == nullptr) continue ;
//            auto tikv_txn = tikv_client_ptr->begin();
//            for (auto i = 0; i < txn_ptr->row_size(); i++) {
//                const auto& row = txn_ptr->row(i);
//                if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
//                    tikv_txn.put(row.key(), row.data());
//                }
//            }
//            tikv_txn.commit();
//            tikv_epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
//        }
//        usleep(sleep_time);
        while(redo_log_queue->try_dequeue(txn_ptr)) {
            if(txn_ptr == nullptr) continue ;
            if(tikv_client_ptr == nullptr) continue ;
            auto tikv_txn = tikv_client_ptr->begin();
            for (auto i = 0; i < txn_ptr->row_size(); i++) {
                const auto& row = txn_ptr->row(i);
                if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                    tikv_txn.put(row.key(), row.data());
                }
            }
            tikv_txn.commit();
            tikv_epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        }
    }


    void TiKV::sendTransactionToTiKV_Wait() {
        std::unique_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch;
        while(!EpochManager::IsTimerStop()) {
            redo_log_queue->wait_dequeue(txn_ptr);
            if(txn_ptr == nullptr) continue;
            epoch = txn_ptr->commit_epoch();
            if(tikv_client_ptr == nullptr) continue;
            auto tikv_txn = tikv_client_ptr->begin();
            for (auto i = 0; i < txn_ptr->row_size(); i++) {
                const auto& row = txn_ptr->row(i);
                if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                    tikv_txn.put(row.key(), row.data());
                }
            }
            tikv_txn.commit();
            tikv_epoch_pushed_down_txn_num.IncCount(epoch, txn_ptr->server_id(), 1);
        }
    }

    void TiKV::TiKVRedoLogQueueEnqueue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &&txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        tikv_epoch_redo_log_queue[epoch_mod]->enqueue(std::move(txn_ptr));
        tikv_epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
    }
    bool TiKV::TiKVRedoLogQueueTryDequeue(uint64_t &epoch, std::unique_ptr<proto::Transaction> &txn_ptr) {
        auto epoch_mod = epoch % ctx.kCacheMaxLength;
        return tikv_epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
    }

    bool TiKV::CheckEpochPushDownComplete(uint64_t &epoch) {
        if(epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->load()) return true;
        if(epoch < EpochManager::GetLogicalEpoch() &&
                tikv_epoch_pushed_down_txn_num.GetCount(epoch) >= tikv_epoch_should_push_down_txn_num.GetCount(epoch)) {
            epoch_redo_log_complete[epoch % ctx.kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }

}
