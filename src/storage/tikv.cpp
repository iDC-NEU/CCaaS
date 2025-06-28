//
// Created by user on 23-3-26.
//

#include <queue>
#include "epoch/epoch_manager.h"
#include "storage/tikv.h"
#include "tikv_client.h"

#include <glog/logging.h>

namespace Taas {

    tikv_client::TransactionClient* TiKV::tikv_client_ptr = nullptr;

    std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>  TiKV::task_queue, TiKV::redo_log_queue;
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>> TiKV::epoch_redo_log_queue;
    std::atomic<uint64_t> TiKV::pushed_down_epoch(1);
    std::atomic<uint64_t> TiKV::total_commit_txn_num(0), TiKV::success_commit_txn_num(0), TiKV::failed_commit_txn_num(0);
    std::vector<std::unique_ptr<std::atomic<bool>>> TiKV::epoch_redo_log_complete;
    std::condition_variable TiKV::commit_cv;

    std::atomic<uint64_t> TiKV::inc_id;

    std::vector<std::shared_ptr<AtomicCounters_Cache>>
            TiKV::epoch_should_push_down_txn_num_local_vec,
            TiKV::epoch_pushed_down_txn_num_local_vec;

    void TiKV::Init() {
        thread_id = inc_id.fetch_add(1);
        shard_num = TaasContext::kTxnNodeNum;
        max_length = TaasContext::kCacheMaxLength;
        local_server_id = TaasContext::txn_node_ip_index;
        epoch_should_push_down_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, shard_num);
        epoch_pushed_down_txn_num_local = std::make_shared<AtomicCounters_Cache>(max_length, shard_num);
        epoch_should_push_down_txn_num_local_vec[thread_id] = epoch_should_push_down_txn_num_local;
        epoch_pushed_down_txn_num_local_vec[thread_id] = epoch_pushed_down_txn_num_local;
    }

    void TiKV::StaticInit() {
        task_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        redo_log_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        epoch_redo_log_complete.resize(TaasContext::kCacheMaxLength);
        epoch_redo_log_queue.resize(TaasContext::kCacheMaxLength);
        for(int i = 0; i < static_cast<int>(TaasContext::kCacheMaxLength); i ++) {
            epoch_redo_log_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_redo_log_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }
        epoch_should_push_down_txn_num_local_vec.resize(StorageContext::kTikvThreadNum);
        epoch_pushed_down_txn_num_local_vec.resize(StorageContext::kTikvThreadNum);
    }

    void TiKV::StaticClear(const uint64_t &epoch) {
        epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->store(false);
//        epoch_redo_log_queue[epoch % TaasContext::kCacheMaxLength] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        ClearAllThreadLocalCountNum(epoch, epoch_should_push_down_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_pushed_down_txn_num_local_vec);
    }

    void TiKV::ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        for(const auto& i : vec) {
            if(i != nullptr)
                i->Clear(epoch);
        }
    }
    uint64_t TiKV::GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch);
        }
        return ans;
    }
    uint64_t TiKV::GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &shard_id, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch, shard_id);
        }
        return ans;
    }

    bool TiKV::CheckEpochPushDownComplete(const uint64_t &epoch) {
        if(epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->load()) return true;
//        if(epoch < EpochManager::GetLogicalEpoch() &&
//           epoch_pushed_down_txn_num.GetCount(epoch) >= epoch_should_push_down_txn_num.GetCount(epoch)) {
//            epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->store(true);
//            return true;
//        }
        if(epoch < EpochManager::GetLogicalEpoch() &&
            GetAllThreadLocalCountNum(epoch, epoch_pushed_down_txn_num_local_vec) >=
            GetAllThreadLocalCountNum(epoch, epoch_should_push_down_txn_num_local_vec)
                ) {
            epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->store(true);
            return true;
        }
        return false;
    }
    void TiKV::DBRedoLogQueueEnqueue(const uint64_t& thread_id, const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
//        epoch_should_push_down_txn_num_local->IncCount(epoch, txn_ptr->txn_server_id(), 1);
        epoch_should_push_down_txn_num_local_vec[thread_id % inc_id.load() ]->IncCount(epoch, txn_ptr->txn_server_id(), 1);
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        epoch_redo_log_queue[epoch_mod]->enqueue(txn_ptr);
        epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
        txn_ptr.reset();
    }

    bool TiKV::DBRedoLogQueueTryDequeue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        return epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
    }

    bool TiKV::GeneratePushDownTask(const uint64_t &epoch) {
        auto txn_ptr = std::make_shared<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(txn_ptr);
        task_queue->enqueue(nullptr);
        return true;
    }


    void TiKV::SendTransactionToDB_Usleep() {
        bool sleep_flag;
        std::shared_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch, epoch_mod;
        while(!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetPushDownEpoch();
            while(!EpochManager::IsRecordCommitted(epoch)) {
                usleep(storage_sleep_time);
                epoch = EpochManager::GetPushDownEpoch();
            }
            epoch_mod = epoch % TaasContext::kCacheMaxLength;
            sleep_flag = true;
            while(epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if(txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
//                commit_cv.notify_all();
                if(tikv_client_ptr == nullptr) continue ;
                auto tikv_txn = tikv_client_ptr->begin();
                for (auto i = 0; i < txn_ptr->row_size(); i++) {
                    const auto& row = txn_ptr->row(i);
                    if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                        tikv_txn.put(row.key(), row.data());
                    }
                }
                try{
                    total_commit_txn_num.fetch_add(1);
                    tikv_txn.commit();
                }
                catch (std::exception &e) {
                    LOG(INFO) << "*** Commit Txn To Tikv Failed: " << e.what();
                    failed_commit_txn_num.fetch_add(1);
                }
                epoch_pushed_down_txn_num_local->IncCount(txn_ptr->commit_epoch(), txn_ptr->txn_server_id(), 1);
                txn_ptr.reset();
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(storage_sleep_time);
        }
    }


    void TiKV::SendTransactionToDB_Block() {
        std::mutex mtx;
        std::unique_lock lck(mtx);
        uint64_t epoch, epoch_mod;
        bool sleep_flag;
        std::shared_ptr<proto::Transaction> txn_ptr;
        while(!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetPushDownEpoch();
            while (!EpochManager::IsCommitComplete(epoch)) {
                commit_cv.wait(lck);
                epoch = EpochManager::GetPushDownEpoch();
            }
            epoch_mod = epoch % TaasContext::kCacheMaxLength;
            sleep_flag = true;
            while (epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if (txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                if (tikv_client_ptr == nullptr) continue;
                auto tikv_txn = tikv_client_ptr->begin();
                for (auto i = 0; i < txn_ptr->row_size(); i++) {
                    const auto &row = txn_ptr->row(i);
                    if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                        tikv_txn.put(row.key(), row.data());
                    }
                }
                try {
                    total_commit_txn_num.fetch_add(1);
                    tikv_txn.commit();
                }
                catch (std::exception &e) {
                    LOG(INFO) << "*** Commit Txn To Tikv Failed: " << e.what();
                    failed_commit_txn_num.fetch_add(1);
                }
                epoch_pushed_down_txn_num_local->IncCount(txn_ptr->commit_epoch(), txn_ptr->txn_server_id(), 1);
                txn_ptr.reset();
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(storage_sleep_time);
        }
    }

}
