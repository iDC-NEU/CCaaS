//
// Created by zwx on 23-6-30.
//

#include "storage/hbase.h"
#include "hbase_server/hbaseHandler.h"
#include "epoch/epoch_manager.h"

#include <glog/logging.h>

namespace Taas {
    std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>  HBase::task_queue, HBase::redo_log_queue;
    std::vector<std::unique_ptr<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>> HBase::epoch_redo_log_queue;
    std::atomic<uint64_t> HBase::pushed_down_epoch(1);
    std::atomic<uint64_t> HBase::total_commit_txn_num(0), HBase::success_commit_txn_num(0), HBase::failed_commit_txn_num(0);
    std::vector<std::unique_ptr<std::atomic<bool>>> HBase::epoch_redo_log_complete;
    std::condition_variable HBase::commit_cv;


    std::atomic<uint64_t> HBase::inc_id;

    std::vector<std::shared_ptr<AtomicCounters_Cache>>
            HBase::epoch_should_push_down_txn_num_local_vec,
            HBase::epoch_pushed_down_txn_num_local_vec;

    void HBase::Init() {
        thread_id = inc_id.fetch_add(1);
        shard_num = TaasContext::kTxnNodeNum;
        max_length = TaasContext::kCacheMaxLength;
        local_server_id = TaasContext::txn_node_ip_index;
        epoch_should_push_down_txn_num_local= std::make_shared<AtomicCounters_Cache>(max_length, shard_num);
        epoch_pushed_down_txn_num_local= std::make_shared<AtomicCounters_Cache>(max_length, shard_num);
        epoch_should_push_down_txn_num_local_vec[thread_id] = epoch_should_push_down_txn_num_local;
        epoch_pushed_down_txn_num_local_vec[thread_id] = epoch_pushed_down_txn_num_local;
    }

    void HBase::StaticInit() {


        task_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        redo_log_queue = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        epoch_redo_log_complete.resize(TaasContext::kCacheMaxLength);
        epoch_redo_log_queue.resize(TaasContext::kCacheMaxLength);
        for(int i = 0; i < static_cast<int>(TaasContext::kCacheMaxLength); i ++) {
            epoch_redo_log_complete[i] = std::make_unique<std::atomic<bool>>(false);
            epoch_redo_log_queue[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        }
        epoch_should_push_down_txn_num_local_vec.resize(StorageContext::kHbaseThreadNum);
        epoch_pushed_down_txn_num_local_vec.resize(StorageContext::kHbaseThreadNum);
    }

    void HBase::StaticClear(const uint64_t &epoch) {
        epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->store(false);
//        epoch_redo_log_queue[epoch % TaasContext::kCacheMaxLength] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<proto::Transaction>>>();
        ClearAllThreadLocalCountNum(epoch, epoch_should_push_down_txn_num_local_vec);
        ClearAllThreadLocalCountNum(epoch, epoch_pushed_down_txn_num_local_vec);
    }

    void HBase::ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        for(const auto& i : vec) {
            if(i != nullptr)
                i->Clear(epoch);
        }
    }
    uint64_t HBase::GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch);
        }
        return ans;
    }
    uint64_t HBase::GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &shard_id, const std::vector<std::shared_ptr<AtomicCounters_Cache>> &vec) {
        uint64_t ans = 0;
        for(const auto& i : vec) {
            if(i != nullptr)
                ans += i->GetCount(epoch, shard_id);
        }
        return ans;
    }

    bool HBase::CheckEpochPushDownComplete(const uint64_t &epoch) {
        if(epoch_redo_log_complete[epoch % TaasContext::kCacheMaxLength]->load()) return true;
//        if(epoch < EpochManager::GetLogicalEpoch() &&
//           epoch_pushed_down_txn_num_local->GetCount(epoch) >= epoch_should_push_down_txn_num.GetCount(epoch)) {
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
    void HBase::DBRedoLogQueueEnqueue(const uint64_t& thread_id, const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
//        epoch_should_push_down_txn_num_local->IncCount(epoch, txn_ptr->txn_server_id(), 1);
        epoch_should_push_down_txn_num_local_vec[thread_id % inc_id.load() ]->IncCount(epoch, txn_ptr->txn_server_id(), 1);
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        epoch_redo_log_queue[epoch_mod]->enqueue(txn_ptr);
        epoch_redo_log_queue[epoch_mod]->enqueue(nullptr);
        txn_ptr.reset();
    }

    bool HBase::DBRedoLogQueueTryDequeue(const uint64_t &epoch, std::shared_ptr<proto::Transaction> txn_ptr) {
        auto epoch_mod = epoch % TaasContext::kCacheMaxLength;
        return epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr);
    }

    bool HBase::GeneratePushDownTask(const uint64_t &epoch) {
        auto txn_ptr = std::make_shared<proto::Transaction>();
        txn_ptr->set_commit_epoch(epoch);
        task_queue->enqueue(txn_ptr);
        task_queue->enqueue(nullptr);
        return true;
    }


    ///YCSB
    const std::string table_name("usertable");
    const std::string family("entire");
    //not clear about the form of row.data(), so insert it as an entire column
    const std::string qualifier("");

    void HBase::SendTransactionToDB_Usleep() {
        bool sleep_flag;
        std::shared_ptr<proto::Transaction> txn_ptr;
        uint64_t epoch, epoch_mod;
        //connect to thrift2 server in order to communicate with hbase
        CHbaseHandler hbase_txn;
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
                hbase_txn.connect(StorageContext::kHbaseIP,9090);
                try{
                    total_commit_txn_num.fetch_add(1);
                    for (auto i = 0; i < txn_ptr->row_size(); i++) {
                        const auto& row = txn_ptr->row(i);
                        if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                            const std::string key = row.key();
                            const std::string row_value = row.data();
                            hbase_txn.putRow(table_name, key, family, qualifier, row_value);
                        }

                    }
                }
                catch (std::exception &e) {
                    LOG(INFO) << "*** Commit Txn To Hbase Failed: " << e.what();
                    failed_commit_txn_num.fetch_add(1);
                }
                hbase_txn.disconnect();
                epoch_pushed_down_txn_num_local->IncCount(txn_ptr->commit_epoch(), txn_ptr->txn_server_id(), 1);
                txn_ptr.reset();
                sleep_flag = false;
            }
            if(sleep_flag)
                usleep(storage_sleep_time);
        }

    }


    void HBase::SendTransactionToDB_Block() {
        std::shared_ptr<proto::Transaction> txn_ptr;
        CHbaseHandler hbase_txn;
        hbase_txn.connect(StorageContext::kHbaseIP,9090);
        std::mutex mtx;
        std::unique_lock lck(mtx);
        uint64_t epoch, epoch_mod;
        bool sleep_flag;
        while(!EpochManager::IsTimerStop()) {
            epoch = EpochManager::GetPushDownEpoch();
            while(!EpochManager::IsCommitComplete(epoch)) {
                commit_cv.wait(lck);
                epoch = EpochManager::GetPushDownEpoch();
            }
            epoch_mod = epoch % TaasContext::kCacheMaxLength;
            sleep_flag = true;
            while(epoch_redo_log_queue[epoch_mod]->try_dequeue(txn_ptr)) {
                if (txn_ptr == nullptr || txn_ptr->txn_type() == proto::TxnType::NullMark) {
                    continue;
                }
                try {
                    total_commit_txn_num.fetch_add(1);
                    for (auto i = 0; i < txn_ptr->row_size(); i++) {
                        const auto &row = txn_ptr->row(i);
                        if (row.op_type() == proto::OpType::Insert || row.op_type() == proto::OpType::Update) {
                            const std::string key = row.key();
                            const std::string row_value = row.data();
                            hbase_txn.putRow(table_name, key, family, qualifier, row_value);
                        }

                    }
                }
                catch (std::exception &e) {
                    LOG(INFO) << "*** Commit Txn To Hbase Failed: " << e.what();
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
